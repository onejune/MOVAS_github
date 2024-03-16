import os,sys
os.environ['KMP_DUPLICATE_LIB_OK']='True'

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from tensorflow.keras.preprocessing.sequence import pad_sequences

cur_path = os.path.realpath(__file__)
cur_dir = os.path.dirname(cur_path)
parent_dir = os.path.dirname(cur_dir)
sys.path.append(parent_dir)

from deepctr_torch.inputs import SparseFeat, VarLenSparseFeat, get_feature_names
from deepctr_torch.models import GateNet2Head
from deepctr_torch.layers.utils import slice_arrays
from deepctr_torch.movas_logger import *
from deepctr_torch.loss_utils import *

class ModelTrainFlow():
    def __init__(self, sample_path):
        self.sample_path = sample_path
        self.key2index = {}
        
    def run(self):
        self.load_sample()
        self.sample_process()
        
        self.model_train()
        self.model_validation()

    def load_sample(self):
        data = pd.read_csv(self.sample_path, delimiter=',', dtype=str)
        # shuffle data
        data = data.sample(frac=1.0)
        data = data.reset_index()
        MovasLogger.add_log(content = '-' * 20 + 'sample info' + '-' * 20)
        MovasLogger.add_log(content = type(data))
        MovasLogger.add_log(content = data.shape)
        #MovasLogger.add_log(content = 'sample_info:\n%s' % data.info)
        self.raw_sample_df = data
        
    def split(self, x):
        key_ans = x.split('\001')
        for key in key_ans:
            if key not in self.key2index:
                # Notice : input value 0 is a special "padding",so we do not use 0 to encode valid feature for sequence input
                self.key2index[key] = len(self.key2index) + 1
        return list(map(lambda x: self.key2index[x], key_ans))

    def feature_process(self, data):
        sparse_features = [
            'demand_creative_id','campaign_id',
            'bid_type','supply_platform','supply_normalized_developer_id','supply_normalized_package_name',
            'app_id_core_model','ad_type_core_model','placement_id_core_model','unit_id','countryCode',
            'language_core_model','supply_mtg_category_level_3','make','model','osVersion','connectionType',
            'ups_fea_session_num_14d','ups_fea_avg_session_minute','ups_fea_avg_imp_in_session',
            'ups_fea_session_stage_by_longest','ups_fea_imp_not_click_ps','ups_fea_imp_num_day_1','ups_fea_imp_num_day_2',
            'ups_fea_imp_num_day_3','ups_fea_imp_num_day_4','ups_fea_imp_num_day_5','ups_fea_imp_num_day_6',
            'ups_fea_imp_num_day_7','ups_fea_click_pkg_24_hour_top3','ups_fea_click_pkg_top3','ups_fea_click_pkg_num',
            'ups_fea_click_pkg_seq_24_hour_uniq','ups_fea_ct_top3','ups_fea_req_ctop1_hour_diff','ups_fea_clk_num_day_1',
            'ups_fea_clk_num_day_2','ups_fea_clk_num_day_3','ups_fea_clk_num_day_4','ups_fea_clk_num_day_5',
            'ups_fea_clk_num_day_6','ups_fea_clk_num_day_7','ups_fea_install_pkg_24_hour_top3','ups_fea_install_pkg_top3',
            'ups_fea_install_pkg_num','ups_fea_last_ip','ups_fea_ins_num_day_1','ups_fea_ins_num_day_2','ups_fea_ins_num_day_3',
            'ups_fea_ins_num_day_4','ups_fea_ins_num_day_5','ups_fea_ins_num_day_6','ups_fea_ins_num_day_7',
            'ups_fea_req_lastIns_time_diff','ups_ipua_fea_click_pkg_top3','ups_ipua_fea_install_pkg_top3',
            'ups_ipua_fea_click_pkg_num','ups_ipua_fea_install_pkg_num','ups_ipua_fea_click_pkg_24_hour_top3',
            'ups_ipua_fea_install_pkg_24_hour_top3','ups_ipua_fea_req_lastIns_time_diff','ups_ipua_fea_click_pkg_seq_24_hour_uniq',
            'ups_ipua_fea_ct_top3','ups_ipua_fea_req_ctop1_hour_diff','ups_ipua_fea_last_ip','ups_ipua_fea_imp_not_click_ps',
            'ups_ipua_fea_imp_num_day_1','ups_ipua_fea_imp_num_day_2','ups_ipua_fea_imp_num_day_3','ups_ipua_fea_imp_num_day_4',
            'ups_ipua_fea_imp_num_day_5','ups_ipua_fea_imp_num_day_6','ups_ipua_fea_imp_num_day_7','ups_ipua_fea_clk_num_day_1',
            'ups_ipua_fea_clk_num_day_2','ups_ipua_fea_clk_num_day_3','ups_ipua_fea_clk_num_day_4','ups_ipua_fea_clk_num_day_5',
            'ups_ipua_fea_clk_num_day_6','ups_ipua_fea_clk_num_day_7','ups_ipua_fea_ins_num_day_1','ups_ipua_fea_ins_num_day_2',
            'ups_ipua_fea_ins_num_day_3','ups_ipua_fea_ins_num_day_4','ups_ipua_fea_ins_num_day_5','ups_ipua_fea_ins_num_day_6',
            'ups_ipua_fea_ins_num_day_7','ups_ipua_fea_session_num_14d','ups_ipua_fea_avg_session_minute',
            'ups_ipua_fea_avg_imp_in_session','ups_ipua_fea_session_stage_by_longest','oneIdType','adtype_appid_real_clk_8h',
            'adtype_appid_fake_clk_8h','adtype_appid_real_clk_1d','adtype_appid_fake_clk_1d','adtype_appid_real_clk_7d',
            'adtype_appid_fake_clk_7d','total_real_clk_8h','total_fake_clk_8h','total_real_clk_1d','total_fake_clk_1d',
            'total_real_clk_7d','total_fake_clk_7d','adtype_his_imp','adtype_his_ins','unit_his_imp','unit_his_clk',
            'ups_fea_install_pkg_uniq','devid_type','publisher_id_core_model','sdk_version','isIdfa','demand_package_name',
            'demand_mtg_category_level_1','demand_mtg_category_level_2','demand_mtg_category_level_3',
            'demand_normalized_developer_id','demand_normalized_package_name','mmp_id','is_vta','demand_adv_id',
            'link_type','video_template','endcard_template','m201_algo_id','ec_unique_cid','m106_algo_id','component_ids'
        ]
        sparse_features1 = [
            'campaign_id',
            'bid_type','supply_platform','supply_normalized_developer_id','supply_normalized_package_name',
            'app_id_core_model','ad_type_core_model','placement_id_core_model','unit_id','countryCode',
            'devid_type','publisher_id_core_model','sdk_version','isIdfa','demand_package_name',
            'demand_mtg_category_level_1','demand_mtg_category_level_2','demand_mtg_category_level_3'
        ]
        data[sparse_features] = data[sparse_features].fillna('none')
        
        # 1.Label Encoding for sparse features,and process sequence features
        for feat in sparse_features:
            #print(feat, data[feat].nunique())
            lbe = LabelEncoder()
            data[feat] = lbe.fit_transform(data[feat])
            #print(data[feat])

        # preprocess the sequence feature
        fixlen_feature_columns = [
            SparseFeat(feat, data[feat].nunique(), embedding_dim=8) for feat in sparse_features
        ]
        feature_map = {name: data[name] for name in sparse_features}
        
        lbe = LabelEncoder()
        self.context_indicator = lbe.fit_transform(data['request_id'])

        multi_value_features = []
        data[multi_value_features] = data[multi_value_features].fillna('none')
        varlen_feature_columns = []
        print('-' * 20, 'multi_value_features', '-' * 20)
        for fea_name in multi_value_features:
            print(fea_name, data[fea_name].values)
            key2index = {}
            genres_list = list(map(self.split, data[fea_name].values))
            genres_length = np.array(list(map(len, genres_list)))
            max_len = max(genres_length)
            # pad_sequences的结果是个ndarray
            genres_list = pad_sequences(
                genres_list,
                maxlen=max_len,
                padding='post',
            )
            feature_map[fea_name] = genres_list

            var_len_fea = VarLenSparseFeat(
                SparseFeat(fea_name,
                        vocabulary_size=len(key2index) + 1,
                        embedding_dim=8),
                maxlen=max_len,
                combiner='mean'
            )  # Notice : value 0 is for padding for sequence input feature
            varlen_feature_columns.append(var_len_fea)

        self.linear_feature_columns = fixlen_feature_columns + varlen_feature_columns
        self.dnn_feature_columns = fixlen_feature_columns + varlen_feature_columns
        
        return feature_map
        
    def sample_process(self):
        data = self.raw_sample_df
        data = data.astype(str)
        data = data.sort_values('request_id')
        #print(data.dtypes)
        data['rank_pos'] = data['rank_pos'].astype(int)
        data['label_pos'] = [1 if rank_pos < 3 else 0 for rank_pos in data['rank_pos']]
        data['label_neg'] = [0 if rank_pos < 3 else 1 for rank_pos in data['rank_pos']]

        target = ['label_neg', 'label_pos']
        #print(data.info())
        #MovasLogger.add_log(content = 'label value_counts:\n%s' % data['label'].value_counts())
        print(type(data), data.shape)
        self.label = data[target].values
        MovasLogger.add_log(content = 'labels:\n %s' % self.label)
        
        self.raw_sample_df = data
        feature_map = self.feature_process(data)
        self.generate_train_val_data(feature_map)

    def generate_train_val_data(self, feature_map):
        val_ratio = 0.1
        sample_cnt = len(self.label)
        split_at = int(sample_cnt * (1 - val_ratio))
        fea_name_seq = list(feature_map.keys())
        #print('fea_name_seq:', len(fea_name_seq), fea_name_seq)
        value_list = [feature_map[k] for k in fea_name_seq]

        self.x, self.val_x = (slice_arrays(value_list, 0,
                                split_at), slice_arrays(value_list, split_at))
        self.y, self.val_y = (slice_arrays(self.label, 0,
                                split_at), slice_arrays(self.label, split_at))
        
        self.train_indicator, self.val_indicator = self.context_indicator[0 : split_at], self.context_indicator[split_at : -1]
        MovasLogger.add_log(content='train_indicator:\n%s %s %s' % (len(self.x[0]), split_at, len(self.train_indicator)))

        self.train_data = {k: self.x[fea_name_seq.index(k)] for k in fea_name_seq}
        self.val_data = {k: self.val_x[fea_name_seq.index(k)] for k in fea_name_seq}
        
        MovasLogger.add_log(content='train_data:%s %s' % (len(self.train_data['campaign_id']), len(self.val_data['campaign_id'])))
        
    def model_train(self):
        device = 'cpu'
        use_cuda = True
        if use_cuda and torch.cuda.is_available():
            print('cuda ready...')
            device = 'cuda:0'

        self.model = GateNet2Head(
                    self.linear_feature_columns,
                    self.dnn_feature_columns,
                    task='multiclass',
                    device=device)

        self.model.compile(
            "sgd",
            #"binary_crossentropy",
            LossMethods.jrc_loss,
            metrics=["binary_crossentropy", "auc"],
        )
        
        history = self.model.fit(
                    self.train_data,
                    self.y,
                    context_indicator = self.train_indicator,
                    batch_size=100,
                    epochs=3,
                    verbose=2,
                    shuffle=False,
                    validation_split=0.1)
        
    def model_validation(self):
        pred_ans = self.model.predict(self.val_data, 64)
        loss = round(log_loss(self.val_y, pred_ans), 4)
        auc = round(roc_auc_score(self.val_y, pred_ans), 4)
        MovasLogger.add_log(content = "validation LogLoss: %s" % (loss))
        MovasLogger.add_log(content = "validation AUC: %s" % (auc))

if __name__ == "__main__":
    MovasLogger.init(None, cur_dir + "/" + 'log/jrc_mtg.log')
    input_path = ""
    if len(sys.argv) == 2:
        input_path = sys.argv[1]
    else:
        input_path = "data/sample_small.csv"
    data_path = cur_dir + "/" + input_path
    MovasLogger.add_log(content = 'data_path: %s' % (data_path))
    
    trainer = ModelTrainFlow(data_path)
    trainer.run()
    
    MovasLogger.save_to_local()