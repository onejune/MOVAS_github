import numpy as np
import pandas as pd
from scipy import sparse
import torch
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import log_loss, roc_auc_score
from tensorflow.python.keras.preprocessing.sequence import pad_sequences
import os, sys

cur_path = os.path.realpath(__file__)
cur_dir = os.path.dirname(cur_path)
parent_dir = os.path.dirname(cur_dir)
sys.path.append(parent_dir)
sys.path.insert(0, '..')

import numpy as np
import torch
from deepctr_torch.inputs import (DenseFeat, SparseFeat, VarLenSparseFeat,
                                  get_feature_names)
from deepctr_torch.models.din import DIN
from deepctr_torch.layers.utils import slice_arrays

behavior_feature_list = ["video"]


def split(x):
    key_ans = x.split('\001')
    for key in key_ans:
        if key not in key2index:
            #key2index用于为每一个value分配一个唯一的index,从1开始
            key2index[key] = len(key2index) + 1
    return list(map(lambda x: key2index[x], key_ans))


def train_data_process():
    global key2index
    input_path = ""
    if len(sys.argv) == 2:
        input_path = sys.argv[1]
    else:
        input_path = "data/sample_small.txt"
    data_path = cur_dir + "/" + input_path
    print("cur_dir:", cur_dir)
    print("input_path:", input_path)
    print("data_path:", data_path)
    data = pd.read_csv(data_path, delimiter='\002', dtype=str)

    # shuffle data
    data = data.sample(frac=1.0)
    data = data.reset_index()
    # print('-' * 20, 'sample info', '-' * 20)
    # print(type(data))
    print(data.shape)
    # print(data.head())
    # #print(data.info())
    # print(data.columns)

    data.rename(columns={
        "creative_algo_id_m201_algo_id": "video",
        "clk_video": "hist_video"
    },
                inplace=True)
    target = ['label']
    data[target] = data[target].astype('int')
    # print(data.columns)
    sparse_features = [
        'week_new', 'hour_new', 'hercules_language', 'country_code', 'app_id',
        'unit_id', 'platform', 'network_type', 'os_version', 'sdk_version',
        'ad_type', 'campaign_id', 'package_name', 'package_rate',
        'hercules_package_size', 'package_install_num', 'package_subcategory',
        'package_total_rating_num', 'package_comment_count', 'advertiser_id',
        'rank', 'idfa', 'video_ratio', 'bit_rate', 'resolution_201',
        'video_length', 'video_size', 'playable_id', 'template_group',
        'video_template', 'endcard_template', 'minicard_template',
        'third_party', 'ext_brand', 'ext_model', 'placement_id',
        'creative_algo_id_m106_algo_id', 'video',
        'creative_algo_id_m61002_algo_id', 'devid_type',
        'traffic_unify_pkg_name', 'traffic_app_developer', 'ad_unify_pkg_name',
        'ad_app_developer', 'is_traffic_ad_app_developer_same',
        'sweety_publisher_id', 'crt_ad_title_id', 'crt_bigtemplate_id'
    ]
    multi_value_features = [
        'long_top_tag', 'middle_top_tag', 'short_top_tag', 'dmp_tag',
        'hist_video'
    ]
    #print(data.info())
    print(data['label'].value_counts())
    data[sparse_features] = data[sparse_features].fillna('none')

    print('-' * 20, 'LabelEncoder', '-' * 20)
    for feat in sparse_features:
        print(feat, data[feat].nunique())
        lbe = LabelEncoder()
        tem = lbe.fit_transform(data[feat])
        #tem = tem.tolist()
        # print(type(tem))
        data[feat] = tem

    # preprocess the sequence feature
    fixlen_feature_columns = [
        SparseFeat(feat, data[feat].nunique(), embedding_dim=4)
        for feat in sparse_features
    ]

    model_input = {name: data[name] for name in sparse_features}
    # multi_value_features = ['long_top_tag', 'hist_video']
    data[multi_value_features] = data[multi_value_features].fillna('none')
    varlen_feature_columns = []
    print('-' * 20, 'VarLenSparseFeat', '-' * 20)
    for fea_name in multi_value_features:
        print(fea_name)
        key2index = {}
        values = data[fea_name].values
        #将原始的value string mapping到index list
        genres_list = list(map(split, values))
        genres_length = np.array(list(map(len, genres_list)))
        print('genres_length:', genres_length)
        if 'hist_' in fea_name:
            model_input['seq_length'] = genres_length
        max_len = max(genres_length)
        # pad_sequences的结果是个ndarray,每个元素都是相同长度的list
        genres_list = pad_sequences(
            genres_list,
            maxlen=max_len,
            padding='post',
        )

        print('genres_list:', max_len, type(genres_list))
        # print(genres_list)
        # print(data[fea_name].head())
        model_input[fea_name] = genres_list

        var_len_fea = VarLenSparseFeat(
            SparseFeat(fea_name,
                       vocabulary_size=len(key2index) + 1,
                       embedding_dim=4),
            maxlen=max_len,
            length_name="seq_length",
            combiner='mean'
        )  # Notice : value 0 is for padding for sequence input feature
        varlen_feature_columns.append(var_len_fea)

    linear_feature_columns = fixlen_feature_columns + varlen_feature_columns
    dnn_feature_columns = fixlen_feature_columns + varlen_feature_columns
    feature_columns = dnn_feature_columns
    print('train_model_input:', type(model_input), model_input.keys())

    return feature_columns, model_input, data[target].values


if __name__ == "__main__":
    feature_columns, train_input, label = train_data_process()

    print('-' * 20, 'sample split', '-' * 20)
    val_ratio = 0.1
    sample_cnt = len(label)
    split_at = int(sample_cnt * (1 - val_ratio))
    fea_name_seq = list(train_input.keys())
    value_list = [train_input[k] for k in fea_name_seq]

    print(type(value_list), len(value_list))
    x, val_x = (slice_arrays(value_list, 0,
                             split_at), slice_arrays(value_list, split_at))
    y, val_y = (slice_arrays(label, 0,
                             split_at), slice_arrays(label, split_at))
    train_data = {k: x[fea_name_seq.index(k)] for k in fea_name_seq}
    val_data = {k: val_x[fea_name_seq.index(k)] for k in fea_name_seq}

    device = 'cpu'
    use_cuda = True
    if use_cuda and torch.cuda.is_available():
        print('cuda ready...')
        device = 'cuda:0'

    model = DIN(feature_columns,
                behavior_feature_list,
                device=device,
                att_weight_normalization=True)
    model.compile('adagrad',
                  'binary_crossentropy',
                  metrics=['binary_crossentropy', 'auc'])
    #注意这里的train_input必须是个dict，不能是dataframe
    history = model.fit(train_data,
                        y,
                        batch_size=128,
                        epochs=1,
                        verbose=2,
                        validation_split=0.1)

    pred_ans = model.predict(val_data, 256)
    print('-' * 20, 'validation', '-' * 20)
    print("test LogLoss", round(log_loss(val_y, pred_ans), 4))
    print("test AUC", round(roc_auc_score(val_y, pred_ans), 4))