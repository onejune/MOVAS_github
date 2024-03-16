# -*- coding: utf-8 -*-
import os, sys
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
import pandas as pd
import torch
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, MinMaxScaler

cur_path = os.path.realpath(__file__)
cur_dir = os.path.dirname(cur_path)
parent_dir = os.path.dirname(cur_dir)
sys.path.append(parent_dir)

from deepctr_torch.inputs import SparseFeat, DenseFeat, get_feature_names
from deepctr_torch.models import *

if __name__ == "__main__":
    data = pd.read_csv(cur_dir + '/data/criteo_sample.txt')

    sparse_features = ['C' + str(i) for i in range(1, 27)]  #文本特征
    dense_features = ['I' + str(i) for i in range(1, 14)]  #数值特征

    data[sparse_features] = data[sparse_features].fillna('-1',
                                                         )  #填充缺失值，文本特征用-1填充
    data[dense_features] = data[dense_features].fillna(0, )
    target = ['label']

    # 1.Label Encoding for sparse features,and do simple Transformation for dense features
    for feat in sparse_features:
        lbe = LabelEncoder()
        data[feat] = lbe.fit_transform(data[feat])
    mms = MinMaxScaler(feature_range=(0, 1))
    data[dense_features] = mms.fit_transform(
        data[dense_features])  #数值特征归一化到[0,1]

    # 2.count #unique features for each sparse field,and record dense feature field name

    fixlen_feature_columns = [
        SparseFeat(feat, data[feat].nunique()) for feat in sparse_features
    ] + [DenseFeat(
        feat,
        1,
    ) for feat in dense_features]

    dnn_feature_columns = fixlen_feature_columns
    linear_feature_columns = fixlen_feature_columns

    feature_names = get_feature_names(linear_feature_columns +
                                      dnn_feature_columns)

    # 3.generate input data for model

    train, test = train_test_split(data, test_size=0.2, random_state=2020)
    train_model_input = {name: train[name] for name in feature_names}
    test_model_input = {name: test[name] for name in feature_names}

    # 4.Define Model,train,predict and evaluate

    device = 'cpu'
    use_cuda = True
    if use_cuda and torch.cuda.is_available():
        print('cuda ready...')
        device = 'cuda:0'

    model = DeepFM(linear_feature_columns=linear_feature_columns,
                   dnn_feature_columns=dnn_feature_columns,
                   task='binary',
                   l2_reg_embedding=1e-5,
                   device=device)

    model.compile(
        "adagrad",
        "binary_crossentropy",
        metrics=["binary_crossentropy", "auc"],
    )

    history = model.fit(train_model_input,
                        train[target].values,
                        batch_size=32,
                        epochs=50,
                        verbose=2,
                        validation_split=0.2)
    pred_ans = model.predict(test_model_input, 256)
    print("")
    print("test LogLoss", round(log_loss(test[target].values, pred_ans), 4))
    print("test AUC", round(roc_auc_score(test[target].values, pred_ans), 4))
