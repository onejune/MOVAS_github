import yaml
import torch
import argparse
import mindalpha as ma
import torch.nn.functional as F

def parse_args():
    parser = argparse.ArgumentParser(description='two tower for dsp in juno')
    parser.add_argument('-c', '--job-config', type=str,
                        help="job config YAML file path;")
    return parser.parse_args()

class DSSMDense(torch.nn.Module):
    def __init__(self, emb_out_size):
        super().__init__()
        self._emb_bn = ma.nn.Normalization(emb_out_size, momentum=0.01, eps=1e-5)
        self._d1 = torch.nn.Linear(emb_out_size, 1024)
        self._d2 = torch.nn.Linear(1024, 512)
        self._d3 = torch.nn.Linear(512, 64)

    def forward(self, x):
        x = self._emb_bn(x)
        x = F.relu(self._d1(x))
        x = F.relu(self._d2(x))
        x = self._d3(x)
        return x

class SimilarityModule(torch.nn.Module):
    def __init__(self):
        super().__init__()
    
    def forward(self, x, y):
        z = torch.sum(x * y, dim=1).reshape(-1, 1)
        s = torch.sigmoid(z)
        return s

class UserModule(torch.nn.Module):
    def __init__(self, column_name_path, combine_schema_path, emb_size = 16):
        super().__init__()
        self._embedding_size = emb_size
        self._column_name_path = column_name_path
        self._combine_schema_path = combine_schema_path
        self._sparse = ma.EmbeddingSumConcat(self._embedding_size, self._column_name_path, self._combine_schema_path)
        self._sparse.updater = ma.FTRLTensorUpdater(alpha = 0.01)
        self._sparse.initializer = ma.NormalTensorInitializer(var = 0.0001)
        self._sparse.output_batchsize1_if_only_level0 = True
        self._emb_out_size = self._sparse.feature_count * self._embedding_size
        self._dense = DSSMDense(self._emb_out_size)

    def forward(self, x):
        x = self._sparse(x)
        x = self._dense(x)
        return x

class ItemModule(torch.nn.Module):
    def __init__(self, column_name_path, db_column_name_path, combine_schema_path, emb_size = 16):
        super().__init__()
        self._embedding_size = emb_size
        self._column_name_path = column_name_path
        self._db_column_name_path = db_column_name_path
        self._combine_schema_path = combine_schema_path
        self._sparse = ma.EmbeddingSumConcat(self._embedding_size, self._column_name_path, self._combine_schema_path)
        self._sparse.updater = ma.FTRLTensorUpdater(alpha = 0.01)
        self._sparse.initializer = ma.NormalTensorInitializer(var = 0.0001)
        self._emb_out_size = self._sparse.feature_count * self._embedding_size
        self._dense = DSSMDense(self._emb_out_size)

    def forward(self, x):
        x = self._sparse(x)
        x = self._dense(x)
        return x

class ItemEmbeddingModule(torch.nn.Module):
    def __init__(self, column_name_path, db_column_name_path, combine_schema_path, emb_size = 64):
        super().__init__()
        self._embedding_size = emb_size
        self._column_name_path = column_name_path
        self._db_column_name_path = db_column_name_path
        self._combine_schema_path = combine_schema_path
        self._sparse = ma.EmbeddingSumConcat(self._embedding_size, self._column_name_path, self._combine_schema_path,
                                             alternative_column_name_file_path = self._db_column_name_path)

        self._sparse.use_nan_fill=True
    def forward(self, x):
        x = self._sparse(x)
        return x


if __name__ == "__main__":
    args = parse_args()
    job_config = {}
    with open(args.job_config) as fin:
        job_config = yaml.full_load(fin)

    learning_conf = job_config.get('learning_conf')
    batch_size = learning_conf.get('batch_size')
    worker_count = learning_conf.get('worker_count')
    server_count = learning_conf.get('server_count')

    data_conf = job_config.get('data_conf')
    train_dataset_path = data_conf.get('train_dataset_path')
    item_dataset_path = data_conf.get('item_dataset_path')
    user_dataset_path = data_conf.get('user_dataset_path')
    model_in_path = data_conf.get('model_in_path')
    model_out_path = data_conf.get('model_out_path')
    model_export_path = data_conf.get('model_export_path')
    model_version = data_conf.get('model_version')
    experiment_name = data_conf.get('experiment_name')

    print('batch_size',batch_size)
    print('train_dataset_path',train_dataset_path)
    print('item_dataset_path',item_dataset_path)
    print('model_in_path',model_in_path )
    print('model_out_path',model_out_path )

    feature_conf = job_config.get('feature_conf')
    user_column_name = feature_conf.get('user_column_name')
    user_combine_schema = feature_conf.get('user_combine_schema')
    item_column_name = feature_conf.get('item_column_name')
    itemdb_column_name = feature_conf.get('itemdb_column_name')
    item_combine_schema = feature_conf.get('item_combine_schema')
    itemdb_combine_schema = feature_conf.get('itemdb_combine_schema')


    spark_session = ma.spark.get_session(batch_size = batch_size,
                                         worker_count = worker_count,
                                         server_count = server_count)
    
    # ma.TwoTowerRankingModule
    module = ma.TwoTowerRankingModule(UserModule(user_column_name, user_combine_schema),
                                  ItemModule(item_column_name, itemdb_column_name, item_combine_schema),
                                  ItemEmbeddingModule(item_column_name, itemdb_column_name, itemdb_combine_schema),
                                  SimilarityModule())

    train_dataset = ma.input.read_s3_csv(spark_session, train_dataset_path, True, worker_count)
    item_dataset = ma.input.read_s3_csv(spark_session, item_dataset_path, True, worker_count)
    user_dataset = ma.input.read_s3_csv(spark_session, user_dataset_path, True, worker_count)
    
    estimator = ma.TwoTowerRankingEstimator(module = module,
                                        item_dataset = item_dataset,
                                        worker_count = worker_count,
                                        server_count = server_count,
                                        model_in_path = model_in_path,
                                        model_out_path = model_out_path,
                                        model_export_path = model_export_path,
                                        model_version = model_version,
                                        experiment_name = experiment_name,
                                        input_label_column_index = 1) 

    estimator.updater = ma.AdamTensorUpdater(1e-5)
    model = estimator.fit(train_dataset)
    # result = model.transform(item_dataset)
