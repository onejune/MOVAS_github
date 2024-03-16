import ps
import torch
from pyspark.sql.types import *
import numpy as np


class NNRankAgent(ps.Agent):
    def run(self):
        self.start_workers()
        if self.dataset_path!='':
            print('now train '+ self.dataset_path )
            self.feed_training_dataset(self.dataset_path)
        if self.val_dataset_path !='':
            print('now validation '+ self.val_dataset_path )
            print('now validation_output'+ self.val_dataset_output_path )
            self.feed_validation_dataset(self.val_dataset_path,self.val_dataset_output_path)
            pass
        self.stop_workers()

    def validate_minibatch(self, minibatch):
        ndarrays, labels = self.preprocess_minibatch(minibatch)
        predictions, match = self._net(ndarrays)
        labels = torch.from_numpy(labels).reshape(-1, 1)
        loss = self.log_loss(predictions, labels)
        self._minibatch_id += 1
        self.update_criterion(predictions, labels)
        if self._minibatch_id % 10 == 0:
            self.push_criterion()
        return predictions.clone().detach()

    def feed_validation_minibatch(self):
        from pyspark.sql.types import FloatType
        from pyspark.sql.functions import pandas_udf
        @pandas_udf(returnType=StringType())
        def _feed_validation_minibatch(minibatch):
            self = __class__.get_instance()
            result = self.validate_minibatch(minibatch)
            result = self.process_minibatch_result(minibatch, result)
            for i in minibatch.index:
                data_pre_np_array_str=np.concatenate((minibatch.at[i],np.array([str(result.at[i][0].item())])))
                minibatch.at[i]='\002'.join(['' if x is None else x for x in data_pre_np_array_str])
            return minibatch
        return _feed_validation_minibatch 

    def feed_validation_dataset(self, dataset_path,output_path, nepoches=1):
        for epoch in range(nepoches):
            df = self.load_dataset(dataset_path)
            df = df.select(self.feed_validation_minibatch()(*df.columns).alias('validate'))
            df.write.text(output_path)
    def nansum(self, x):
        return torch.where(torch.isnan(x), torch.zeros_like(x), x).sum()

    def log_loss(self, yhat, y):
        return self.nansum(-(y * (yhat+1e-12).log() + (1 - y) * (1 - yhat+1e-12).log()))

    def worker_start(self):
        from nn_rank import NNRankModel
        self._net = ps.SparseModel(self, NNRankModel(self.column_name, self.combine_schema, 
                                    self.combine_schema_user, self.combine_schema_item))
        if not self.isTraining:
            self._net._module.eval()
        dense_updater = ps.AdamTensorUpdater(1e-5)
        dense_initializer = ps.XavierTensorInitializer(distribution_type='uniform')
        self._trainer = ps.DistributedTrainer(self._net, updater=dense_updater, initializer=dense_initializer)
        self._trainer.initialize()
        if self.model_in_path is not None and self.model_in_path !='':
            print('\033[38;5;196mloading model from %s\033[m' % self.model_in_path)
            self._trainer.load(self.model_in_path,keep_meta=True)
        self._minibatch_id = 0
        import os
        import threading
        ident = threading.current_thread().ident
        print('\033[38;5;046mworker: id=%d, pid=%d, ident=0x%x\033[m' % (self.rank, os.getpid(), ident))

    def worker_stop(self):
        import os
        import threading
        ident = threading.current_thread().ident
        self._net.prune_old(15)
        if self.model_out_path is not None and self.model_out_path !='':
            print('\033[38;5;196msaving model to %s\033[m' % self.model_out_path)
            self._trainer.save(self.model_out_path)
        print('\033[38;5;196mworker: id=%d, pid=%d, ident=0x%x stopped\033[m' % (self.rank, os.getpid(), ident))
        if self.model_export_path is None or  self.model_export_path =='':
            return
        self._net.eval()
        self._net.experiment_name = 'mp_v1'
        self._net.prune_small(0.0)
        self._net.export(self.model_export_path)


    def load_dataset(self, dataset_path):
        from pyspark.sql import functions as F
        from ps import input
        if dataset_path.startswith('s3://'):
            df = input.read_s3_csv(self.spark_session, dataset_path, shuffle=True, num_workers=self.worker_count)
        elif dataset_path.startswith('kudu://'):
            df = input.read_kudu(self.spark_session, dataset_path,
                                 self.column_name, sql=self.sql,
                                 condition_select_conf=self.condition_select_conf,
                                 shuffle=True, num_workers=self.worker_count)
        else:
            raise ValueError('unsupported dataset_path: %s, should be either s3:// or kudu://' % dataset_path)
        return df.select(F.array(df.columns))

    def train_minibatch(self, minibatch):
        ndarrays, labels = self.preprocess_minibatch(minibatch)
        predict, match = self._net(ndarrays)
        labels = torch.from_numpy(labels).reshape(-1, 1)
        # deep mp
        predict_loss = self.log_loss(predict, labels.float())
        match_loss = self.log_loss(match, labels.float())
        alpha = 0.3
        loss = predict_loss + alpha * match_loss
        self._minibatch_id += 1
        self.update_criterion(predict, labels)
        if self._minibatch_id % 10 == 0:
            self.push_criterion()
        self._trainer.train(loss)

