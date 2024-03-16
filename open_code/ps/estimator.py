import io
import torch
import pyspark.ml.base
import cloudpickle
from .agent import Agent
from .model import Model
from .updater import TensorUpdater
from .updater import AdamTensorUpdater
from .distributed_trainer import DistributedTrainer
from .ps_launcher import PSLauncher

class PyTorchAgent(Agent):
    def __init__(self):
        super().__init__()
        self.module = None
        self.updater = None
        self.dataset = None
        self.model = None
        self.trainer = None
        self.is_training_mode = None
        self.validation_result = None
        self.model_in_path = None
        self.model_out_path = None
        self.model_export_path = None
        self.model_version = None
        self.experiment_name = None
        self.max_sparse_feature_age = None
        self.criterion_update_interval = None
        self.input_label_column_index = None
        self.output_label_column_name = None
        self.output_label_column_type = None
        self.output_prediction_column_name = None
        self.output_prediction_column_type = None
        self.minibatch_id = 0

    def run(self):
        self.distribute_module()
        self.distribute_updater()
        self.start_workers()
        self.feed_dataset()
        self.collect_module()
        self.stop_workers()

    def distribute_module(self):
        buf = io.BytesIO()
        torch.save(self.module, buf, pickle_module=cloudpickle)
        module = buf.getvalue()
        rdd = self.spark_context.parallelize(range(self.worker_count), self.worker_count)
        rdd.barrier().mapPartitions(lambda _: __class__._distribute_module(module, _)).collect()

    @classmethod
    def _distribute_module(cls, module, _):
        buf = io.BytesIO(module)
        module = torch.load(buf)
        self = __class__.get_instance()
        self.module = module
        return _

    def distribute_updater(self):
        updater = self.updater
        rdd = self.spark_context.parallelize(range(self.worker_count), self.worker_count)
        rdd.barrier().mapPartitions(lambda _: __class__._distribute_updater(updater, _)).collect()

    @classmethod
    def _distribute_updater(cls, updater, _):
        self = __class__.get_instance()
        self.updater = updater
        return _

    def collect_module(self):
        if self.is_training_mode:
            rdd = self.spark_context.parallelize(range(self.worker_count), self.worker_count)
            state, = rdd.barrier().mapPartitions(lambda _: __class__._collect_module(_)).collect()
            self.module.load_state_dict(state)

    @classmethod
    def _collect_module(cls, _):
        self = __class__.get_instance()
        self.model.sync()
        if self.rank != 0:
            return ()
        state = self.module.state_dict()
        return state,

    def setup_model(self):
        self.model = Model.wrap(self, self.module)

    def setup_trainer(self):
        self.trainer = DistributedTrainer(self.model, updater=self.updater)
        self.trainer.initialize()

    def worker_start(self):
        self.setup_model()
        self.setup_trainer()
        self.load_model()

    def load_model(self):
        if self.model_in_path is not None:
            print('\033[38;5;196mloading model from %s\033[m' % self.model_in_path)
            self.trainer.load(self.model_in_path)

    def save_model(self):
        self.model.prune_old(self.max_sparse_feature_age)
        if self.model_out_path is not None:
            print('\033[38;5;196msaving model to %s\033[m' % self.model_out_path)
            self.trainer.save(self.model_out_path)

    def export_model(self):
        if self.model_export_path is not None:
            print('\033[38;5;196mexporting model to %s\033[m' % self.model_export_path)
            self.model.eval()
            self.model.model_version = self.model_version
            self.model.experiment_name = self.experiment_name
            self.model.prune_small(0.0)
            self.model.export(self.model_export_path)

    def worker_stop(self):
        # Make sure the final criterion buffers are pushed.
        self.push_criterion()
        if self.is_training_mode:
            self.save_model()
            self.export_model()

    def feed_dataset(self):
        if self.is_training_mode:
            self.feed_training_dataset()
        else:
            self.feed_validation_dataset()

    def feed_training_dataset(self):
        df = self.dataset.select(self.feed_training_minibatch()(*self.dataset.columns).alias('train'))
        df.groupBy(df[0]).count().show()

    def feed_validation_dataset(self):
        df = self.dataset.withColumn(self.output_prediction_column_name,
                                     self.feed_validation_minibatch()(*self.dataset.columns))
        df = df.withColumn(self.output_label_column_name,
                           df[self.input_label_column_index].cast(self.output_label_column_type))
        df = df.withColumn(self.output_prediction_column_name,
                           df[self.output_prediction_column_name].cast(self.output_prediction_column_type))
        self.validation_result = df
        # PySpark DataFrame & RDD is lazily evaluated.
        # We must call ``cache`` here otherwise PySpark will try to reevaluate
        # ``validation_result`` when we use it, which is not possible as the
        # PS system has been shutdown.
        df.cache()
        df.groupBy(df[0]).count().show()

    def feed_training_minibatch(self):
        from pyspark.sql.types import FloatType
        from pyspark.sql.functions import pandas_udf
        @pandas_udf(returnType=FloatType())
        def _feed_training_minibatch(*minibatch):
            self = __class__.get_instance()
            result = self.train_minibatch(minibatch)
            result = self.process_minibatch_result(minibatch, result)
            return result
        return _feed_training_minibatch

    def feed_validation_minibatch(self):
        from pyspark.sql.types import FloatType
        from pyspark.sql.functions import pandas_udf
        @pandas_udf(returnType=FloatType())
        def _feed_validation_minibatch(*minibatch):
            self = __class__.get_instance()
            result = self.validate_minibatch(minibatch)
            result = self.process_minibatch_result(minibatch, result)
            return result
        return _feed_validation_minibatch

    def preprocess_minibatch(self, minibatch):
        import numpy as np
        import pandas as pd
        ndarrays = [col.values for col in minibatch]
        labels = minibatch[self.input_label_column_index].values.astype(np.int64)
        return ndarrays, labels

    def process_minibatch_result(self, minibatch, result):
        import pandas as pd
        minibatch_size = len(minibatch[self.input_label_column_index])
        if result is None:
            result = [0.0] * minibatch_size
        if len(result) != minibatch_size:
            message = "result length (%d) and " % len(result)
            message += "minibatch size (%d) mismatch" % minibatch_size
            raise RuntimeError(message)
        if not isinstance(result, pd.Series):
            result = pd.Series(result)
        return result

    def train_minibatch(self, minibatch):
        self.model.train()
        ndarrays, labels = self.preprocess_minibatch(minibatch)
        predictions = self.model(ndarrays)
        labels = torch.from_numpy(labels).reshape(-1, 1)
        loss = self.compute_loss(predictions, labels)
        self.trainer.train(loss)
        self.update_progress(predictions, labels)

    def validate_minibatch(self, minibatch):
        self.model.eval()
        ndarrays, labels = self.preprocess_minibatch(minibatch)
        predictions = self.model(ndarrays)
        labels = torch.from_numpy(labels).reshape(-1, 1)
        loss = self.compute_loss(predictions, labels)
        self.update_progress(predictions, labels)
        return predictions.detach().reshape(-1)

    def compute_loss(self, predictions, labels):
        from .loss_utils import log_loss
        return log_loss(predictions, labels) / labels.shape[0]

    def update_progress(self, predictions, labels):
        self.minibatch_id += 1
        self.update_criterion(predictions, labels)
        if self.minibatch_id % self.criterion_update_interval == 0:
            self.push_criterion()

class PyTorchLauncher(PSLauncher):
    def __init__(self):
        super().__init__()
        self.module = None
        self.updater = None
        self.dataset = None
        self.worker_count = None
        self.server_count = None
        self.agent_class = None
        self.agent_object = None
        self.is_training_mode = None
        self.model_in_path = None
        self.model_out_path = None
        self.model_export_path = None
        self.model_version = None
        self.experiment_name = None
        self.max_sparse_feature_age = None
        self.criterion_update_interval = None
        self.input_label_column_index = None
        self.output_label_column_name = None
        self.output_label_column_type = None
        self.output_prediction_column_name = None
        self.output_prediction_column_type = None
        self.extra_agent_attributes = None

    def _get_agent_class(self):
        return self.agent_class

    def _initialize_agent(self, agent):
        agent.module = self.module
        agent.updater = self.updater
        agent.dataset = self.dataset
        self.agent_object = agent

    def launch(self):
        self._worker_count = self.worker_count
        self._server_count = self.server_count
        self._agent_attributes = dict()
        self._agent_attributes['is_training_mode'] = self.is_training_mode
        self._agent_attributes['model_in_path'] = self.model_in_path
        self._agent_attributes['model_out_path'] = self.model_out_path
        self._agent_attributes['model_export_path'] = self.model_export_path
        self._agent_attributes['model_version'] = self.model_version
        self._agent_attributes['experiment_name'] = self.experiment_name
        self._agent_attributes['max_sparse_feature_age'] = self.max_sparse_feature_age
        self._agent_attributes['criterion_update_interval'] = self.criterion_update_interval
        self._agent_attributes['input_label_column_index'] = self.input_label_column_index
        self._agent_attributes['output_label_column_name'] = self.output_label_column_name
        self._agent_attributes['output_label_column_type'] = self.output_label_column_type
        self._agent_attributes['output_prediction_column_name'] = self.output_prediction_column_name
        self._agent_attributes['output_prediction_column_type'] = self.output_prediction_column_type
        self._agent_attributes.update(self.extra_agent_attributes)
        self._keep_session = True
        self.launch_agent()

class PyTorchHelperMixin(object):
    def __init__(self,
                 module,
                 updater=AdamTensorUpdater(1e-5),
                 worker_count=100,
                 server_count=100,
                 agent_class=PyTorchAgent,
                 model_in_path=None,
                 model_out_path=None,
                 model_export_path=None,
                 model_version=None,
                 experiment_name=None,
                 max_sparse_feature_age=15,
                 criterion_update_interval=10,
                 input_label_column_index=1,
                 output_label_column_name='label',
                 output_label_column_type='double',
                 output_prediction_column_name='rawPrediction',
                 output_prediction_column_type='double',
                 **kwargs):
        super().__init__()
        self.module = module
        self.updater = updater
        self.worker_count = worker_count
        self.server_count = server_count
        self.agent_class = agent_class
        self.model_in_path = model_in_path
        self.model_out_path = model_out_path
        self.model_export_path = model_export_path
        self.model_version = model_version
        self.experiment_name = experiment_name
        self.max_sparse_feature_age = max_sparse_feature_age
        self.criterion_update_interval = criterion_update_interval
        self.input_label_column_index = input_label_column_index
        self.output_label_column_name = output_label_column_name
        self.output_label_column_type = output_label_column_type
        self.output_prediction_column_name = output_prediction_column_name
        self.output_prediction_column_type = output_prediction_column_type
        self.extra_agent_attributes = kwargs
        self.final_criterion = None

    def _check_properties(self):
        if not isinstance(self.module, torch.nn.Module):
            raise TypeError(f"module must be torch.nn.Module; {self.module!r} is invalid")
        if not isinstance(self.updater, TensorUpdater):
            raise TypeError(f"updater must be ps.TensorUpdater; {self.updater!r} is invalid")
        if not isinstance(self.worker_count, int) or self.worker_count <= 0:
            raise TypeError(f"worker_count must be positive integer; {self.worker_count!r} is invalid")
        if not isinstance(self.server_count, int) or self.server_count <= 0:
            raise TypeError(f"server_count must be positive integer; {self.server_count!r} is invalid")
        if not issubclass(self.agent_class, PyTorchAgent):
            raise TypeError(f"agent_class must be subclass of ps.PyTorchAgent; {self.agent_class!r} is invalid")
        if self.model_in_path is not None and not isinstance(self.model_in_path, str):
            raise TypeError(f"model_in_path must be string; {self.model_in_path!r} is invalid")
        if self.model_out_path is not None and not isinstance(self.model_out_path, str):
            raise TypeError(f"model_out_path must be string; {self.model_out_path!r} is invalid")
        if self.model_export_path is not None and not isinstance(self.model_export_path, str):
            raise TypeError(f"model_export_path must be string; {self.model_export_path!r} is invalid")
        if self.model_version is not None and not isinstance(self.model_version, str):
            raise TypeError(f"model_version must be string; {self.model_version!r} is invalid")
        if self.experiment_name is not None and not isinstance(self.experiment_name, str):
            raise TypeError(f"experiment_name must be string; {self.experiment_name!r} is invalid")
        if not isinstance(self.max_sparse_feature_age, int) or self.max_sparse_feature_age <= 0:
            raise TypeError(f"max_sparse_feature_age must be positive integer; {self.max_sparse_feature_age!r} is invalid")
        if not isinstance(self.criterion_update_interval, int) or self.criterion_update_interval <= 0:
            raise TypeError(f"criterion_update_interval must be positive integer; {self.criterion_update_interval!r} is invalid")
        if not isinstance(self.input_label_column_index, int) or self.input_label_column_index < 0:
            raise TypeError(f"input_label_column_index must be non-negative integer; {self.input_label_column_index!r} is invalid")
        if not isinstance(self.output_label_column_name, str):
            raise TypeError(f"output_label_column_name must be string; {self.output_label_column_name!r} is invalid")
        if not isinstance(self.output_label_column_type, str):
            raise TypeError(f"output_label_column_type must be string; {self.output_label_column_type!r} is invalid")
        if not isinstance(self.output_prediction_column_name, str):
            raise TypeError(f"output_prediction_column_name must be string; {self.output_prediction_column_name!r} is invalid")
        if not isinstance(self.output_prediction_column_type, str):
            raise TypeError(f"output_prediction_column_type must be string; {self.output_prediction_column_type!r} is invalid")
        if self.model_export_path is not None and (self.model_version is None or self.experiment_name is None):
            raise RuntimeError("model_version and experiment_name are required when model_export_path is specified")

    def _create_launcher(self, dataset, is_training_mode):
        self._check_properties()
        launcher = PyTorchLauncher()
        launcher.module = self.module
        launcher.updater = self.updater
        launcher.dataset = dataset
        launcher.worker_count = self.worker_count
        launcher.server_count = self.server_count
        launcher.agent_class = self.agent_class
        launcher.is_training_mode = is_training_mode
        launcher.model_in_path = self.model_in_path
        launcher.model_out_path = self.model_out_path
        launcher.model_export_path = self.model_export_path
        launcher.model_version = self.model_version
        launcher.experiment_name = self.experiment_name
        launcher.max_sparse_feature_age = self.max_sparse_feature_age
        launcher.criterion_update_interval = self.criterion_update_interval
        launcher.input_label_column_index = self.input_label_column_index
        launcher.output_label_column_name = self.output_label_column_name
        launcher.output_label_column_type = self.output_label_column_type
        launcher.output_prediction_column_name = self.output_prediction_column_name
        launcher.output_prediction_column_type = self.output_prediction_column_type
        launcher.extra_agent_attributes = self.extra_agent_attributes
        return launcher

    def _create_model(self, module):
        model = PyTorchModel(module,
                             updater=self.updater,
                             worker_count=self.worker_count,
                             server_count=self.server_count,
                             agent_class=self.agent_class,
                             model_in_path=self.model_out_path,
                             criterion_update_interval=self.criterion_update_interval,
                             input_label_column_index=self.input_label_column_index,
                             output_label_column_name=self.output_label_column_name,
                             output_label_column_type=self.output_label_column_type,
                             output_prediction_column_name=self.output_prediction_column_name,
                             output_prediction_column_type=self.output_prediction_column_type,
                             **self.extra_agent_attributes)
        return model

class PyTorchModel(PyTorchHelperMixin, pyspark.ml.base.Model):
    def _transform(self, dataset):
        launcher = self._create_launcher(dataset, False)
        launcher.launch()
        result = launcher.agent_object.validation_result
        self.final_criterion = launcher.agent_object._criterion
        return result

class PyTorchEstimator(PyTorchHelperMixin, pyspark.ml.base.Estimator):
    def _check_properties(self):
        super()._check_properties()
        if self.model_out_path is None:
            # ``model_out_path`` must be specified otherwise an instance of PyTorchModel
            # does not known where to load the model from. This is due to the approach
            # we implement ``_fit`` and ``_transform`` that the PS system will be
            # shutdown when ``_fit`` finishes and restarted in ``_transform``.
            # Later, we may refine the implementation of PS to remove this limitation.
            raise RuntimeError("model_out_path of estimator must be specified")

    def _fit(self, dataset):
        launcher = self._create_launcher(dataset, True)
        launcher.launch()
        module = launcher.agent_object.module
        module.eval()
        model = self._create_model(module)
        self.final_criterion = launcher.agent_object._criterion
        return model
