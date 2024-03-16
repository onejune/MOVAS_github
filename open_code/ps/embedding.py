import asyncio
import os
import numpy
import torch
import feature_extraction.fe
from .name_utils import is_valid_qualified_name
from .updater import TensorUpdater
from .initializer import TensorInitializer

class EmbeddingOperator(torch.nn.Module):
    def __init__(self,
                 embedding_size=None,
                 column_name_file_path=None,
                 combine_schema_file_path=None,
                 delimiter=None,
                 dtype=torch.float32,
                 requires_grad=True,
                 updater=None,
                 initializer=None):
        if embedding_size is not None:
            if not isinstance(embedding_size, int) or embedding_size <= 0:
                raise TypeError(f"embedding_size must be positive integer; {embedding_size!r} is invalid")
        if column_name_file_path is not None:
            if not isinstance(column_name_file_path, str) or not os.path.isfile(column_name_file_path):
                raise RuntimeError(f"column name file {column_name_file_path!r} not found")
        if combine_schema_file_path is not None:
            if not isinstance(combine_schema_file_path, str) or not os.path.isfile(combine_schema_file_path):
                raise RuntimeError(f"combine schema file {combine_schema_file_path!r} not found")
        if delimiter is not None:
            if not isinstance(delimiter, str) or len(delimiter) != 1:
                raise TypeError(f"delimiter must be string of length 1; {delimiter!r} is invalid")
        if dtype not in (torch.float32, torch.float64):
            raise TypeError(f"dtype must be one of: torch.float32, torch.float64; {dtype!r} is invalid")
        if updater is not None:
            if not isinstance(updater, TensorUpdater):
                raise TypeError(f"updater must be ps.TensorUpdater; {updater!r} is invalid")
        if initializer is not None:
            if not isinstance(initializer, TensorInitializer):
                raise TypeError(f"initializer must be ps.TensorInitializer; {initializer!r} is invalid")
        super().__init__()
        self._embedding_size = embedding_size
        self._column_name_file_path = column_name_file_path
        self._combine_schema_file_path = combine_schema_file_path
        self._delimiter = delimiter
        self._dtype = dtype
        self._requires_grad = requires_grad
        self._updater = updater
        self._initializer = initializer
        self._distributed_tensor = None
        self._combine_schema_source = None
        self._combine_schema = None
        self._feature_extractor = feature_extraction.fe.FeatureExtraction()
        if self._column_name_file_path is not None and self._combine_schema_file_path is not None:
            self._load_combine_schema()
        self._clean()

    @torch.jit.unused
    def _load_combine_schema(self):
        if self._combine_schema is not None:
            raise RuntimeError("combine schema has been loaded")
        column_name_file_path = self._checked_get_column_name_file_path()
        combine_schema_file_path = self._checked_get_combine_schema_file_path()
        self._combine_schema = feature_extraction.fe.CombineSchema(column_name_file_path, combine_schema_file_path)
        self._combine_schema_source = self._combine_schema.schema_source
        string = f"\033[32mcolumn name file \033[m{column_name_file_path!r} "
        string += f"\033[32mand combine schema file \033[m{combine_schema_file_path!r} "
        string += f"\033[32mloaded\033[m"
        print(string)

    @torch.jit.unused
    def _ensure_combine_schema_loaded(self):
        if self._combine_schema is None:
            self._load_combine_schema()

    def __del__(self):
        if self._combine_schema is not None:
            self._combine_schema.release()
            self._combine_schema = None

    def __repr__(self):
        args = []
        if self._embedding_size is not None:
            args.append(f"{self._embedding_size}")
        if self._column_name_file_path is not None:
            args.append(f"column_name_file_path={self._column_name_file_path!r}")
        if self._combine_schema_file_path is not None:
            args.append(f"combine_schema_file_path={self._combine_schema_file_path!r}")
        if self._delimiter is not None:
            args.append(f"delimiter={self._delimiter!r}")
        if self._dtype is not None and self._dtype is not torch.float32:
            args.append(f"dtype={self._dtype}")
        if not self._requires_grad:
            args.append("requires_grad=False")
        return f"{self.__class__.__name__}({', '.join(args)})"

    @property
    @torch.jit.unused
    def embedding_size(self):
        return self._embedding_size

    @embedding_size.setter
    @torch.jit.unused
    def embedding_size(self, value):
        if value is not None:
            if not isinstance(value, int) or value <= 0:
                raise TypeError(f"embedding_size must be positive integer; {value!r} is invalid")
        if self._embedding_size is not None:
            raise RuntimeError(f"can not reset embedding_size {self._embedding_size} to {value!r}")
        self._embedding_size = value

    @torch.jit.unused
    def _checked_get_embedding_size(self):
        if self._embedding_size is None:
            raise RuntimeError("embedding_size is not set")
        return self._embedding_size

    @property
    @torch.jit.unused
    def column_name_file_path(self):
        return self._column_name_file_path

    @column_name_file_path.setter
    @torch.jit.unused
    def column_name_file_path(self, value):
        if value is not None:
            if not isinstance(value, str) or not os.path.isfile(value):
                raise RuntimeError(f"column name file {value!r} not found")
        if self._column_name_file_path is not None:
            raise RuntimeError(f"can not reset column_name_file_path {self._column_name_file_path!r} to {value!r}")
        self._column_name_file_path = value

    @torch.jit.unused
    def _checked_get_column_name_file_path(self):
        if self._column_name_file_path is None:
            raise RuntimeError("column_name_file_path is not set")
        return self._column_name_file_path

    @property
    @torch.jit.unused
    def combine_schema_file_path(self):
        return self._combine_schema_file_path

    @combine_schema_file_path.setter
    @torch.jit.unused
    def combine_schema_file_path(self, value):
        if value is not None:
            if not isinstance(value, str) or not os.path.isfile(value):
                raise RuntimeError(f"combine schema file {value!r} not found")
        if self._combine_schema_file_path is not None:
            raise RuntimeError(f"can not reset combine_schema_file_path {self._combine_schema_file_path!r} to {value!r}")
        self._combine_schema_file_path = value

    @torch.jit.unused
    def _checked_get_combine_schema_file_path(self):
        if self._combine_schema_file_path is None:
            raise RuntimeError("combine_schema_file_path is not set")
        return self._combine_schema_file_path

    @property
    @torch.jit.unused
    def delimiter(self):
        return self._delimiter

    @delimiter.setter
    @torch.jit.unused
    def delimiter(self, value):
        if value is not None:
            if not isinstance(value, str) or len(value) != 1:
                raise TypeError(f"delimiter must be string of length 1; {value!r} is invalid")
        if self._delimiter is not None:
            raise RuntimeError(f"can not reset delimiter {self._delimiter!r} to {value!r}")
        self._delimiter = value

    @torch.jit.unused
    def _checked_get_delimiter(self):
        if self._delimiter is None:
            return '\001'
        return self._delimiter

    @property
    @torch.jit.unused
    def feature_count(self):
        schema = self._combine_schema
        if schema is None:
            raise RuntimeError(f"combine schema is not loaded; can not get feature count")
        count = schema.feature_count
        return count

    @property
    @torch.jit.unused
    def dtype(self):
        return self._dtype

    @property
    @torch.jit.unused
    def requires_grad(self):
        return self._requires_grad

    @requires_grad.setter
    @torch.jit.unused
    def requires_grad(self, value):
        self._requires_grad = value

    @property
    @torch.jit.unused
    def updater(self):
        return self._updater

    @updater.setter
    @torch.jit.unused
    def updater(self, value):
        if value is not None:
            if not isinstance(value, TensorUpdater):
                raise TypeError(f"updater must be ps.TensorUpdater; {value!r} is invalid")
        if self._updater is not None:
            raise RuntimeError(f"can not reset updater {self._updater!r} to {value!r}")
        self._updater = value

    @property
    @torch.jit.unused
    def initializer(self):
        return self._initializer

    @initializer.setter
    @torch.jit.unused
    def initializer(self, value):
        if value is not None:
            if not isinstance(value, TensorInitializer):
                raise TypeError(f"initializer must be ps.TensorInitializer; {value!r} is invalid")
        if self._initializer is not None:
            raise RuntimeError(f"can not reset initializer {self._initializer!r} to {value!r}")
        self._initializer = value

    @property
    @torch.jit.unused
    def _is_clean(self):
        return (self._indices is None and
                self._indices_meta is None and
                self._index_batch is None and
                self._keys is None and
                self._data is None)

    @torch.jit.unused
    def _clean(self):
        self._indices = None
        self._indices_meta = None
        self._index_batch = None
        self._keys = None
        self._data = None
        self._output = torch.tensor(0.0)

    @torch.jit.unused
    def _check_clean(self):
        if self._keys is None and not self._is_clean:
            raise RuntimeError(f"{self!r} is expected to be clean when keys is None")

    @torch.jit.unused
    def _check_dtype_and_shape(self, keys, data):
        if not isinstance(data, numpy.ndarray):
            raise TypeError(f"data must be numpy.ndarray; got {type(data)!r}")
        dtype = str(self.dtype).rpartition('.')[-1]
        if data.dtype.name != dtype:
            raise TypeError(f"data must be numpy.ndarray of {dtype}; got {data.dtype.name}")
        embedding_size = self._checked_get_embedding_size()
        shape = len(keys), embedding_size
        if data.shape != shape:
            raise RuntimeError(f"data shape mismatches with op; {data.shape} vs. {shape}")

    @property
    @torch.jit.unused
    def keys(self):
        self._check_clean()
        return self._keys

    @property
    @torch.jit.unused
    def data(self):
        self._check_clean()
        return self._data

    @property
    @torch.jit.unused
    def grad(self):
        data = self.data
        if data is None:
            return None
        return data.grad

    @property
    @torch.jit.unused
    def output(self):
        return self._output

    @property
    @torch.jit.unused
    def keys_and_data(self):
        self._check_clean()
        if self._keys is None and self._data is None:
            return None, None
        if self._keys is not None and self._data is not None:
            return self._keys, self._data
        raise RuntimeError(f"keys and data of {self!r} must be both None or both not None")

    @keys_and_data.setter
    @torch.jit.unused
    def keys_and_data(self, value):
        if value is None:
            self._clean()
            return
        if not isinstance(value, tuple) or len(value) != 2:
            raise TypeError(f"value must be None or a pair of keys and data; {value!r} is invalid")
        keys, data = value
        if keys is None and value is None:
            self._clean()
            return
        if keys is not None and data is not None:
            if isinstance(keys, torch.Tensor):
                keys = keys.numpy()
            if not isinstance(keys, numpy.ndarray) or len(keys.shape) != 1 or keys.dtype not in (numpy.int64, numpy.uint64):
                raise TypeError("keys must be 1-D array of int64 or uint64")
            keys = keys.view(numpy.uint64)
            if isinstance(data, torch.Tensor):
                data = data.numpy()
            self._check_dtype_and_shape(keys, data)
            self._clean()
            self._keys = keys
            self._update_data(data)
            return
        raise RuntimeError(f"keys and data of {self!r} must be both None or both not None")

    @torch.jit.unused
    def _update_data(self, data):
        self._data = torch.from_numpy(data)
        self._data.requires_grad = self.training and self.requires_grad

    @torch.jit.unused
    def _combine_all_dense_padding(self, ndarrays, manager):
        delim = self._checked_get_delimiter()
        indices, widths, batch = self._feature_extractor.combine_all_dense_padding(ndarrays, self._combine_schema, delim=delim)
        manager.add(batch)
        return indices, widths, batch

    @torch.jit.unused
    def _combine_all(self, ndarrays, manager):
        delim = self._checked_get_delimiter()
        indices, ranges, batch = self._feature_extractor.combine_all(ndarrays, self._combine_schema, delim=delim)
        manager.add(batch)
        return indices, ranges, batch

    @torch.jit.unused
    def _do_combine(self, ndarrays, manager):
        raise NotImplementedError

    @torch.jit.unused
    def _uniquify_hash_codes(self, indices):
        keys = feature_extraction.fe.hash_uniquify(indices)
        return keys

    @torch.jit.unused
    def _combine(self, ndarrays, manager):
        self._clean()
        self._ensure_combine_schema_loaded()
        self._indices, self._indices_meta, self._index_batch = self._do_combine(ndarrays, manager)
        self._keys = self._uniquify_hash_codes(self._indices)

    @torch.jit.unused
    def _check_embedding_bag_mode(self, mode):
        if mode not in ('mean', 'sum', 'max'):
            raise ValueError(f"embedding bag mode must be one of: 'mean', 'sum', 'max'; {mode!r} is invalid")

    @torch.jit.unused
    def _compute_sum_concat(self):
        minibatch_size = self._indices.shape[0]
        embedding_size = self._checked_get_embedding_size()
        indices = self._indices.reshape(-1)
        indices_1d = torch.from_numpy(indices.view(numpy.int64))
        offsets = feature_extraction.fe.widths_to_bag_offsets(self._indices_meta, minibatch_size)
        offsets_1d = torch.from_numpy(offsets.view(numpy.int64))
        embs = torch.nn.functional.embedding_bag(indices_1d, self._data, offsets_1d, mode='sum')
        feature_count = len(self._indices_meta)
        expected_shape = minibatch_size * feature_count, embedding_size
        if embs.shape != expected_shape:
            raise RuntimeError(f"embs has unexpected shape; expect {expected_shape}, found {embs.shape}")
        out = embs.reshape(minibatch_size, feature_count * embedding_size)
        return out

    @torch.jit.unused
    def _compute_embedding_prepare(self):
        minibatch_size = self._indices_meta.shape[0] - 1
        embedding_size = self._checked_get_embedding_size()
        indices = self._indices
        indices_1d = torch.from_numpy(indices.view(numpy.int64))
        offsets = self._indices_meta[:-1]
        offsets_1d = torch.from_numpy(offsets.astype(numpy.int64))
        t = minibatch_size, embedding_size, indices_1d, offsets_1d
        return t

    @torch.jit.unused
    def _compute_embedding_lookup(self):
        t = self._compute_embedding_prepare()
        minibatch_size, embedding_size, indices_1d, offsets_1d = t
        embs = torch.nn.functional.embedding(indices_1d, self._data)
        expected_shape = len(indices_1d), embedding_size
        if embs.shape != expected_shape:
            raise RuntimeError(f"embs has unexpected shape; expect {expected_shape}, found {embs.shape}")
        out = embs, offsets_1d
        return out

    @torch.jit.unused
    def _compute_embedding_bag(self, *, mode='mean'):
        self._check_embedding_bag_mode(mode)
        t = self._compute_embedding_prepare()
        minibatch_size, embedding_size, indices_1d, offsets_1d = t
        embs = torch.nn.functional.embedding_bag(indices_1d, self._data, offsets_1d, mode=mode)
        expected_shape = minibatch_size, embedding_size
        if embs.shape != expected_shape:
            raise RuntimeError(f"embs has unexpected shape; expect {expected_shape}, found {embs.shape}")
        out = embs.reshape(minibatch_size, embedding_size)
        return out

    @torch.jit.unused
    def _do_compute(self):
        raise NotImplementedError

    @torch.jit.unused
    def _compute(self):
        if self._is_clean:
            raise RuntimeError(f"{self!r} is clean; can not execute the 'compute' step")
        self._output = self._do_compute()

    def forward(self, x):
        return self._output

    @torch.jit.unused
    async def _sparse_tensor_clear(self):
        tensor = self._distributed_tensor
        await tensor._sparse_tensor_clear()

    @torch.jit.unused
    async def _sparse_tensor_import_from(self, meta_file_path, *, data_only=False, skip_existing=False):
        tensor = self._distributed_tensor
        await tensor._sparse_tensor_import_from(meta_file_path, data_only=data_only, skip_existing=skip_existing)

    @torch.jit.unused
    def clear(self):
        if self._distributed_tensor is None:
            raise RuntimeError(f"{self!r} is not properly initialized; attribute '_distributed_tensor' is None")
        tensor = self._distributed_tensor
        agent = tensor._handle.agent
        agent.barrier()
        if agent.rank == 0:
            asyncio.run(self._sparse_tensor_clear())
        agent.barrier()

    @torch.jit.unused
    def import_from(self, meta_file_path, *, clear_existing=False, data_only=False, skip_existing=False):
        if self._distributed_tensor is None:
            raise RuntimeError(f"{self!r} is not properly initialized; attribute '_distributed_tensor' is None")
        if clear_existing:
            self.clear()
        tensor = self._distributed_tensor
        agent = tensor._handle.agent
        agent.barrier()
        asyncio.run(self._sparse_tensor_import_from(meta_file_path, data_only=data_only, skip_existing=skip_existing))
        agent.barrier()

class EmbeddingSumConcat(EmbeddingOperator):
    @torch.jit.unused
    def _do_combine(self, ndarrays, manager):
        return self._combine_all_dense_padding(ndarrays, manager)

    @torch.jit.unused
    def _do_compute(self):
        return self._compute_sum_concat()

class EmbeddingRangeSum(EmbeddingOperator):
    @torch.jit.unused
    def _do_combine(self, ndarrays, manager):
        return self._combine_all(ndarrays, manager)

    @torch.jit.unused
    def _do_compute(self):
        return self._compute_embedding_bag(mode='sum')
