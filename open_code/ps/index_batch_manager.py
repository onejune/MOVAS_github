import feature_extraction.fe

class IndexBatchManager(object):
    def __init__(self):
        self._batches = []

    @property
    def batches(self):
        return self._batches

    def add(self, batch):
        self._batches.append(batch)

    def clear(self):
        deleter = feature_extraction.fe.LibConcat.delete_index_batch_handle
        for batch in self._batches:
            handle = batch.handle
            if handle is not None:
                deleter(handle)
        self._batches.clear()

    def __del__(self):
        self.clear()

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.clear()
