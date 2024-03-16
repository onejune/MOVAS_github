import ps
import torch
import torch.nn as nn
import torch.nn.functional as F

class Normalization(nn.modules.batchnorm._BatchNorm):
    def _check_input_dim(self, input):
        if input.dim() != 2 and input.dim() != 3:
            raise ValueError('expected 3D or 3D input (got {}D input)'.format(input.dim()))


    def forward(self, input):
        self._check_input_dim(input)
        if self.momentum is None:
            exponential_average_factor = 0.0 
        else:
            exponential_average_factor = self.momentum

        if self.training and self.track_running_stats:
            if self.num_batches_tracked is not None:
                self.num_batches_tracked = self.num_batches_tracked + 1 
                if self.momentum is None:  # use cumulative moving average
                    exponential_average_factor = 1.0 / float(self.num_batches_tracked)
                else:  # use exponential moving average
                    exponential_average_factor = self.momentum
        if self.training:
            bn_training = True
        else:
            bn_training = (self.running_mean is None) and (self.running_var is None)
        batch_mean = input.mean(dim=0)
        batch_var = ((input-self.running_mean) * (input-self.running_mean)).mean(dim=0)
        output = (input - self.running_mean)/(self.running_var + self.eps).sqrt()#not
        if self.training:
            with torch.no_grad():
                self.running_mean[...] = batch_mean
                self.running_var[...] = batch_var
        output1 = output * self.weight + self.bias
        return output1


class NNRankModel(nn.Module):
    def __init__(self,column_name, combine_schema):
        super().__init__()
        self._embedding_size = 16
        self._sparse = ps.EmbeddingSumConcat(self._embedding_size, column_name, combine_schema)
        self._sparse.updater = ps.FTRLTensorUpdater(alpha=0.1,l1=0.01)
        self._sparse.initializer = ps.NormalTensorInitializer(var=0.001)
        self._dense = torch.nn.Sequential(
            torch.nn.Linear(self._sparse.feature_count * self._embedding_size, 1024),
            torch.nn.ReLU(),
            torch.nn.Linear(1024, 512),
            torch.nn.ReLU(),
            torch.nn.Linear(512, 1),
        )
        #self._bn = torch.nn.BatchNorm1d(152 * self._embedding_size, momentum=0.01, eps=1e-5, affine=True)
        self._bn = Normalization(self._sparse.feature_count * self._embedding_size, momentum=0.01, eps=1e-5, affine=True)
        self._dense[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense[2].bias.initializer = ps.ZeroTensorInitializer()
        self._dense[4].bias.initializer = ps.ZeroTensorInitializer()


    def forward(self, x):
        emb = self._sparse(x)
        bno = self._bn(emb)
        x = self._dense(bno)
        return torch.sigmoid(x)

