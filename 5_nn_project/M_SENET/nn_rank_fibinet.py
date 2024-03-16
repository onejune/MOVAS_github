import ps
import itertools
import torch
import torch.nn as nn
import torch.nn.functional as F

class SENETLayer(nn.Module):
    def __init__(self, filed_size, reduction_ratio=10, seed=1024):
        super(SENETLayer, self).__init__()
        self.seed = seed
        self.filed_size = filed_size
        self.reduction_size = max(1, filed_size // reduction_ratio)
        self.excitation = nn.Sequential(
            nn.Linear(self.filed_size, self.reduction_size, bias=False),
            nn.ReLU(),
            nn.Linear(self.reduction_size, self.filed_size, bias=False),
            nn.ReLU())

    def forward(self, inputs):
        if len(inputs.shape) != 3:
            raise ValueError(
                "Unexpected inputs dimensions %d, expect to be 3 dimensions" %
                (len(inputs.shape)))
        Z = torch.mean(inputs, dim=-1, out=None)
        A = self.excitation(Z)
        V = torch.mul(inputs, torch.unsqueeze(A, dim=2))

        return V

class BilinearInteraction(nn.Module):
    def __init__(self,
                 filed_size,
                 embedding_size,
                 bilinear_type="all",
                 seed=1024):
        super(BilinearInteraction, self).__init__()
        self.bilinear_type = bilinear_type
        self.seed = seed
        self.bilinear = nn.ModuleList()
        if self.bilinear_type == "all":
            self.bilinear = nn.Linear(embedding_size,
                                      embedding_size,
                                      bias=False)
        elif self.bilinear_type == "each":
            for _ in range(filed_size):
                self.bilinear.append(
                    nn.Linear(embedding_size, embedding_size, bias=False))
        elif self.bilinear_type == "interaction":
            for i, j in itertools.combinations(range(filed_size), 2):
                self.bilinear.append(
                    nn.Linear(embedding_size, embedding_size, bias=False))
        else:
            raise NotImplementedError

    def forward(self, inputs):
        if len(inputs.shape) != 3:
            raise ValueError(
                "Unexpected inputs dimensions %d, expect to be 3 dimensions" %
                (len(inputs.shape)))
        inputs = torch.split(inputs, 1, dim=1)
        if self.bilinear_type == "all":
            p = [
                torch.mul(self.bilinear(v_i), v_j)
                for v_i, v_j in itertools.combinations(inputs, 2)
            ]
        elif self.bilinear_type == "each":
            p = [
                torch.mul(self.bilinear[i](inputs[i]), inputs[j])
                for i, j in itertools.combinations(range(len(inputs)), 2)
            ]
        elif self.bilinear_type == "interaction":
            p = [
                torch.mul(bilinear(v[0]), v[1]) for v, bilinear in zip(
                    itertools.combinations(inputs, 2), self.bilinear)
            ]
        else:
            raise NotImplementedError
        return torch.cat(p, dim=1)
        

class Normalization(nn.modules.batchnorm._BatchNorm):
    def _check_input_dim(self, input):
        if input.dim() != 2 and input.dim() != 3:
            raise ValueError('expected 3D or 3D input (got {}D input)'.format(input.dim()))


    def forward(self, input):
        if not self.training:
            return F.batch_norm(input, self.running_mean, self.running_var, self.weight, self.bias, False)
        self._check_input_dim(input)
        if self.momentum is None:
            exponential_average_factor = 0.0 
        else:
            exponential_average_factor = self.momentum

        if self.track_running_stats:
            if self.num_batches_tracked is not None:
                self.num_batches_tracked = self.num_batches_tracked + 1 
                if self.momentum is None:  # use cumulative moving average
                    exponential_average_factor = 1.0 / float(self.num_batches_tracked)
                else:  # use exponential moving average
                    exponential_average_factor = self.momentum
        output = (input - self.running_mean)/(self.running_var + self.eps).sqrt()
        with torch.no_grad():
            rate = input.shape[0]/1000.0 #batch_size
            batch_mean = input.mean(dim=0)
            batch_var = ((input-self.running_mean) * (input-self.running_mean)).mean(dim=0)
            self.running_mean[...] = (1-exponential_average_factor*rate) * self.running_mean + exponential_average_factor*rate * batch_mean
            self.running_var[...] = (1-exponential_average_factor*rate) * self.running_var + exponential_average_factor*rate * batch_var
            #self.running_mean[...] = batch_mean
            #self.running_var[...] = batch_var
        output1 = output * self.weight + self.bias
        return output1


class NNRankModel(nn.Module):
    def __init__(self,column_name, combine_schema, seed=1024, reduction_ratio=8, bilinear_type='all'):
        super().__init__()
        #print('[MSENET]NNRankModel-init')
        self._embedding_size = 8
        self._sparse = ps.EmbeddingSumConcat(self._embedding_size, column_name, combine_schema)
        self._sparse.updater = ps.FTRLTensorUpdater(alpha=0.01,l1=1.0)
        self._sparse.initializer = ps.NormalTensorInitializer(var=0.01)
        
        self.filed_size = self._sparse.feature_count
        self.SE = SENETLayer(self.filed_size, reduction_ratio, seed)
        self.Bilinear = BilinearInteraction(self.filed_size, self._embedding_size, bilinear_type, seed)
        
        dense_input_dim = self.filed_size * (self.filed_size - 1) * self._embedding_size
        #print('[MSENET]filed_size:', self.filed_size, ' dense_input_dim:', dense_input_dim)
        self._dense = torch.nn.Sequential(
            torch.nn.Linear(dense_input_dim, 1024),
            torch.nn.ReLU(),
            torch.nn.Linear(1024, 512),
            torch.nn.ReLU(),
            torch.nn.Linear(512, 1),
            torch.nn.Dropout(0.3),
        )
        
        #print('[MSENET]dense:', self._dense)
        #self._bn = torch.nn.BatchNorm1d(152 * self._embedding_size, momentum=0.01, eps=1e-5, affine=True)
        self._bn = Normalization(self._sparse.feature_count * self._embedding_size, momentum=0.01, eps=1e-5, affine=True)
        self._dense[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense[2].bias.initializer = ps.ZeroTensorInitializer()
        self._dense[4].bias.initializer = ps.ZeroTensorInitializer()


    def forward(self, x):
        #print('[MSENET]forward')
        emb = self._sparse(x)
        bno_emb = self._bn(emb)
        #print('[MSENET]bno_emb:', bno_emb.shape)
        bno_emb = torch.reshape(bno_emb, (bno_emb.shape[0], self.filed_size, self._embedding_size))
        #print('[MSENET]bno_emb:', bno_emb.shape, ' feature_count:', self.filed_size)
        senet_output = self.SE(bno_emb)
        #print('[MSENET]senet_output:', senet_output.shape)
        senet_bilinear_out = self.Bilinear(senet_output)
        bilinear_out = self.Bilinear(bno_emb)
        #print('[MSENET]bilinear_out:', bilinear_out.shape)
        
        sparse_embedding_list = torch.split(torch.cat((senet_bilinear_out, bilinear_out), dim=1), 1, dim=1)
        #print('[MSENET]sparse_embedding_list:', sparse_embedding_list)
        dnn_input = torch.flatten(torch.cat(sparse_embedding_list, dim=-1), start_dim=1)
        #print('[MSENET]dnn_input:', dnn_input.shape, dnn_input)
        x = self._dense(dnn_input)
        #print('[MSENET]x:', x.shape)
        return torch.sigmoid(x)


