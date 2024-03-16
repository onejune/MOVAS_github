import ps
import torch
import torch.nn as nn
import torch.nn.functional as F


class MatchingDense(torch.nn.Module):
    def __init__(self, emb_out_size):
        super().__init__()
        self._emb_bn = nn.BatchNorm1d(emb_out_size, momentum=0.01, eps=1e-5)
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
    def __init__(self, column_name, combine_schema, emb_size=16):
        super().__init__()
        self._embedding_size = emb_size
        self._column_name = column_name
        self._combine_schema = combine_schema
        self._sparse = ps.EmbeddingSumConcat(self._embedding_size,
                                             self._column_name,
                                             self._combine_schema)
        self._sparse.updater = ps.FTRLTensorUpdater(alpha=0.01)
        self._sparse.initializer = ps.NormalTensorInitializer(var=0.0001)
        self._emb_out_size = self._sparse.feature_count * self._embedding_size
        self._dense = MatchingDense(self._emb_out_size)

    def forward(self, x):
        x = self._sparse(x)
        x = self._dense(x)
        return x


class ItemModule(torch.nn.Module):
    def __init__(self, column_name, combine_schema, emb_size=16):
        super().__init__()
        self._embedding_size = emb_size
        self._column_name = column_name
        self._combine_schema = combine_schema
        self._sparse = ps.EmbeddingSumConcat(self._embedding_size,
                                             self._column_name,
                                             self._combine_schema)
        self._sparse.updater = ps.FTRLTensorUpdater(alpha=0.01)
        self._sparse.initializer = ps.NormalTensorInitializer(var=0.0001)
        self._dense = MatchingDense(self._emb_out_size)

    def forward(self, x):
        x = self._sparse(x)
        x = self._dense(x)
        return x


class Normalization(nn.modules.batchnorm._BatchNorm):
    def _check_input_dim(self, input):
        if input.dim() != 2 and input.dim() != 3:
            raise ValueError('expected 3D or 3D input (got {}D input)'.format(
                input.dim()))

    def forward(self, input):
        if not self.training:
            return F.batch_norm(input, self.running_mean, self.running_var,
                                self.weight, self.bias, False)
        self._check_input_dim(input)
        if self.momentum is None:
            exponential_average_factor = 0.0
        else:
            exponential_average_factor = self.momentum

        if self.track_running_stats:
            if self.num_batches_tracked is not None:
                self.num_batches_tracked = self.num_batches_tracked + 1
                if self.momentum is None:  # use cumulative moving average
                    exponential_average_factor = 1.0 / float(
                        self.num_batches_tracked)
                else:  # use exponential moving average
                    exponential_average_factor = self.momentum
        output = (input - self.running_mean) / (self.running_var +
                                                self.eps).sqrt()
        with torch.no_grad():
            #rate = input.shape[0]/512.0 #batch_size
            rate = 1.0
            batch_mean = input.mean(dim=0)
            batch_var = ((input - self.running_mean) *
                         (input - self.running_mean)).mean(dim=0)
            self.running_mean[...] = (
                1 - exponential_average_factor * rate
            ) * self.running_mean + exponential_average_factor * rate * batch_mean
            self.running_var[...] = (
                1 - exponential_average_factor * rate
            ) * self.running_var + exponential_average_factor * rate * batch_var
        output1 = output * self.weight + self.bias
        return output1


class FGLayer(nn.Module):
    def __init__(self, in_size, out_size, act1 = nn.ReLU(), \
                                        act2 = nn.Sigmoid(), act3 = nn.ReLU()):
        super().__init__()
        self.layer1 = torch.nn.Linear(in_size, out_size)
        self.layer2 = torch.nn.Linear(in_size, out_size)
        self.act1 = act1
        self.act2 = act2
        self.act3 = act3

    def forward(self, input):
        info_out = self.act1(self.layer1(input))
        gate_out = self.act2(self.layer2(input))
        return self.act3(info_out * gate_out)


class MatchingNet(torch.nn.Module):
    def __init__(self,
                 column_name_user,
                 combine_schema_user,
                 column_name_item,
                 combine_schema_item,
                 emb_size=16):
        super().__init__()
        self.user_model = UserModule(column_name_user, combine_schema_user)
        self.item_model = ItemModule(column_name_item, combine_schema_item)
        self.sim_score = SimilarityModule()

    def forward(self, x):
        x1 = self.user_model(x)
        x2 = self.item_model(x)
        score = self.sim_score(x1, x2)
        return score


class PredictionNet(torch.nn.Module):
    def __init__(self, column_name, combine_schema, emb_size=16):
        super().__init__()
        self._embedding_size = emb_size
        self._column_name = column_name
        self._combine_schema = combine_schema
        self._sparse = ps.EmbeddingSumConcat(self._embedding_size,
                                             self._column_name,
                                             self._combine_schema)
        self._sparse.updater = ps.FTRLTensorUpdater(alpha=0.01)
        self._sparse.initializer = ps.NormalTensorInitializer(var=0.0001)
        self._sparse_feature_num = self._sparse.feature_count * self._embedding_size
        self._dense = torch.nn.Sequential(
            torch.nn.Linear(self._sparse_feature_num, 1024), nn.ReLU(),
            torch.nn.Linear(1024, 512), nn.ReLU(), torch.nn.Linear(512, 1),
            nn.Sigmoid())
        self._bn = Normalization(self._sparse_feature_num,
                                 momentum=0.01,
                                 eps=1e-5,
                                 affine=True)
        self._bn.running_mean.updater = ps.EMATensorUpdater(0.01)
        self._bn.running_var.updater = ps.EMATensorUpdater(0.01)
        self._dense[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense[2].bias.initializer = ps.ZeroTensorInitializer()
        self._dense[4].bias.initializer = ps.ZeroTensorInitializer()

    def forward(self, x):
        x = self._sparse(x)
        x = self._bn(x)
        x = self._dense(x)
        return x


class NNRankModel(nn.Module):
    def __init__(self, column_name_p, combine_schema_p, column_name_user, combine_schema_user, column_name_item,
                 combine_schema_item):
        super().__init__()
        self.predict_net = PredictionNet(column_name_p, combine_schema_p)
        self.matches_net = MatchingNet(column_name_user, combine_schema_user,
                                       column_name_item, combine_schema_item)

        self._batch_id = 0

    def forward(self, x):
        x1, x2 = self.predict_net(x), self.matches_net(x)
        return x1, x2
