import torch
import torch.nn.functional as F

class Normalization(torch.nn.modules.batchnorm._BatchNorm):
    def _check_input_dim(self, input):
        if input.dim() != 2 and input.dim() != 3:
            raise ValueError('expected 3D or 3D input (got {}D input)'.format(input.dim()))

    def forward(self, input):
        self._check_input_dim(input)

        if not self.training:
            return F.batch_norm(input, self.running_mean, self.running_var, self.weight, self.bias, False)

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
        batch_var = ((input - self.running_mean) * (input - self.running_mean)).mean(dim=0)
        output = (input - self.running_mean) / (self.running_var + self.eps).sqrt()
        if self.training:
            with torch.no_grad():
                self.running_mean[...] = batch_mean
                self.running_var[...] = batch_var
        result = output * self.weight + self.bias
        return result
