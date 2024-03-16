import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np

q = torch.Tensor([[1, 1, 1, 1], [2, 2, 2, 2], [3, 3, 3, 3], [4, 4, 4, 4]])
k = torch.Tensor([[1, 1, 1, 1], [2, 2, 2, 2], [3, 3, 3, 3], [4, 4, 4, 4]])

# print(q.T)
# print(k.T)

l_pos = torch.einsum("nc,nc->n", [q, k]).unsqueeze(-1)
# print(l_pos.T)
x = torch.Tensor([[1, 1, 1, 1], [2, 2, 2, 2], [3, 3, 3, 3], [4, 4, 4, 4]])
y = torch.Tensor([[1, 1, 1, 1], [2, 2, 2, 2], [3, 3, 3, 3], [4, 4, 4, 4]])

x = torch.randn(10, 12)
y = torch.randn(10, 12)

z1 = torch.cat((x, y), dim=1)
# print(z1.shape)

z = torch.split(z1, 1, dim=1)

dnn_input = torch.flatten(torch.cat(z, dim=-1), start_dim=1)

inputs = torch.randn(10, 5)
x = inputs.unsqueeze(2)

y_pred = torch.randn(10, 1)
y_mathch = torch.randn(10, 1)
print(y_pred, y_mathch)

out = torch.cat((y_pred, y_mathch), dim=1)
print(out.shape)
predict, match = out[:, :1], out[:, 1:]
print(y_pred, y_mathch)