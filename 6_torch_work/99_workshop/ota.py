import torch
import torch.nn as nn
import numpy as np

q = torch.Tensor([[1, 1, 1, 1], [2, 2, 2, 2], [3, 3, 3, 3], [4, 4, 4, 4]])
k = torch.Tensor([[1, 1, 1, 1], [2, 2, 2, 2], [3, 3, 3, 3], [4, 4, 4, 4]])

lam = 5
M = torch.randn(5, 3)
# print(M)
n, m = M.shape
P = np.exp(-lam * M)
psum = P.sum()
# print(psum)
P /= P.sum()
# print(P)
u = np.zeros(n)
print(P.sum(1))
r = torch.Tensor([1, 1, 1, 1, 1])
c = torch.Tensor([1, 1, 1])

# normalize this matrix
while np.max(np.abs(u - P.sum(1))) > 0.1:
    u = P.sum(1)
    print("u:", u)
    print("r/u:", r / u)
    P *= (r / u).reshape((-1, 1))
    P *= (c / P.sum(0)).reshape((1, -1))
