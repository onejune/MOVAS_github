import torch

def nansum(x):
    return torch.where(torch.isnan(x), torch.zeros_like(x), x).sum()

def log_loss(yhat, y):
    return nansum(-(y * (yhat + 1e-12).log() + (1 - y) * (1 - yhat + 1e-12).log()))
