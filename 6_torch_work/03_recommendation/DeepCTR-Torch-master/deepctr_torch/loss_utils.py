from collections import OrderedDict, namedtuple, defaultdict
from itertools import chain

import torch
import torch.nn as nn
import numpy as np

from .layers.sequence import SequencePoolingLayer
from .layers.utils import concat_fun
from .movas_logger import *

class LossMethods():
    @staticmethod
    def nansum(x):
        return torch.where(torch.isnan(x), torch.zeros_like(x), x).sum()

    @staticmethod
    def log_loss(yhat, y):
        return LossMethods.nansum(-(y * (yhat + 1e-12).log() + (1 - y) *
                    (1 - yhat + 1e-12).log()))
        
    @staticmethod
    def focus_loss(y_pred, y_true):
            alpha = 0.5
            gamma = 1
            y_pred = y_pred + 1e-12
            return LossMethods.nansum(-((1 - y_pred) ** gamma * y_true * y_pred.log() 
                                        + y_pred ** gamma * (1 - y_true) * (1 - y_pred).log()))
            
    @staticmethod
    def jrc_loss(y_pred, y_true, context_indicator = None):
        alpha = 0.5
        batchsize = y_true.shape[0]
 
        #MovasLogger.add_log(content = "print y_pred: \n %s %s" % (y_pred, y_pred.shape))
        #MovasLogger.add_log(content = "print y_true: \n %s %s" % (y_true, y_true.shape))
        if context_indicator.dim() == 1:
            context_indicator = torch.unsqueeze(context_indicator, 1)
        #MovasLogger.add_log(content = 'context_indicator in jcr_loss: \n%s %s' % (context_indicator, context_indicator.shape))

        #在 label 的第 0 维插入一个新的维度，变成 [1, B, 2], 然后再在第 0维上重复张量，得到[B, B, 2]
        #y = torch.tile(torch.unsqueeze(y_true, 0), [batchsize, 1, 1])
        y = torch.tile(torch.unsqueeze(y_true, 1), [1, batchsize, 1])
        #MovasLogger.add_log(content = 'after unszueeze and tile, y=\n%s %s' % (y, y.shape))

        #计算交叉熵 loss，logit 和 label 都是 2 维向量
        #3种计算 ce loss 的方式，1 和 3 相同
        criterion = nn.BCEWithLogitsLoss()
        #ce_loss1 = 0.5 * torch.mean(criterion(y_pred[:, 0], y_true[:, 0]) + criterion(y_pred[:, 1], y_true[:, 1]))
        #ce_loss2 = torch.mean(criterion(y_pred[:, 1] - y_pred[:, 0], y_true[:, 1]))
        ce_loss3 = torch.mean(criterion(y_pred, y_true))
        #MovasLogger.add_log(content = 'ce_loss:\n loss1=%s, loss2=%s, loss3=%s' % (ce_loss1.item(), ce_loss2.item(), ce_loss3.item()))
        ce_loss = ce_loss3
        
        #logit 和 label 做同样的变换
        #y_pred = torch.tile(torch.unsqueeze(y_pred, 0), [batchsize, 1, 1])
        y_pred = torch.tile(torch.unsqueeze(y_pred, 1), [1, batchsize, 1])

        #根据 context index 得到掩码矩阵，mask[i][j]表示 是第 i 条样本和第 j 条样本是否同一个 context，[B, B]
        context_index_t = context_indicator.t()
        mask = torch.eq(context_indicator, context_index_t)
        unsqueeze_mask = torch.unsqueeze(mask, 2)  #[B, B, 1]

        #不在同一个 context 的样本，把 label 设为[0, 0]
        y = y * unsqueeze_mask
        #不在同一个 context 的样本，把 logit 设为-inf
        y_pred = y_pred + (~unsqueeze_mask) * -1e9 

        # 未点击和点击对应的 label和 logit
        y_neg, y_pos = y[:,:, 0], y[:,:, 1]
        l_neg, l_pos = y_pred[:,:, 0], y_pred[:,:, 1]

        #点击和未点击 logit 对应的 ListNet loss: Loss = -sigma(y*log(softmax(y_pred)))
        log_pos = torch.log1p(torch.softmax(l_pos, dim = 0))
        log_pos = torch.where(torch.isinf(log_pos), torch.tensor(0.0), log_pos) #把 inf 的值转成 0

        log_neg = torch.log1p(torch.softmax(l_neg, dim = 0))
        log_neg = torch.where(torch.isinf(log_neg), torch.tensor(0.0), log_neg)

        loss_pos = -torch.sum(y_pos * log_pos, dim = 0)
        loss_neg = -torch.sum(y_neg * log_neg, dim = 0)
        ge_loss = torch.mean((loss_pos + loss_neg)/torch.sum(mask, dim = 0))

        #MovasLogger.add_log(content='mask: \n%s %s \nmask_sum:\n%s %s' % (mask, mask.shape, ttt, ttt.shape))
        loss = alpha * ce_loss + (1 - alpha) * ge_loss
        #MovasLogger.add_log(content = '\nloss_pos:%s \nloss_neg:%s \nce_loss:%s, ge_loss:%s, loss:%s' % (loss_pos, loss_neg, ce_loss, ge_loss, loss))

        return loss
    
    def ranknet_loss(score_predict, score_real, context_indicator = None, reduction = 'sum'):
        if context_indicator.dim() == 1:
            context_indicator = torch.unsqueeze(context_indicator, 1)
        #MovasLogger.add_log(content = 'context_indicator in ranknet_loss: \n%s %s' % (context_indicator, context_indicator.shape))
        #构造掩码矩阵，属于同一个 context index 的两条样本，对应值为 True
        mask = torch.eq(context_indicator, context_indicator.t())
        #对角线元素表示同一条样本，不需要计算 loss
        mask.fill_diagonal_(False)

        score_pre_diff = score_predict - score_predict.t()
        score_diff_sigmoid = torch.sigmoid(score_pre_diff)
        
        score_real_diff = score_real - score_real.t()
        tij = (1.0 + torch.sign(score_real_diff)) / 2.0
        
        loss_mat = -(tij * torch.log(score_diff_sigmoid) + (1-tij)*torch.log(1-score_diff_sigmoid))
        #只有属于同一个 context index 的样本的 loss 才有效
        loss_mat_mask = loss_mat * mask
        loss = loss_mat_mask.mean()
        #MovasLogger.add_log(content = 'loss: %s' % loss)

        return loss
        
