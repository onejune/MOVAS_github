import os, sys
from collections import OrderedDict, namedtuple, defaultdict
from itertools import chain

import torch
import torch.nn as nn
import numpy as np

class MetricMethods():
    @staticmethod
    def NDCG(current, n = -1):
        log2_table = np.log2(np.arange(2, 102))

        def dcg_at_n(rel, n):
            rel = np.asfarray(rel)[:n]
            dcg = np.sum(np.divide(np.power(2, rel) - 1, log2_table[:rel.shape[0]]))
            return dcg

        ndcgs = []
        for i in range(len(current)):
            k = len(current[i]) if n == -1 else n
            idcg = dcg_at_n(sorted(current[i], reverse=True), n=k)
            dcg = dcg_at_n(current[i], n=k)
            tmp_ndcg = 0 if idcg == 0 else dcg / idcg
            ndcgs.append(tmp_ndcg)
            #print(dcg, idcg, ndcgs)
        result = 0. if len(ndcgs) == 0 else sum(ndcgs) / (len(ndcgs))
        return round(result, 2)

if __name__ == '__main__':
    cur_list = [[2,3,1,4,5], [2,5,1,3,2], [1,5,3,2,4], [2,5,3,4,1], [2,5,3,1,4]]

    ndcg = MetricMethods.NDCG(cur_list)
    print(ndcg)
