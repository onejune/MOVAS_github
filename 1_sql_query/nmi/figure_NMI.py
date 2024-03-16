import numpy as np
from sklearn.metrics.cluster import normalized_mutual_info_score
import  pandas as pd
import os, sys
from pandas import DataFrame,Series


def NMI(lst1, lst2):
    nmi = normalized_mutual_info_score(lst1, lst2)
    return nmi

input_file = sys.argv[1]

df = pd.read_csv(input_file, sep='\t')
print(df.info())
print(df.head())
print(df.columns)

print(df['endcard_tpl'].unique())

label_list1 = df['platform'].values
label_list2 = df['b.is_imp'].values

#label_list1 = np.random.randint(1000, size=1000000)
#label_list2 = np.random.randint(1000, size=1000000)
print(NMI(label_list1, label_list2))
