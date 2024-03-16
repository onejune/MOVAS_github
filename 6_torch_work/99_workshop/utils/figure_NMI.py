import numpy as np
from sklearn.metrics.cluster import normalized_mutual_info_score

label_list1 = [1, 2, 3, 2, 1, 4, 3]
label_list2 = [1, 2, 4, 3, 2, 1, 3]

nmi = normalized_mutual_info_score(label_list1, label_list2)
print(nmi)
