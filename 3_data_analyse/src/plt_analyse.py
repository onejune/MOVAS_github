# coding:utf-8
import matplotlib.pyplot as plt
import numpy as np


def logistic(a, b, x):
    t = 1.0 / (1 + np.exp(-(a * x + b)))
    return t


def profit(a, b, x, cpm):
    t = (cpm - x) * logistic(a, b, x)
    return t


cpm_list = np.arange(1, 20, 1)

x2 = np.linspace(0, 20, 500)
count = 0
# 设置画布大小
fig = plt.figure(figsize=(16, 16))
for cpm in cpm_list[0:16]:
    count += 1
    y2 = [profit(2, -1, e, cpm) for e in x2]

    # 画子图
    ax = fig.add_subplot(4, 4, count)
    plt.plot(x2, y2, '-.', linewidth=2, dashes=[4, 2])

    ax.set_xlabel("bidprice")
    ax.set_ylabel("profit")
    plt.title('cpm=' + str(cpm))
    plt.tight_layout()
plt.figure()
plt.grid(True)
plt.legend(loc='lower right')
plt.show()
