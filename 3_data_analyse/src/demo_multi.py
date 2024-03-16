# coding:utf-8
import matplotlib.pyplot as plt
import numpy as np


def logistic(a, b, x):
    t = 1.0 / (1 + np.exp(-(a * x + b)))
    return t


def profit(a, b, x, cpm):
    t = (cpm - x) * logistic(a, b, x)
    return t


cpm_list = np.arange(10, 40, 0.5)

x2 = np.linspace(0, 40, 500)
xticks = np.arange(0, 50, 5)

count = 0
# 设置画布大小
fig, ax = plt.subplots()
for cpm in cpm_list[0:10]:
    count += 1
    y2 = [profit(1.3, -4, e, cpm) for e in x2]
    ax.plot(x2, y2, '-.', linewidth=2,
            dashes=[2, 2, 10, 2], label='cpm=' + str(cpm))

ax.set_xlabel("bidprice")
ax.set_ylabel("profit")
plt.xlim(0, 50)
plt.xticks(xticks)
plt.legend()
plt.grid(True)
plt.show()
