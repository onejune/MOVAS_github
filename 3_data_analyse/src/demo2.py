# coding=utf-8
import numpy as np
import matplotlib.pyplot as plt


def f(t):
    return np.exp(-t) * np.cos(2*np.pi*t)


t1 = np.arange(0.0, 5.0, 0.1)
t2 = np.arange(0.0, 5.0, 0.02)

# 生成一些区间 [0, 1] 内的数据
y = np.random.normal(loc=0.5, scale=0.4, size=1000)
y = y[(y > 0) & (y < 1)]
y.sort()
x = np.arange(len(y))

# 带有多个轴域刻度的 plot
plt.figure(1)

# 线性
plt.subplot(321)
plt.plot(x, y)
plt.yscale('linear')
plt.title('linear')
plt.grid(True)


# 对数
plt.subplot(322)
plt.plot(x, y)
plt.yscale('log')
plt.title('log')
plt.grid(True)


# 对称的对数
plt.subplot(323)
plt.plot(x, y - y.mean())
plt.yscale('symlog', linthreshy=0.05)
plt.title('symlog')
plt.grid(True)

# logit
plt.subplot(324)
plt.plot(x, y)
plt.yscale('symlog')
plt.title('symlog')
plt.grid(True)

plt.subplot(325)
plt.plot(t1, f(t1), 'bo', t2, f(t2), 'k')

plt.subplot(326)
plt.plot(t2, np.cos(2*np.pi*t2), 'r--')

# Adjust the subplot layout, because the logit one may take more space
# than usual, due to y-tick labels like "1 - 10^{-3}"
plt.subplots_adjust(hspace=0.5, wspace=0.35)

plt.show()
