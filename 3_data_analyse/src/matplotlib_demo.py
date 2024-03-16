# coding:utf-8
import matplotlib.pyplot as plt
import numpy as np


def logistic(a, b, x):
    t = 1.0 / (1 + np.exp(-(a * x + b)))
    return t


x1 = np.arange(0, 20, 0.5)
x1 = np.linspace(0, 10, 500)
y1 = [logistic(1, -2, e) for e in x1]

x2 = np.arange(0, 20, 0.5)
y2 = [logistic(2, -1, e) for e in x2]

print y1[0:10]
print y2[0:10]

plt.figure(1)
plt.subplot(211)
plt.plot(x1, y1, 'bo', x2, y2, 'k', linewidth=1)

plt.subplot(212)
plt.plot(x1, np.cos(2 * np.pi * x1), 'r--', linewidth=1)

plt.xlabel('bidprice')
plt.ylabel('winrate')
plt.title('My Very Own Histogram')
plt.text(23, 45, r'$\mu=15, b=3$')

plt.show()
