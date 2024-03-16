import torch
from fc import FullyConnectedLayer

N, D_in, D_out = 100, 5, 2  # 一共10组样本，输入特征数为5，输出特征为3 

# 先定义一个模型
class MyNet(torch.nn.Module):
    def __init__(self):
        super(MyNet, self).__init__()  # 第一句话，调用父类的构造函数
        self.mylayer1 = FullyConnectedLayer(D_in, [200, 80, 1], [True, True, True], sigmoid = True)
 
    def forward(self, x):
        x = self.mylayer1(x)
        return x
 
model = MyNet()
#输出网络结构
print(model)

# 创建输入、输出数据
x = torch.randn(N, D_in)  #（10，5）
y = torch.randn(N, D_out) #（10，2）

#定义损失函数
loss_fn = torch.nn.MSELoss(reduction='sum')
#loss_fn = torch.nn.CrossEntropyLoss()

learning_rate = 1e-4
#构造一个optimizer对象
#optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
optimizer = torch.optim.SGD(model.parameters(), lr = 0.01, momentum=0.9)


for t in range(10): # 
    # 第一步：数据的前向传播，计算预测值p_pred
    y_pred = model(x)
 
    # 第二步：计算计算预测值p_pred与真实值的误差
    loss = loss_fn(y_pred, y)
    print(f"第 {t} 个epoch, 损失是 {loss.item()}")
 
    # 在反向传播之前，将模型的梯度归零，这
    optimizer.zero_grad()
 
    # 第三步：反向传播误差
    loss.backward()
 
    # 直接通过梯度一步到位，更新完整个网络的训练参数
    optimizer.step()


