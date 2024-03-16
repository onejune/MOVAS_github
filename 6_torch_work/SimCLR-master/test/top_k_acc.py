import torch
logits = torch.tensor([[0.1, 0.3, 0.2, 0.4],
                       [0.5, 0.01, 0.9, 0.4],
                       [0.8, 0.2, 0.1, 0.04],
                       [0.3, 0.5, 0.1, 0.7]
                       ])
y = torch.tensor([3, 0, 1, 2])

def calculate_top_k_accuracy(logits, targets, k=2):
    _, indices = torch.topk(logits, k=k, sorted=True)
    print(indices)
    y = torch.reshape(targets, [-1, 1])
    print(y)
    correct = (y == indices) * 1.  # 对比预测的K个值中是否包含有正确标签中的结果
    print(correct) #(k, N)
    top_k_accuracy = torch.mean(correct) * k  # 计算最后的准确率
    return top_k_accuracy

print(calculate_top_k_accuracy(logits, y, k=2).item())
print(calculate_top_k_accuracy(logits, y, k=1).item())