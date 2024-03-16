import torch.nn as nn
#from .dice import Dice
from dice import Dice

class FullyConnectedLayer(nn.Module):
    def __init__(self, input_size, hidden_size, bias, batch_norm=True, dropout_rate=0.5, activation='relu', sigmoid=False, dice_dim=2):
        super(FullyConnectedLayer, self).__init__()
        assert len(hidden_size) >= 1 and len(bias) >= 1
        assert len(bias) == len(hidden_size)
        self.sigmoid = sigmoid

        layers = []
        #input_size: size of each input sample
        layers.append(nn.Linear(input_size, hidden_size[0], bias=bias[0]))
        
        for i, h in enumerate(hidden_size[:-1]):
            if batch_norm:
                layers.append(nn.BatchNorm1d(hidden_size[i]))
            
            if activation.lower() == 'relu':
                layers.append(nn.ReLU(inplace=True))
            elif activation.lower() == 'dice':
                assert dice_dim
                layers.append(Dice(hidden_size[i], dim=dice_dim))
            elif activation.lower() == 'prelu':
                layers.append(nn.PReLU())
            else:
                raise NotImplementedError
            
            layers.append(nn.Dropout(p=dropout_rate))
            layers.append(nn.Linear(hidden_size[i], hidden_size[i+1], bias=bias[i]))
        
        self.fc = nn.Sequential(*layers)
        if self.sigmoid:
            self.output_layer = nn.Sigmoid()
        
        # weight initialization xavier_normal (or glorot_normal in keras, tf)
        for m in self.modules():
            if isinstance(m, nn.Linear):
                nn.init.xavier_normal_(m.weight.data, gain=1.0)
                if m.bias is not None:
                    nn.init.zeros_(m.bias.data)

    def forward(self, x):
        #print("fc_forward:", x)
        return self.output_layer(self.fc(x)) if self.sigmoid else self.fc(x) 
        

if __name__ == "__main__":
    from torchsummary import summary
    #第一个参数是训练数据的列数,也就是特征数，第二个参数表示每个layer的节点数
    a = FullyConnectedLayer(5, [16], [True], sigmoid = True)
    summary(a, input_size=(5,))
    import torch
    #b = torch.zeros((100, 5))
    b = torch.rand((100, 5))
    #print(b)
    c = a(b)
    print(c.size())
    print(c)