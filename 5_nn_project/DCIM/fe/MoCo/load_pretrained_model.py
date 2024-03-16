import torch
import torchvision.models as models
from torch import nn
import torch.nn.functional as F
import moco.loader
import moco.builder

model_names = sorted(name for name in models.__dict__
                     if name.islower() and not name.startswith("__")
                     and callable(models.__dict__[name]))

print(model_names)

model_dict = models.__dict__
for k in model_dict:
    print("model_dict----:", k, model_dict[k])

pretained_path = "./moco_v2_800ep_pretrain.pth.tar"
model = models.resnet50(pretrained=False)

#model = moco.builder.MoCo(models.__dict__["resnet50"])

print('load pretrained model...')

checkpoint = torch.load(pretained_path)
state_dict = checkpoint['state_dict']

for k in list(state_dict.keys()):
    print(k)
    if k.startswith('module.encoder_q') and not k.startswith('module.encoder_q.fc'):
    	state_dict[k[len("module.encoder_q."):]] = state_dict[k]
    del state_dict[k]


msg = model.load_state_dict(state_dict, strict=False)
print(msg)


