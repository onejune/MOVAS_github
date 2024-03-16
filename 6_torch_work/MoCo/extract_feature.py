#!/usr/bin/env python
import sys
import torch
import torch.nn as nn
import torchvision.transforms as transforms
import torchvision.datasets as datasets
import torchvision.models as models


class BaseModel(nn.Module):
    def __init__(self, checkpoint_path):
        super(BaseModel, self).__init__()
        self.checkpoint_path = checkpoint_path
        self.load_model()

    def load_model(self):
        self.model = models.resnet50()
        self.model = self.model.eval()
        checkpoint = torch.load(self.checkpoint_path)
        state_dict = checkpoint['state_dict']
        for k in list(state_dict.keys()):
            if k.startswith('module.encoder_q'
                            ) and not k.startswith('module.encoder_q.fc'):
                state_dict[k[len("module.encoder_q."):]] = state_dict[k]
            del state_dict[k]
        msg = self.model.load_state_dict(state_dict, strict=False)
        print(msg)

        self.features = nn.Sequential(*list(self.model.children())[:-1])

    def forward(self, x):
        q = self.features(x)
        q = nn.functional.normalize(q, dim=1)
        x = torch.squeeze(q, -1)
        x = torch.squeeze(x, -1)
        return self.torch2list(x)

    def torch2list(self, torch_data):
        return torch_data.cpu().detach().numpy().tolist()


class ImageFolderWithPaths(datasets.ImageFolder):
    """Custom dataset that includes image file paths. Extends
    torchvision.datasets.ImageFolder
    """

    # override the __getitem__ method. this is the method that dataloader calls
    def __getitem__(self, index):
        # this is what ImageFolder normally returns
        original_tuple = super(ImageFolderWithPaths, self).__getitem__(index)
        # the image file path
        path = self.imgs[index][0]
        # make a new tuple that includes original and the path
        tuple_with_path = (original_tuple + (path, ))
        return tuple_with_path


if __name__ == "__main__":
    checkpoint_path = sys.argv[1]
    sample_dir = sys.argv[2]
    batch_size = 64
    model = BaseModel(checkpoint_path)
    normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])
    dataset = ImageFolderWithPaths(
        sample_dir,
        transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.RandomGrayscale(p=0.6),
            transforms.ToTensor(),
            normalize,
        ]))
    sample_loader = torch.utils.data.DataLoader(dataset,
                                                batch_size=batch_size,
                                                shuffle=False,
                                                num_workers=1,
                                                pin_memory=True)

    for i, (images, target, image_name) in enumerate(sample_loader):
        # compute output
        print(i, target, image_name)
        output = model(images)
