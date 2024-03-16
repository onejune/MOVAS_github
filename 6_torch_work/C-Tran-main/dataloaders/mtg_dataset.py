import os
import torch
import numpy as np
from PIL import Image
from dataloaders.data_utils import get_unk_mask_indices


class MtgDataset(torch.utils.data.Dataset):
    def __init__(
            self,
            img_dir='/home/ubuntu/dcim/dataset/mtg_202105_06_multi_label_video',
            anno_path=None,
            image_transform=None,
            labels_path='/home/ubuntu/dcim/dataset/dmptag/train.csv',
            known_labels=0,
            testing=False,
            use_difficult=False):
        self.num_labels = 78
        self.img_dir = img_dir
        self.known_labels = known_labels
        self.testing = testing
        self.img_names = []
        self.labels = []
        self.label_names = []
        i = 0
        with open(labels_path, 'r') as f:
            for line in f:
                label_vector = np.zeros(self.num_labels)
                line = line.rstrip()
                arr = line.split(',')
                try:
                    float(arr[1])
                except:
                    if i == 0:
                        self.label_names = arr[1:]
                    continue
                self.img_names.append(arr[0])
                label_vector = [float(d) for d in arr[1:]]
                self.labels.append(label_vector)
                i += 1

        # self.labels = np.array(self.labels).astype(np.float32)
        self.labels = np.array(self.labels).astype(int)
        self.image_transform = image_transform
        self.epoch = 1

    def __getitem__(self, index):
        name = self.img_names[index] + '.jpg'
        image = Image.open(os.path.join(self.img_dir, name)).convert('RGB')

        if self.image_transform:
            image = self.image_transform(image)

        labels = torch.Tensor(self.labels[index])
        unk_mask_indices = get_unk_mask_indices(image, self.testing,
                                                self.num_labels,
                                                self.known_labels, self.epoch)

        mask = labels.clone()
        mask.scatter_(0, torch.Tensor(unk_mask_indices).long(), -1)

        sample = {}
        sample['image'] = image
        sample['labels'] = labels
        sample['mask'] = mask
        sample['imageIDs'] = str(name)

        return sample

    def __len__(self):
        return len(self.img_names)
