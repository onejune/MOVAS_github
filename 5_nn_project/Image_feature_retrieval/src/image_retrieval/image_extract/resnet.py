# Copyright 2019 The Vearch Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# ==============================================================================

import cv2
import numpy as np
from PIL import Image
import torch
from torchvision import transforms
import torchvision.models as models

import torch.nn.functional as F


class BaseModel(object):
    def __init__(self):
        self.image_size = 224
        self.dimision = 2048
        self.load_model()

    def load_model(self):
        self.device = torch.device(
            "cuda" if torch.cuda.is_available() else "cpu")
        self.model = models.resnet50(pretrained=True).to(self.device)
        self.model = self.model.eval()
        self.PIXEL_MEANS = torch.tensor((0.485, 0.456, 0.406)).to(self.device)
        self.PIXEL_STDS = torch.tensor((0.229, 0.224, 0.225)).to(self.device)
        self.num = torch.tensor(255.0).to(self.device)
        # self.model.cuda()

    def preprocess_input(self, image):
        image = cv2.resize(image, (self.image_size, self.image_size))
        # gpu version
        image_tensor = torch.from_numpy(image.copy()).to(self.device).float()
        image_tensor /= self.num
        image_tensor -= self.PIXEL_MEANS
        image_tensor /= self.PIXEL_STDS
        image_tensor = image_tensor.permute(2, 0, 1)
        return image_tensor

    def forward(self, x):
        # x = torch.stack(x)
        # x = x.to(self.device)
        x = self.preprocess_input(x).unsqueeze(0)
        x = self.model.conv1(x)
        x = self.model.bn1(x)
        x = self.model.relu(x)
        x = self.model.maxpool(x)

        x = self.model.layer1(x)
        x = self.model.layer2(x)
        x = self.model.layer3(x)
        x = self.model.layer4(x)
        x = F.avg_pool2d(x, kernel_size=x.size()[2:])
        x = torch.squeeze(x, -1)
        x = torch.squeeze(x, -1)
        return self.torch2list(x)

    def torch2list(self, torch_data):
        return torch_data.cpu().detach().numpy().tolist()


def load_model():
    return BaseModel()


def get_feature(image_data):
    model = load_model()
    model.load_model()
    #image_data = cv2.imread(image)
    print(image_data.shape)
    feat = model.forward(image_data)
    #print(len(feat))
    feature = (feat[0] / np.linalg.norm(feat[0])).tolist()

    return feature


def image_rotate(img):
    img = cv2.imread(img)
    rows, cols = img.shape[:2]
    # 第一个参数是旋转中心，第二个参数是旋转角度，第三个参数是缩放比例
    M1 = cv2.getRotationMatrix2D((cols / 2, rows / 2), 55, 0.5)
    M2 = cv2.getRotationMatrix2D((cols / 2, rows / 2), 45, 2)
    M3 = cv2.getRotationMatrix2D((cols / 2, rows / 2), 65, 1)
    res1 = cv2.warpAffine(img, M1, (cols, rows))
    res2 = cv2.warpAffine(img, M2, (cols, rows))
    res3 = cv2.warpAffine(img, M3, (cols, rows))

    # cv2.imshow('ori', img)
    # cv2.imshow('rotate1', res1)
    # cv2.imshow('rotate2', res2)
    # cv2.imshow('rotate3', res3)
    # cv2.waitKey(0)
    # cv2.destroyAllWindows()
    return res1, res2, res3


def image_cut(img):
    img = cv2.imread(img)
    print("image_cut:", img)
    height, width = img.shape[:2]
    img = img[int(height / 3):int(height * 2 / 3),
              int(width / 3):int(width * 2 / 3)]

    height = len(img)
    width = len(img[0])

    # cv2.imshow('cut_image', img)
    # cv2.waitKey(0)
    return img


if __name__ == "__main__":
    import sys
    img = "/Users/Shared/Relocated_Items/Security/onejune/0-git/m_script_collection/6_torch_work/Image_feature_retrieval/src/image_retrieval/image_extract/big_image.jpg"
    img_data = cv2.imread(img)
    fea0 = get_feature(img_data)
    fea0 = torch.Tensor(fea0).unsqueeze(0)
    print(fea0)

    m1, m2, m3 = image_rotate(img)
    fea1 = get_feature(m1)
    fea1 = torch.Tensor(fea1).unsqueeze(0)
    print(fea1)

    fea2 = get_feature(m2)
    fea2 = torch.Tensor(fea2).unsqueeze(0)
    print(fea2)

    fea3 = get_feature(m3)
    fea3 = torch.Tensor(fea3).unsqueeze(0)
    print(fea3)

    m4 = image_cut(img)
    fea4 = get_feature(m4)
    fea4 = torch.Tensor(fea4).unsqueeze(0)
    print(fea4)

    sim = F.cosine_similarity(fea0, fea1)
    print(sim)
    sim = F.cosine_similarity(fea0, fea2)
    print(sim)
    sim = F.cosine_similarity(fea0, fea3)
    print(sim)
    sim = F.cosine_similarity(fea2, fea4)
    print(sim)
    sim = F.cosine_similarity(fea0, fea4)
    print(sim)