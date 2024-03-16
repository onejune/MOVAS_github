# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.
#
#!/bin/bash

DIR="/home/ubuntu/dcim/dataset/mtg_image_106/train"
ARCH="alexnet"
LR=0.05
WD=-5
K=1000
WORKERS=12
EXP="./test/exp"
PYTHON="/home/ubuntu/anaconda3/envs/aws_neuron_pytorch_p36/bin/python"

mkdir -p ${EXP}

CUDA_VISIBLE_DEVICES=0 ${PYTHON} main.py ${DIR} --exp ${EXP} --arch ${ARCH} \
  --lr ${LR} --wd ${WD} --k ${K} --sobel --verbose --workers ${WORKERS}
