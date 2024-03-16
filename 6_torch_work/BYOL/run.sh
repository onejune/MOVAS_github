#########################################################################
# File Name: run.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Fri Jul 30 02:15:58 2021
#########################################################################
#!/bin/bash

image_path='/home/ubuntu/dcim/dataset/mtg_image_106/train/image_sid'

python train.py --image_folder $image_path
