#########################################################################
# File Name: run_train.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Sat Jul 31 23:51:32 2021
#########################################################################
#!/bin/bash

python main.py  --epochs 50 --batch_size 16  --lr 0.00001 --optim 'adam' --layers 3  --dataset 'mtg' --use_lmt --grad_ac_step 2 --dataroot /home/ubuntu/dcim/dataset/
