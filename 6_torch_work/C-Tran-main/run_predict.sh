#########################################################################
# File Name: run_train.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Sat Jul 31 23:51:32 2021
#########################################################################
#!/bin/bash

python main.py  \
	--dataroot /home/ubuntu/dcim/dataset/ \
	--inference \
	--saved_model_name ./results/mtg.3layer.bsz_32.adam1e-05.lmt.unk_loss/best_model.pt \
	--test_batch_size 32 \
	--dataset 'mtg' --use_lmt \
	--predict_out_path ./label_predict.out
