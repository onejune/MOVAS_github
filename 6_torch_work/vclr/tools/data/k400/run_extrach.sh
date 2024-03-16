#########################################################################
# File Name: run_extrach.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Sun Aug  8 07:38:07 2021
#########################################################################
#!/bin/bash

source_dir="/home/ubuntu/dcim/dataset/vclr_videos"
target_dir="/home/ubuntu/dcim/dataset/vclr_videos/video_frames"

python extract_frames.py --source_dir $source_dir \
	--target_dir $target_dir
