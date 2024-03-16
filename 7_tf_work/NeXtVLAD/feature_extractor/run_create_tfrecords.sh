#########################################################################
# File Name: run_create_tfrecords.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu Aug  5 04:36:26 2021
#########################################################################
#!/bin/bash

python extract_tfrecords_main.py \
	--input_videos_csv mtg_video_dataset.csv \
	--output_tfrecords_file mtg_video.tfrecord
