import os, sys

video_root="/home/ubuntu/dcim/dataset/mtg_202105_06_multi_label_video/"
video_cate_path="/home/ubuntu/wanjun/m_script_collection/5_NN/DCIM/dataset/results/train.csv"
output="mtg_video_dataset.csv"

fout = open(output, 'w')
category_index = {}
fin = open(video_cate_path)
for line in fin:
    arr = line.strip().split(',')
    try:
        float(arr[1])
    except:
        cate_list = arr[1:]
        for i in range(len(cate_list)):
            category_index[cate_list[i]] = i
        continue
    video_path = video_root + arr[0] + '.jpg'
    cate_list = arr[1:]
    cates = ''
    for i in range(len(cate_list)):
        if int(cate_list[i]) > 0:
            cates += str(i) + ';'

    fout.write(video_path + ',' + cates.rstrip(';') + '\n')

fout.close()
print(category_index)
fout = open('cate_index.dat', 'w')
for k in category_index:
    fout.write(str(category_index[k]) + '\t' + k + '\n')

