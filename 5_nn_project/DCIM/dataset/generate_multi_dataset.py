import os ,sys, random

dest_dir = '/home/ubuntu/dcim/dataset/mtg_202105_06_multi_label_video/'

sid_dir_list = ['/home/ubuntu/dcim/dataset/mtg_video_202105_sf/train/', '/home/ubuntu/dcim/dataset/202105_202106_CnUs_image_all_imp/', '/home/ubuntu/dcim/dataset/202106_CnUs_video_fram5_packageDir_increase_imp/', '/home/ubuntu/dcim/dataset/202101_202106_videoFrame10_allCountry_hitPackage/']

#sid_dir_list = ['/home/ubuntu/dcim/dataset/202101_202106_videoFrame10_allCountry_hitPackage/']
sid_dir_list = ['/home/ubuntu/dcim/dataset/202107_image_sid_increase_imp/']

all_tag_list = []
fin = open('./data/labels')
for line in fin:
    all_tag_list.append(line.strip())

sample_labels = []
pkg_tag = {}
fin = open('output/package_tag.dat')
for line in fin:
    arr = line.split()
    if len(arr) < 3:
        continue
    pkg = arr[0]
    pkg_tag[pkg] = []
    for tag in arr[2:]:
        brr = tag.split(':')
        t = brr[0]
        pkg_tag[pkg].append(t)
        sample_labels.append(t)

sample_labels = list(set(sample_labels))
if len(sample_labels) != len(all_tag_list):
    print('labels count is valid.....')
    sys.exit()

tag_img_map = {}
img_dmptag_map = {}
img_label_list = {}

for sid_dir in sid_dir_list:
    pkg_list = os.listdir(sid_dir)
    pkg_cnt = len(pkg_list)
    for pkg in pkg_list:
        dmptag = pkg_tag.get(pkg)
        if not dmptag:
            continue
        label_list = []
        for tag in all_tag_list:
            if tag in dmptag:
                label_list.append('1')
            else:
                label_list.append('0')
            tag_img_map.setdefault(tag, [])
        
        sid_list = os.listdir(sid_dir + pkg)
        sid_cnt = len(sid_list)
        if sid_cnt == 0:
            continue
        image_file_path = sid_dir + pkg
        for img in os.listdir(image_file_path):
            if img in img_dmptag_map:
                continue
            img_dmptag_map[img] = dmptag
            pth = image_file_path + '/' + img
            os.system('cp ' + pth + ' ' + dest_dir + img)
            img_label_list[img] = label_list
            for tag in dmptag:
                tag_img_map[tag].append(img)

f_train = open('result/train.csv', 'w')
f_val = open('result/val.csv', 'w')
f_test = open('result/test.csv', 'w')

f_train.write('package_name,' + ','.join(all_tag_list) + '\n')
f_val.write('package_name,' + ','.join(all_tag_list) + '\n')
f_test.write('package_name,' + ','.join(all_tag_list) + '\n')

for img in img_dmptag_map:
    img_name = img
    label_list = img_label_list[img_name]
    label_str = ','.join(label_list)
    rnd = random.randint(1, 100)
    if rnd <= 80:
        f_train.write(img_name.split('.')[0] + ',' + label_str + '\n')
    elif rnd <= 90:
        f_val.write(img_name.split('.')[0] + ',' + label_str + '\n')
    else:
        f_test.write(img_name.split('.')[0] + ',' + label_str + '\n')


            



