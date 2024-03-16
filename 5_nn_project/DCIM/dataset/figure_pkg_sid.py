import os, sys

sid_dir_list = ['/home/ubuntu/dcim/dataset/mtg_video_202105_sf/train/', '/home/ubuntu/dcim/dataset/202105_202106_CnUs_image_all_imp/', '/home/ubuntu/dcim/dataset/202106_CnUs_video_fram5_packageDir_increase_imp/', '/home/ubuntu/dcim/dataset/202101_202106_videoFrame10_allCountry_hitPackage/']
pkg_sid_ps = ''

lst = {}
for sid_dir in sid_dir_list:
    pkg_list = os.listdir(sid_dir)
    pkg_cnt = len(pkg_list)
    for pkg in pkg_list:
        sid_list = os.listdir(sid_dir + pkg)
        sid_cnt = len(sid_list)
        #pkg_sid_ps += pkg + '\t' + str(sid_cnt) + '\n'
        lst.setdefault(pkg, 0)
        lst[pkg] += sid_cnt

r = sorted(lst.items(), key = lambda d : d[1], reverse = True)
sum_sid = 0
for pkg in r:
    pkg_sid_ps += pkg[0] + '\t' + str(pkg[1]) + '\n'
    sum_sid += pkg[1]

fout = open('output/pkg_sid_cnt', 'w')
fout.write(pkg_sid_ps + '\n')
fout.write('sum_cnt:' + str(sum_sid))
