import os, sys

black_dmptag = ['games', 'games_arcade', 'hypercasual', 'entertainment', 'relax', 'games_casual']
black_dmptag = []

tag_dict = {}
pkg_dict = {}
pkg_sid_cnt = {}
dmptag_sid_cnt = {}
pkg_no_tag = {}


dmptag_mapping = {}
fin = open('output/dmptag_mapping.dat')
for line in fin:
    arr = line.strip().split()
    if len(arr) < 2:
        continue
    dmptag_mapping[arr[1]] = arr[0]

#最终选用的category,归一化和去掉了数据量太少的
dmptag_selected = {}
fin = open('output/dmptag_selected.dat')
for line in fin:
    arr = line.strip().split()
    dmptag_selected[arr[0]] = 1

sum_sid = 0
fin = open('output/pkg_sid_cnt')
for line in fin:
    arr = line.strip().split('\t')
    if len(arr) > 1:
        pkg_sid_cnt[arr[0]] = arr[1]
        sum_sid += int(arr[1])

package_no_sid_map = {}
fin = open('output/package_dmptag_ori.dat')
for line in fin:
    arr = line.strip().split()
    brr = arr[0].split('\001')
    if brr[0] != 'package' and brr[0] != 'dsp_pkg':
        continue
    pkg = brr[1]
    arr = line.strip().split('|')
    dmptag = []
    for d in arr:
        if 'dmp_tag' in d:
            crr = d.split('#')
            for ele in crr:
                tag = ele.split('=')[1]
                tag = dmptag_mapping.get(tag, tag)
                dmptag.append(tag)
    if not dmptag:
        continue
    for tag in dmptag:
        tag_dict.setdefault(tag, [])
        tag_dict[tag].append(pkg)
    #pkg_ps = pkg + '\t' + '\t'.join(dmptag)
    pkg_dict[pkg] = list(set(dmptag))
    if pkg not in pkg_sid_cnt:
        package_no_sid_map[pkg] = list(set(dmptag))

for pkg in pkg_sid_cnt:
    if pkg not in pkg_dict:
        pkg_no_tag[pkg] = int(pkg_sid_cnt[pkg])
r = sorted(pkg_no_tag.items(), key = lambda d : d[1], reverse = True)
fout = open('output/pkg_no_tag.dat', 'w')
for e in r:
    fout.write(e[0] + '\t' + str(e[1]) + '\n')

fout = open('output/pkg_no_sid.dat', 'w')
for e in package_no_sid_map:
    tag = '\t'.join(package_no_sid_map[e])
    fout.write(e + '\t' + tag + '\n')

pkg_cnt = len(pkg_dict)
tag_ps = ''
for tag in tag_dict:
    p_len = len(tag_dict[tag])
    ratio = p_len * 1.0 / pkg_cnt
    tag_ps += tag + '\t' + str(ratio) + '\t' + str(p_len) + '\n'
#print(tag_ps)
fout = open('output/dmptag_all.dat','w')
fout.write(tag_ps)

pkg_ps = ''
for pkg in pkg_dict:
    pkg_ps += pkg
    sid_cnt = pkg_sid_cnt.get(pkg, '0')
    pkg_ps += '\t' + sid_cnt
    tag_list = pkg_dict[pkg]
    r = sorted(tag_list, key = lambda d: len(tag_dict[d]), reverse = True)
    for tag in r:
        p_cnt = len(tag_dict[tag])
        if tag not in dmptag_selected or p_cnt < 1:
            continue
        dmptag_sid_cnt.setdefault(tag, 0)
        dmptag_sid_cnt[tag] += int(sid_cnt)
        pkg_ps += '\t' + tag + ':' + str(len(tag_dict[tag]))
    pkg_ps += '\n'
#print(pkg_ps)
fout = open('output/package_tag.dat', 'w')
fout.write(pkg_ps + '\n')

fout = open('output/dmptag_sid_cnt.dat', 'w')
r = sorted(dmptag_sid_cnt.items(), key = lambda kv:(kv[1], kv[0]), reverse = True)
for tag in r:
    ratio = tag[1] * 1.0 / sum_sid
    fout.write(tag[0] + '\t' + str(tag[1]) + '\t' + str(ratio) + '\n')

