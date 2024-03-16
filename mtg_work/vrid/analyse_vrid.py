import os, sys

cur_path = os.path.realpath(__file__)
cur_dir = os.path.dirname(cur_path)
parent_dir = os.path.dirname(cur_dir)
sys.path.append(parent_dir)

vrid_file = os.path.join(cur_dir, "data/vrid_video_1024.txt")
sid_vrid_dict = {}
vrid_dict = {}
fin = open(vrid_file)
for line in fin:
    arr = line.strip().split()
    source_id = arr[0]
    vrid = arr[1]
    vrid_dict[vrid] = source_id
    sid_vrid_dict[source_id] = vrid
print("total sids:", len(sid_vrid_dict))
print("total vrids:", len(vrid_dict))

imp_file = os.path.join(cur_dir, "./data/crt_imp_2021080800_2121091023.txt")
rev_file = os.path.join(cur_dir, "./data/crt_ins_2021080800_2121091023.txt")
sid_imp = {}
fin = open(imp_file)
for line in fin:
    arr = line.strip().split()
    if len(arr) < 5:
        continue
    adtype, cc, campaign, sid, imp = arr[0:5]
    if cc not in('cn', 'us'):
        continue
    sid = sid.split('_')[0]
    try:
        imp = int(imp)
    except:
        continue
    key = sid + "\t" + adtype + "\t" + cc
    key = sid
    if key in sid_imp:
        sid_imp[key] += imp
    else:
        sid_imp[key] = imp
print("all imped sids:", len(sid_imp))

sid_rev = {}
vrid_rev = {}
fin = open(rev_file)
for line in fin:
    arr = line.strip().split()
    if len(arr) < 6:
        continue
    adtype, cc, campaign, sid, ins, rev = arr[0:6]
    if cc not in('cn', 'us'):
        continue
    sid = sid.split('_')[0]
    vrid = sid_vrid_dict.get(sid, 'null')
    try:
        ins = int(ins)
        rev = float(rev)
    except:
        continue
    if vrid in vrid_rev:
        vrid_rev[vrid] += rev
    else:
        vrid_rev[vrid] = rev
    key = sid + "\t" + adtype + "\t" + cc
    key = sid
    if key in sid_rev:
        sid_rev[key][0] += ins
        sid_rev[key][1] += rev
    else:
        sid_rev[key] = [ins, rev]

print("all insed sid:", len(sid_rev))

imped_vrid = []
insed_vrid = []
imped_sids = {}
vrid_sid_list = {}
null_sid = 0

for key in sid_imp:
    imp = sid_imp[key]
    ins, rev = sid_rev.get(key, [0, 0])
    sid = key.split()[0]
    vrid = sid_vrid_dict.get(sid, 'null')
    if vrid != 'null' and imp >= 10:
        imped_vrid.append(vrid)
        imped_sids[sid] = vrid
        if vrid in vrid_sid_list:
            vrid_sid_list[vrid].append(sid)
        else:
            vrid_sid_list[vrid] = [sid]
    if vrid != 'null' and ins > 0:
        insed_vrid.append(vrid)
    if vrid == 'null':
        null_sid += 1
imped_vrid = list(set(imped_vrid))
insed_vrid = list(set(insed_vrid))

print('sids without vrid:', null_sid)
print('sids with imp(>=100):', len(imped_sids))
print('vrid with imp(>=100):', len(imped_vrid), ' squeeze rate:', len(imped_vrid) * 1.0 / len(imped_sids))
#print('vrid with ins:', len(insed_vrid))

cnt = 0
sid_vrid_lst = []
rev1 = 0
rev2 = 0
for vrid in vrid_sid_list:
    sids = vrid_sid_list[vrid]
    sids = list(set(sids))
    if len(sids) > 1:
        cnt += 1
        sid_vrid_lst.extend(sids)
        rev1 += vrid_rev.get(vrid, 0)
    rev2 += vrid_rev.get(vrid, 0)
sid_vrid_lst = list(set(sid_vrid_lst))
acnt = len(sid_vrid_lst)
        
print("vrid with sids(>1):", cnt, " squeezed sids:", acnt, ' squeeze rate:', cnt * 1.0 / acnt,
      ' rev:', rev1, ' cover_rate:', rev1 / rev2)

output = os.path.join(cur_dir, "sid_cpm.txt")
fout = open(output, 'w')
fout.write("sid\tvrid\timp\tins\trev\n")
for key in sid_imp:
    imp = sid_imp[key]
    ins, rev = sid_rev.get(key, [0, 0])
    sid = key.split()[0]
    vrid = sid_vrid_dict.get(sid, 'null')
    fout.write(key + '\t' + vrid + '\t' + str(imp) + '\t' + str(ins) + '\t' + str(rev) + '\n')
fout.close()