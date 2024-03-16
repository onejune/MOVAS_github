import os, sys, datetime

ftrl_config = {}
fin = open('./conf/m_ftrl_config.dat')
for line in fin:
    if len(line) < 10:
        continue
    arr = line.strip().split(' ')
    arr = arr[1].split('|')
    for d in arr:
        brr = d.split(':')
        name = brr[0]
        percent = float(brr[1])
        if percent == 0:
            continue
        total = 100
        ftrl_config[name] = total / (percent + 0.01)
fin.close()
ftrl_config['default'] = 1

if len(sys.argv) == 1:
    print 'argv error!'
    sys.exit()
file_name = sys.argv[1]

head_list = ['bucket', 'impression', 'click', 'install', 'revenue', 'cost', 'ctr', 'cvr', 'acp', 'cpm', 'fillrate', 'roi']
#base_map = {'1' : '1_uhdii', '2' : '2_ccpc_base', '3' : '3_creative_base'}
#base_map = {'1' : '1_mooc_exp1', '3' : '3_retrieval_base'}
base_map = {'1' : '1_nn_base', '2' : '2_cnrr_base'}

data_map = {}
head_line = []
for line in open(file_name):
    line = line.strip()
    arr = line.split()
    one_record = {}
    bucket = arr[0].strip()
    if bucket == 'bucket':
        head_line = arr
        continue
    layer = bucket[0]
    if layer not in base_map:
        continue
    strategy = bucket[2:]
    if strategy not in ftrl_config:
        continue
    one_record['bucket'] = bucket
    for i in range(1, len(arr)):
        try:
            if arr[i] == 'NULL':
                arr[i] = '0'
            f = float(arr[i])
            field = head_line[i]
            #field = head_list[i]
            if field == 'revenue' or field == 'cost':
                f = float("%.1f" % f)
            else:
                f = float("%.5f" % f)
            one_record[field] = f
        except:
            pass
    data_map[bucket] = {}
    data_map[bucket]['data'] = one_record
    data_map[bucket]['layer'] = layer
    data_map[bucket]['base'] = base_map[layer]


for bucket in data_map:
    data = data_map[bucket]
    base = data['base']
    r = data['data']
    cpm_diff = 100 * (r['cpm'] / data_map[base]['data']['cpm'] -1)
    if cpm_diff > 0:
        cpm_diff = '+%.2f%%' % cpm_diff
    else:
        cpm_diff = '%.2f%%' % cpm_diff
    data_map[bucket]['data']['cpm_diff'] = cpm_diff

    ctr_diff = 100 * (r['ctr'] / data_map[base]['data']['ctr'] -1)
    if ctr_diff > 0:
        ctr_diff = '+%.2f%%' % ctr_diff
    else:
        ctr_diff = '%.2f%%' % ctr_diff
    data_map[bucket]['data']['ctr_diff'] = ctr_diff

    ctr_diff = 100 * (r['cvr'] / data_map[base]['data']['cvr'] -1)
    if ctr_diff > 0:
        ctr_diff = '+%.2f%%' % ctr_diff
    else:
        ctr_diff = '%.2f%%' % ctr_diff
    data_map[bucket]['data']['cvr_diff'] = ctr_diff
    
    ctr_diff = 100 * (r['acp'] / data_map[base]['data']['acp'] -1)
    if ctr_diff > 0:
        ctr_diff = '+%.2f%%' % ctr_diff
    else:
        ctr_diff = '%.2f%%' % ctr_diff
    data_map[bucket]['data']['acp_diff'] = ctr_diff
    
    ctr_diff = 100 * (r['roi'] / data_map[base]['data']['roi'] -1)
    if ctr_diff > 0:
        ctr_diff = '+%.2f%%' % ctr_diff
    else:
        ctr_diff = '%.2f%%' % ctr_diff
    data_map[bucket]['data']['roi_diff'] = ctr_diff

    ctr_diff = 100 * ((r['revenue'] * ftrl_config[bucket[2:]]) / (data_map[base]['data']['revenue']*ftrl_config[base[2:]])-1 )
    if ctr_diff > 0:
        ctr_diff = '+%.2f%%' % ctr_diff
    else:
        ctr_diff = '%.2f%%' % ctr_diff
    data_map[bucket]['data']['revenue_diff'] = ctr_diff

    ctr_diff = 100 * ((r['cost'] * ftrl_config[bucket[2:]]) / (data_map[base]['data']['cost']*ftrl_config[base[2:]])-1 )
    if ctr_diff > 0:
        ctr_diff = '+%.2f%%' % ctr_diff
    else:
        ctr_diff = '%.2f%%' % ctr_diff
    data_map[bucket]['data']['cost_diff'] = ctr_diff

#figure field max width
field_len_map = {}
header_order = ['bucket', 'roi_diff', 'revenue_diff', 'cost_diff', 'cpm_diff', 'ctr_diff', 'cvr_diff', 'acp_diff', 'impression', 'install', 'revenue', 'cost', 'ctr', 'cvr', 'acp', 'cpm', 'roi', 'fillrate']
header_order = ['bucket', 'roi_diff', 'revenue_diff', 'cost_diff', 'cpm_diff', 'acp_diff', 'impression', 'install', 'revenue', 'cost', 'cpm', 'roi', 'fillrate']
for i in range(0, len(header_order)):
    field_len_map[header_order[i]] = len(header_order[i])
for bucket in data_map:
    data = data_map[bucket]['data']
    for field in header_order:
        value = str(data[field])
        if len(value) > field_len_map[field]:
            field_len_map[field] = len(value)

total_line_len = 0
for k in field_len_map:
    total_line_len += field_len_map[k] + 4

#print out formated
bucket_list = sorted(data_map.iteritems(), key = lambda d:d[0])
bucket_list = [d[0] for d in bucket_list]
ps = '-' * (total_line_len + 4) + '\n'
for field in header_order:
    if field != 'bucket':
        ps += ("%" + str(field_len_map[field] + 4) + "s") % (field)
    else:
        ps += ("%-" + str(field_len_map[field] + 4) + "s") % (field)

ps += '\n'
ps += '-' * (total_line_len + 4) + '\n'
pre = '0'
for bucket in bucket_list:
    data = data_map[bucket]['data']
    pre = layer
    layer = data_map[bucket]['layer']
    if layer != pre and layer != '0':
        ps += '\n'
    for field in header_order:
        if field != 'bucket':
            ps += ("%" + str(field_len_map[field] + 4) + "s") % (data[field])
        else:
            ps += ("%-" + str(field_len_map[field] + 4) + "s") % (data[field])
    ps += '\n'
ps +=  '-' * (total_line_len + 4) + '\n\n'

fout = open(file_name + '.format', 'w')
fout.write(ps)
fout.close()



