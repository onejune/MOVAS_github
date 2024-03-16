import os, sys, math
import datetime
import time

min_imp = 100
min_ins = 10
interval_days = 3

def last_n_day(mytime, n):                                                              
    myday = datetime.datetime(int(mytime[0:4]),int(mytime[4 :6 ]),int(mytime[6 :8 ]) )      
    #now = datetime.datetime.now()                                                      
    delta = datetime.timedelta(days=n)                                                  
    my_yestoday = myday + delta                                                          
    my_yes_time = my_yestoday.strftime( '%Y%m%d')                                        
    return my_yes_time 

f_name = sys.argv[1]
cal_date = sys.argv[2]
output = "output/cam_rank_" + cal_date + ".dat"
min_date = last_n_day(cal_date, -1 * interval_days)
print f_name, output, cal_date, min_date

cpm_dict = {}
fin = open(f_name)
n = 0
with open(f_name) as fin:
    for line in fin:
        arr = line.strip().split('\t')
        if len(arr) < 11:
            break
        dtm, ad_type, app_id, cc, pkg_name, cam_id, cr_cpm, price, imp, ins, rev = arr[0:11]
        if dtm < min_date or dtm >= cal_date:
            continue
        try:
            cr_cpm = float(cr_cpm)
            price = float(price)
            imp = float(imp)
        except:
            continue
        try:
            ins = float(ins)
            rev = float(rev)
        except:
            ins = 0
            rev = 0
        key = cal_date + '#' + ad_type + '#' + app_id + '#' + cc
        cpm_dict.setdefault(key, {})
        query = cam_id
        cpm_dict[key].setdefault(query, [0, 0, 0, 0])
        cpm_dict[key][query][0] += cr_cpm
        cpm_dict[key][query][1] += imp
        cpm_dict[key][query][2] += ins
        cpm_dict[key][query][3] += rev
        n += 1
print "sum_record:", n

for key in cpm_dict:
    query_data = cpm_dict[key]
    for query in query_data:
        cr_cpm, imp, ins, rev = query_data[query]
        if imp < min_imp or ins < min_ins:
            cpm = 0
            pcoc = 0
        else:
            cpm = rev * 1000 / imp
            pcoc = cr_cpm / 1000 / rev
        cpm_dict[key][query].extend([pcoc, cpm])

fout = open(output, 'w')
max_rank = 10
for key in cpm_dict:
    query_data = cpm_dict[key]
    sorted_list = sorted(query_data.items(), key = lambda d : d[1][5], reverse = True)
    le = 0
    for item in sorted_list:
        cpm = item[1][5]
        if cpm != 0:
            le += 1
    n = 0
    for item in sorted_list:
        cpm = item[1][5]
        if cpm == 0:
            continue
        if le < max_rank:
            rank = n
        else:
            rank = int(n / round(le / max_rank))
        #cpm_ln = int(round(math.log(100 * cpm)))
        cpm_ln = 0
        if cpm >= 10:
            cpm_ln = int(cpm / 10) * 10
        elif cpm >= 1:
            cpm_ln = int(cpm)
        else:
            cpm_ln = ('%.1f' % cpm)

        prefix = key.split('#')[-2:]
        final_rank = n
        rank_prefix = str(final_rank)
        st = [str(d) for d in item[1]]
        fout.write(key + '\t' + item[0] + '\t' + rank_prefix + '\t' + str(cpm_ln) + '\t' + ','.join(st) + '\n')
        #print key, item[0], item[1], rank
        n += 1

