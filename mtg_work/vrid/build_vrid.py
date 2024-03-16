import os, sys

cur_path = os.path.realpath(__file__)
cur_dir = os.path.dirname(cur_path)
parent_dir = os.path.dirname(cur_dir)
sys.path.append(parent_dir)

min_sim = 0.8
sourceId_vrid_dict = {}
sourceId_package_dict = {}
last_vrid = -1
old_sid_cnt = 0
old_vrid_cnt = 0
old_pkg_cnt = 0

# source_id_a is old, source_id_b is new
def figure_vrid(source_id_a, source_id_b, is_sim):
    global sourceId_vrid_dict
    global last_vrid
    if is_sim == 1:
        if source_id_a not in sourceId_vrid_dict:
            sourceId_vrid_dict[source_id_a] = last_vrid + 1
            last_vrid += 1
        sourceId_vrid_dict[source_id_b] = sourceId_vrid_dict[source_id_a]
    else:
        if source_id_a not in sourceId_vrid_dict:
            sourceId_vrid_dict[source_id_a] = last_vrid + 1
            last_vrid += 1
        if source_id_b not in sourceId_vrid_dict:
            sourceId_vrid_dict[source_id_b] = last_vrid + 1
            last_vrid += 1


def vrid_gen(package, source_id_list, sim_vec_list):
    global sourceId_package_dict
    if not package:
        return
    sid_cnt = len(source_id_list)
    if sid_cnt == 1:
        figure_vrid(source_id_list[0], source_id_list[0], 1)
        return
    sim_vec_cnt = len(sim_vec_list)
    for i in range(sid_cnt):
        source_id = source_id_list[i]
        if source_id not in sourceId_package_dict:
            sourceId_package_dict[source_id] = []
        sourceId_package_dict[source_id].append(package)

        if i >= sim_vec_cnt:
            break
        sim_vec = sim_vec_list[i]
        vec_len = len(sim_vec)
        if vec_len != sid_cnt - i - 1:
            print("err sim vector for ", source_id, vec_len, sid_cnt, i)
            break
        k = 0
        for j in range(i + 1, sid_cnt):
            cur_sid = source_id_list[j]
            if sim_vec[k] >= min_sim:
                figure_vrid(source_id, cur_sid, 1)
            k += 1


def load_vrid(vrid_file):
    global sourceId_vrid_dict
    global last_vrid
    global sourceId_package_dict
    global old_sid_cnt
    global old_vrid_cnt
    global old_pkg_cnt

    try:
        fin = open(vrid_file)
    except:
        return
    vrid = -1
    pkg_list = []
    for line in fin:
        arr = line.strip().split()
        sid = arr[0]
        vrid = int(arr[1])
        pkg = arr[2]
        sourceId_vrid_dict[sid] = vrid
        sourceId_package_dict[sid] = pkg.split(",")
        pkg_list.extend(pkg.split(","))
    pkg_list = list(set(pkg_list))
    old_sid_cnt = len(sourceId_vrid_dict)
    old_vrid_cnt = vrid
    old_pkg_cnt = len(pkg_list)
    print("load", old_sid_cnt, "source ids.")
    print("load", old_vrid_cnt, "vrids")
    print("load", old_pkg_cnt, "packages")


def save_vrid(vrid_file):
    output_f = os.path.join(cur_dir, vrid_file)
    fout = open(output_f, "w")
    i = 0
    vrid = 0
    pkg_list_all = []
    for sid in sourceId_vrid_dict:
        vrid = sourceId_vrid_dict[sid]
        pkg_list = sourceId_package_dict[sid]
        pkg_list = list(set(pkg_list))
        pkg_list_all.extend(pkg_list)
        st = "{0} {1} {2}".format(sid, vrid, ",".join(pkg_list))
        fout.write(st + "\n")
        i += 1

    pkg_list_all = list(set(pkg_list_all))
    new_sid_cnt = len(sourceId_vrid_dict) - old_sid_cnt
    new_vrid_cnt = vrid - old_vrid_cnt
    new_pkg_cnt = len(pkg_list_all) - old_pkg_cnt
    print("save", new_sid_cnt, "new source ids.")
    print("save", new_vrid_cnt, "new vrids")
    print("save", new_pkg_cnt, "new packages")
    fout.close()


vrid_file = os.path.join(cur_dir, "vrid.dat")
load_vrid(vrid_file)
input_f = os.path.join(cur_dir, "simMatPac.txt")
source_id_list = []
sim_vec_list = []
package = ""
for line in open(input_f):
    line = line.strip()
    arr = line.split()
    h = arr[0]
    sim_vec = []
    if h.isdigit() == False:  # new package
        vrid_gen(package, source_id_list, sim_vec_list)
        package = h.lower()
        source_id_list = []
        sim_vec_list = []
    else:
        source_id = arr[0]
        source_id_list.append(source_id)
        if len(arr) >= 2:
            s = arr[1].rstrip(",").split(",")
            sim_vec = [float(d) for d in s]
        sim_vec_list.append(sim_vec)

save_vrid(vrid_file)
