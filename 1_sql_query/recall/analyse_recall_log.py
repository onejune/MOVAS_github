import os, sys

for line in sys.stdin:
    line = line.strip()
    arr = line.split('\t')
    req = arr[1]
    exp = arr[-1]
    arr = line.split(',')
    idx = 0
    for ele in arr:
        brr = ele.split(':')
        if len(brr) < 6:
            continue
        cam = brr[0]
        pivr = float(brr[1])
        idx += 1
        print req, idx, cam, pivr, exp
