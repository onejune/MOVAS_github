import os, sys

for line in sys.stdin:
    arr = line.strip().split('\t')
    arr[0] = arr[0].lower()
    for i in range(len(arr)):
        if arr[i] == r'""':
            arr[i] = '0'
    print '\t'.join(arr)

