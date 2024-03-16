#coding=utf-8
import os, sys, json, random, re, time, datetime
import math, itertools
from ctypes import cdll
import ctypes

BKDRHash=cdll.LoadLibrary('./bkdrEncode.so').BKDRHash
BKDRHash.restype = ctypes.c_char_p
excluded_features = {'allow_playable', 'ext_playable', 'material_106', 'resolution_106', 'detect_type', 'jump_count', }

def get_combine_schema():
    global combine_schema
    combine_schema = []
    combine_schema_file, output_file = sys.argv[1:3]
    with open(combine_schema_file, 'r') as fin, open(output_file, 'w') as fout:
        for line in fin:
            name = line.strip('\n')
            if len(name.split('#')) > 1 and ('his_ins' not in name and 'his_clk' not in name and 'his_imp' not in name):
                continue
            if name in excluded_features:
                continue
            fout.write(name + '\n')
 
if __name__ == "__main__":
    global column_name, combine_schema
    get_combine_schema()

