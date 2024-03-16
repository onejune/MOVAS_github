#coding=utf-8
import os, sys, json, random, re, time, datetime
import math, itertools
from ctypes import cdll
import ctypes

BKDRHash=cdll.LoadLibrary('./bkdrEncode.so').BKDRHash
BKDRHash.restype = ctypes.c_char_p
# source_id_map = {}

def print_counter(group, counter, amount):
    print >> sys.stderr, 'reporter:counter:{g},{c},{a}'.format(g=group, c=counter, a=amount)

# def get_source_id():
#     final_source_id = sys.argv[3]
#     with open(final_source_id, 'r') as fin:
#         for line in fin:
#             try:
#                 adn_creative_id, adv_creative_id, source_id, creative_type, algo_tag, hour = line.strip('\n').split('\t')
#                 if source_id == 'none':
#                     source_id = adv_creative_id
#                     if source_id == '0':
#                         source_id = adn_creative_id
#                 else:
#                     source_id, algo_tag = map(int, [source_id, algo_tag])
#                     source_id = 1000*source_id + algo_tag
#                     source_id = str(source_id)
#                 source_id_map[adn_creative_id] = source_id
#             except:
#                 pass

def get_column_name():
    global column_name
    column_name = []
    column_name_file = sys.argv[1]
    with open(column_name_file, 'r') as fin:
        for line in fin:
            name = line.strip()
            if '@' in name:
                name = name.split('@')[1]
            column_name.append(name)

def get_combine_schema():
    global combine_schema
    combine_schema = []
    combine_schema_file = sys.argv[2]
    with open(combine_schema_file, 'r') as fin:
        for line in fin:
            name = line.strip('\n')
            if len(name.split('#')) > 1 and ('his_ins' not in name and 'his_clk' not in name and 'his_imp' not in name):
                continue
            combine_schema.append(name)

def run():
    global column_name, combine_schema
    fea_num = len(column_name)
    for line in sys.stdin:
        stamp = '%.30f' % random.random()
        info = line.strip('\n').strip().split('\002')
        if len(info) < fea_num:
            print_counter('merge_mapper_error', line.strip('\n').strip(), 1)
            continue
        all_fea = info[0:3]
        column_dic = {}
        for i in range(3, fea_num):
            column = column_name[i]
            values = info[i].replace(' ', '\003').replace(':', '\004').split('\001')
            column_dic[column] = values
        if column_dic['ad_type'][0] != 'interstitial_video' and column_dic['ad_type'][0] != 'rewarded_video':
            continue
        
        # # 新增 source_id: m201_algo_id, m61002_algo_id
        # creative_201 = column_dic.get('creative_201', ['none'])[0]
        # m201_algo_id = source_id_map.get(creative_201, creative_201)
        # column_dic['m201_algo_id'] = [m201_algo_id]
        # 
        # # creative_106 = column_dic.get('creative_106', ['none'])[0]
        # # m106_algo_id = source_id_map.get(creative_106, creative_106)
        # # column_dic['m106_algo_id'] = [m106_algo_id]

        # playable_id = column_dic.get('playable_id', ['none'])[0]
        # m61002_algo_id = source_id_map.get(playable_id, playable_id)
        # column_dic['m61002_algo_id'] = [m61002_algo_id]

        for combine in combine_schema:
            combine_product = []
            feature_names = combine.split('#')
            if len(feature_names) == 1: # 单特征
                schema_feature = []
                feature_values = column_dic.get(feature_names[0], [])
                if len(feature_values) == 0:
                    continue
                for feature_value in feature_values:
                    if feature_names[0] in {'template_group', 'video_template', 'endcard_template', 'minicard_template'} and feature_value == 'null':
                        feature_value = 'none'
                    schema_feature.append(feature_names[0] + '=' + feature_value + ':1')
                all_fea += schema_feature
                continue
            combine_feature = [] # 组合特征
            for feature_name in feature_names:
                feature_set = set()
                feature_values = column_dic.get(feature_name, [])
                feature_set = set(feature_values)
                combine_feature.append(feature_set)
            schema_feature = []
            if len(combine_feature) < 2:
                continue
            if len(combine_feature) == 2:
                combine_product = itertools.product(combine_feature[0], combine_feature[1])
            elif len(combine_feature) == 3:
                combine_product = itertools.product(combine_feature[0], combine_feature[1], combine_feature[2])
            elif len(combine_feature) == 4:
                combine_product = itertools.product(combine_feature[0], combine_feature[1], combine_feature[2], combine_feature[3])
            elif len(combine_feature) == 5:
                combine_product = itertools.product(combine_feature[0], combine_feature[1], combine_feature[2], combine_feature[3], combine_feature[4])
            elif len(combine_feature) == 6:
                combine_product = itertools.product(combine_feature[0], combine_feature[1], combine_feature[2], combine_feature[3], combine_feature[4], combine_feature[5])
            schema_feature = ['|'.join(feature_names) + '=' + BKDRHash('|'.join(x)) + ':1' for x in combine_product]
            if len(schema_feature) == 0:
                print_counter('schema_feature_error', 'len(schema_feature)==0', 1)
                continue
            all_fea += schema_feature
        print stamp + '\t' + ' '.join(all_fea) 
        
 
if __name__ == "__main__":
    global column_name, combine_schema
    get_column_name()
    get_combine_schema()
    # get_source_id()
    run()

