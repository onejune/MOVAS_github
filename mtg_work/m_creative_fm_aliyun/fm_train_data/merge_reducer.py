#coding=utf-8
import os, sys, json, random, re, time, datetime
import math

def print_counter(group, counter, amount):
    print >> sys.stderr, 'reporter:counter:{g},{c},{a}'.format(g=group, c=counter, a=amount)
 
if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip('\n').split('\t')
        print line[1]

    

