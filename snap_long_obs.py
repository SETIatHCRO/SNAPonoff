#!/usr/bin/python

import sys
import os

import time

if len(sys.argv) != 8:

    print("%s snap freq ant dest obsid source obsname" % sys.argv[0])
    sys.exit()

snap = sys.argv[1]
freq = sys.argv[2]
ant = sys.argv[3]
dest = sys.argv[4]
obsid = sys.argv[5]
source = sys.argv[6]
obsname = sys.argv[7]

fpga_file = "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-07-07_1844.fpg"

for i in range(1, 10000):
    t = ''
    if i == 1:
        t = '-t 12.0'
    cmd = "./snap_record.py %s %s -n 1000 -a %s -p %s %s -o 1000000 -c %s_%s_on_%s_obsid%s_%smhz_%d" % (snap, fpga_file, ant, dest, t, source, ant, obsname, obsid, freq, i)
    print(cmd)
    os.system(cmd)
