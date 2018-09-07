#!/usr/bin/python

# snap_control.py
# Module containing methods to control the SNAP boards
# Author: Jon Richards, SETI Institute
#         Sep 05, 2018

import sys
import numpy as np, scipy.io
from plumbum import local
import math
import os
import datetime as dt
import snap_onoffs_contants
import snap_array_helpers
import snap_obs_db
import ata_control
import math
import logging
import MySQLdb
import MySQLdb.cursors
import logging
from threading import Thread
from subprocess import Popen, PIPE

RECORDER_PATH = "/home/sonata/jr/SNAPonoff/snap_record.py"

def do_onoff_obs_threaded(snap, fpga_file, on_or_off, num_captures, ant, source, rep, obsid, freq):

    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
    logger.info("Starting %s %s obs for %s, freq: %.2f, %d rep, obsid: %d, source: %s, caps: %d, rep=%d" % \
                (snap, on_or_off, ant, freq, rep, obsid, source, num_captures, rep))
    proc = Popen(["python", RECORDER_PATH,\
                 snap, fpga_file, "-n", \
                 "%d" % num_captures, "-a", ant, "-c", \
                 "%s_%s_%03d_ant_%s_%.2f_obsid%d" % \
                 (source, on_or_off, rep, ant, freq, obsid)])

    proc.wait()
    logger.info("Fnished %s obs for %s, freq: %.2f, %d rep, obsid: %d, source: %s" % \
                (on_or_off, ant, freq, rep, obsid, source))

    logger.info(snap_obs_db.record_on_off_obs(snap, ant, source, freq, on_or_off, rep))

    # snaps and ants are csv strings
def do_onoff_obs(snaps, fpga_file, source, num_captures, repetitions, ants, freq, obsid, az_offset, el_offset):

    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)

    logger.info("do_onoff_obs(%s, %s, %s, %d, %d, %s, %.2f, %d, %.2f, %.2f)" % \
            (snaps, fpga_file, source, num_captures, repetitions, ants, freq, obsid, az_offset, el_offset))

    # set the RF switches
    ata_control.rf_switch_thread(ants, True)

    snap_list = snap_array_helpers.string_to_array(snaps)
    ant_list = snap_array_helpers.string_to_array(ants)

    for rep in range(repetitions):

        for on_or_off in ["on", "off"]:

            print "##########"
            print "%s - %s" % (on_or_off.upper(), ants)
            print "##########"

            logger.info("Performing %s for %s" % (on_or_off.upper(), ants))

            # Position the ants
            logger.info("Move all ants %s target: %s" % (on_or_off.upper(), ata_control.point_ants(on_or_off, ants ))); 

            threads = []

            for idx, ant in enumerate(ant_list):

                snap = snap_list[idx]

                t = Thread(target=do_onoff_obs_threaded, \
                        args=(snap, fpga_file, \
                                on_or_off, num_captures, ant, source, \
                                rep, obsid, freq))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

        logger.info("Finished %s for %s" % (on_or_off, ants))


if __name__== "__main__":

    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter('[%(asctime)-15s] %(message)s')
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    ants = snap_array_helpers.array_to_string(['2a', '2j'])
    snaps = snap_array_helpers.array_to_string(['snap0', 'snap1'])

    logger.info(ata_control.create_ephems("moon", 0.0, 5.0))
    logger.info(ata_control.point_ants("on", ants))

    print "Start test %s" % ants
    print snap_obs_db.start_new_obs(ants, 1000.0, "moon", 0.0, 10.0)
    obsid = snap_obs_db.get_most_recent_obsid()
    do_onoff_obs(snaps, \
            "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-06-23_1048.fpg", \
            "moon", 16, 2, ants, 1000.0, obsid, 0.0, 10.0)

