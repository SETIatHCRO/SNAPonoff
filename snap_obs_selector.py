#!/usr/bin/python

##
# ATAPOsitions class
# Calculated the Az/Elof various objects in the sky, including ra/dec.
# Note that the calculated positions are not exact, but within a degree.
# Author: Jon Richards, SETI Institute
# July 16, 2018
##

import sys
import numpy as np, scipy.io
import math
import os
import datetime as dt
import snap_onoffs_contants
import snap_array_helpers
import math
import logging
import ata_positions
import snap_obs_db

def pick_next_freq_ant_for_snap(snap, ant_list, freq_list):

    #which fequency should we use?
    #Get the frequency of the last antpol observed for this source
    # returns something like:
    #   {'status': 'OK', 'source': ['casa'], 
    #    'ts': [datetime.datetime(2018, 9, 4, 23, 31, 7)], 
    #    'ant': ['1a'], 'obsid': [725L], 'snap': ['snap1'], 
    #    'freq': [1000.0], 'id': [1L]}
    # or if there is no result:
    #   {'status': 'NONE'}
    onoff_obs = snap_obs_db.get_latest_onoff_obs(snap)
    if(onoff_obs['status'] == "NONE"):
        return { "ant" : ant_list[0], "freq" : freq_list[0] }

    # Get the index of the last ant used
    prev_ant = onoff_obs['ant'][0]
    prev_freq = onoff_obs['freq'][0]
    prev_ant_index = -1
    prev_freq_index = -1

    try:
        prev_freq_index = freq_list.index(prev_freq)
    except TypeError as e:
        return { "status" : "error", "details" : "last freq used %.2f is not in the freq list, default to first freq and ant" % prev_freq, \
                 "ant" : ant_list[0], "freq" : freq_list[0] }

    try:
        prev_ant_index = ant_list.index(prev_ant)
    except TypeError as e:
        return { "status" : "error", "details" : "last ant used %s is not in the ant list, default to first freq and ant" % prev_ant,\
                 "ant" : ant_list[0], "freq" : freq_list[0]}

    next_freq = freq_list[prev_freq_index]
    next_ant = ant_list[prev_ant_index]
    if(prev_ant_index == (len(ant_list) - 1)): # past the end
        next_ant = ant_list[0]
        next_freq_index = prev_freq_index + 1
        if(next_freq_index >= len(freq_list)): # past the end
            next_freq = freq_list[0]
        else: 
            next_freq = freq_list[next_freq_index]
    else:
        next_ant = ant_list[prev_ant_index + 1]
        next_freq = freq_list[prev_freq_index]; # Stays the same

    return { "ant" : next_ant, "freq" : next_freq }



# Get the next source and associated parameters to look at
def get_next(snap_list, source_list, ant_list, freq_list):

    # First, determine if there is a source up. Go through the source_list
    # in prder and get the first one that is up
    best_source_up = ata_positions.ATAPositions.getFirstInListThatIsUp(source_list)
    if(best_source_up['status'] == 'next_up'):
        return { "status" : "none_up", "next_up" : best_source_up['source'], \
                    "minutes" : best_source_up['minutes'] }

    source = best_source_up['source'];
    print "Best source = %s" % source

    result = {}

    selected_source = source;
    selected_freq = 0.0
    selected_ants = []

    for snap_index,snap in enumerate(snap_list): # enumerate() gives us the index


        next_info = pick_next_freq_ant_for_snap(snap, ant_list[snap_index], freq_list)

        # If this is the first snap, this determines the frequency to use
        if(snap_index == 0):
            selected_freq = next_info['freq']

        selected_ants.append(next_info['ant'])

    return { "source" : source, "ants" : selected_ants, "freq" : selected_freq }


if __name__== "__main__":

    snap_list = ['snap1', 'snap2']
    source_list = ['casa', 'moon', 'taua', 'vira']
    #ant_list = [['2a','2b','2e','3l','1f','5c','4l','4g'],['2j','2d','4k','1d','2f','5h','3j','3e']]
    ant_list = [['1a','2b','2e','3l','1f','5c','4l','4g'],['2j','2d','4k','1d','2f','5h','3j','3e']]
    freq_list = [1000.0, 2000.0]

    print get_next(snap_list, source_list, ant_list, freq_list)
    for i,snap in enumerate(snap_list):
        print pick_next_freq_ant_for_snap(snap, ant_list[i], freq_list)








