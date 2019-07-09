#!/usr/bin/python

##
# snap_obs_selector.py class
# Functionality to select best obs, frequency and ants to observe.
# Removes a lot of complexity out of the main observing loop.
# Author: Jon Richards, SETI Institute
# Sep 04, 2018
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
import ata_control
import snap_obs_db

def pick_next_freq_ant_for_snap(snap, ant_list, freq_list, source):

    #which fequency should we use?
    #Get the frequency of the last antpol observed for this source
    # returns something like:
    #   {'status': 'OK', 'source': ['casa'], 
    #    'ts': [datetime.datetime(2018, 9, 4, 23, 31, 7)], 
    #    'ant': ['2b'], 'obsid': [725L], 'snap': ['snap1'], 
    #    'freq': [1000.0], 'id': [1L]}
    # or if there is no result:
    #   {'status': 'NONE'}
    onoff_obs = snap_obs_db.get_latest_onoff_obs(snap, source)
    #print onoff_obs
    if(onoff_obs['status'] == "NONE"):
        return { "ant" : ant_list[0], "freq" : freq_list[0] }

    # Get the index of the last ant used
    prev_ant = onoff_obs['ant'][0]
    prev_freq = onoff_obs['freq'][0]
    prev_ant_index = -1
    prev_freq_index = -1

    #print("PREV FREQ=%f" % prev_freq)

    try:
        prev_freq_index = freq_list.index(prev_freq)
    except TypeError as e:
        return { "status" : "error", "details" : "last freq used %.2f is not in the freq list, default to first freq and ant" % prev_freq, \
                 "ant" : ant_list[0], "freq" : freq_list[0] }
    except ValueError as e:
        print("In DB freq %f not found", prev_freq)
    #    prev_freq_index = (len(freq_list) - 1);
    #    pass

    try:
        prev_ant_index = ant_list.index(prev_ant)
    except TypeError as e:
        return { "status" : "error", "details" : "last ant used %s is not in the ant list, default to first freq and ant" % prev_ant,\
                 "ant" : ant_list[0], "freq" : freq_list[0]}
    except ValueError as e:
        print "Prev ant %s is not in list, resetting index to 0"  % prev_ant
        prev_ant_index  = (len(ant_list) - 1)
        prev_freq_index = (len(freq_list) - 1);

    next_freq = freq_list[prev_freq_index]
    #print("F1=%f, %s" % (next_freq, str(freq_list)))
    next_ant = ant_list[prev_ant_index]
    if(prev_ant_index == (len(ant_list) - 1)): # past the end
        next_ant = ant_list[0]
        next_freq_index = prev_freq_index + 1
        if(next_freq_index >= len(freq_list)): # past the end
            next_freq = freq_list[0]
            #print("F2=%f" % next_freq)
        else: 
            next_freq = freq_list[next_freq_index]
            #print("F3=%f" % next_freq)
    else:
        next_ant = ant_list[prev_ant_index + 1]
        next_freq = freq_list[prev_freq_index]; # Stays the same
        #print("F4=%f" % next_freq)

    return { "ant" : next_ant, "freq" : next_freq }



# Get the next source and associated parameters to look at
def get_next(snap_list, source_list, ant_list, freq_list, d=None):

    if(d == None):
        d=dt.datetime.now()

    # First, determine if there is a source up. Go through the source_list
    # in prder and get the first one that is up
    best_source_up = ata_positions.ATAPositions.getFirstInListThatIsUp(source_list, d)
    print best_source_up
    if(best_source_up == None):
        return None
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


        next_info = pick_next_freq_ant_for_snap(snap, ant_list[snap_index], freq_list, source)

        index_snap_biggest_antlist = -1

        # If this is the first snap, this determines the frequency to use
        if(snap_index == (len(snap_list)-1)):
            selected_freq = next_info['freq']

        selected_ants.append(next_info['ant'])

    return { "status" : "OK", "source" : source, "ants" : selected_ants, "freq" : selected_freq }


if __name__== "__main__":
    #snap_obs_db.set_test(False)

    snaps = {   
            "snap0" : ['2a','2b','2e','3l','1f','5c','4l','4g'],
            "snap1" : ['2j','2d','4k','1d','2f','5h','3j','3e'],
            "snap2" : ['1a','1b','1g','1h','2k','2m','3d','4j','5e','2c','4e','2l','2h','5g']
            }

    snap_list = ['snap0', 'snap1', 'snap2']
    source_list = [ 'moon', 'taua', 'casa', 'cyga', 'goes-16']
    ant_list = [snaps['snap0'], snaps['snap1'], snaps['snap2']]
    freq_list = [1400.0, 2500.0, 3500.0]


    d = dt.datetime.now() + dt.timedelta(minutes=(0))
    #for i,snap in enumerate(snap_list):
    #    print pick_next_freq_ant_for_snap(snap, ant_list[i], freq_list)
    current_source = ""
    current_freq = ""
    az_offset = 10.0
    el_offset = 0.0

    logger = ata_control.setup_logger("~/data/temp")
    full_ant_list = snaps['snap0'][:]
    full_ant_list.extend(snaps['snap1'])
    full_ant_list.extend(snaps['snap2'])
    full_ant_list_string = ",".join(full_ant_list)


    obs_params = get_next(snap_list, source_list, ant_list, freq_list)
    print obs_params
    '''

    if(current_source != obs_params['source'] or current_freq != obs_params['freq']):
        status = snap_obs_db.start_new_obs(full_ant_list_string, obs_params['freq'], obs_params['source'], az_offset, el_offset)
        print status
        current_source = obs_params['source']
        current_freq = obs_params['freq']

    ants_to_observe = ",".join(obs_params['ants'])

    print "ANTS_TO_OBSERVE=%s,%.1f,%s" % (ants_to_observe, float(current_freq), current_source)
    print "snaps = \n\t%s\n\t%s\n\t%s" % (snaps['snap0'], snaps['snap1'], snaps['snap2'])

    snap_obs_db.record_on_off_obs('snap0', obs_params['ants'][0], current_source, float(current_freq), "on", 0)
    snap_obs_db.record_on_off_obs('snap1', obs_params['ants'][1], current_source, float(current_freq), "on", 0)
    snap_obs_db.record_on_off_obs('snap2', obs_params['ants'][2], current_source, float(current_freq), "on", 0)


    '''














