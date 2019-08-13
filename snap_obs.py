#!/usr/bin/python

'''
 Name: snap_obs.py
 Author: Jon Richards, SETI Institute
 Date: August/Sept 2018
 Note: This program was originally called snap_observation_run.py written by 
    Jack Hicish. It has been modified by Jon Richards to handle multiple RF 
    switches to work in unison. 

    As of August 25 the rfswitch and atten programs have been changed.
    Here is the new syntax:
    
    rfswitch <ant|antPol>
        Swiched the appropriate rfswitch to the specified antenn pols or 
        pols. If you specify an ant pol, only that one switch will select the
        antenna pol. For instance, "rfswitch 2a" will switch both the x an y
        rfswitches to 2a's x and y pol inputs. "rfswitch 2ax" will only
        switch the x pol. THis is useful when using x and y pols hooked to the
        same 16x1 rf switch.

    atten <db>  <ant|antPol>
        Sets the attenuation for and antenna (does pol pols) or just one
        pol.
'''
import os
import sys
#from ata_snap import ata_control
from subprocess import Popen, PIPE
import time
import datetime as dt
import argparse
import logging
import ata_control
import snap_array_helpers
import snap_onoffs_contants
import snap_obs_selector
import snap_obs_db
import snap_control
import snap_cli
from snap_redis import RedisManager

# Python code to remove duplicate elements 
def remove_dups(duplicate): 
    final_list = [] 
    for num in duplicate: 
        if num not in final_list: 
            final_list.append(num) 
    return final_list 

#Publish we are initializonf
RedisManager.get_instance().set_and_pub('onoff_state', { 'state' : 'init' }, 'onoff_state')
RedisManager.get_instance().set_and_pub('onoff_position', { 'position' : 'parked' }, 'onoff_position')
RedisManager.get_instance().set_and_pub('onoff_params', { 'ants' : [], 'freq' : -1.0, 'source' : '' }, 'onoff_params')

default_fpga_file = "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-06-23_1048.fpg"

#SNAP ant connections. 5b removed for now, put into maint
#SNAP ant connections. 1f removed for now, put into maint
"""
snaps = {
            "snap0" : ['2a','2b','2e','3l','1f','5c','4l','4g','2a','2b','2e','3l','1f','5c','4l'],
            "snap1" : ['2j','2d','4k','1d','2f','5h','3j','3e','2j','2d','4k','1d','2f','5h','3j'],
            "snap2" : ['1a','1b','1g','1h','2k','2m','3d','4j','5e','2c','4e','2l','2h','5g','1a']
        }
"""
# Oct 5, snap1, y pols rfswitch started failing. Need to ignore the obs for these. Till 
# USB reset at Tue Oct  9 04:44:02 UTC 2018 . Problem started at 2018-10-05 03:53:01
snaps = {
            "snap0" : ['2a','2b','2e','3l','5c','4l','4g'],
            "snap1" : ['2j','2d','4k','1d','2f','5h','3j','3e'],
            "snap2" : ['1a','1b','1g','1h','2k','2m','3d','4j','5e','2c','4e','2l','2h','5g']
        }
# removed 2h,2m,3d for repair
# 4e in in maint
# Dec 22, 2018 - 1f added back in.
# Dec 22, 2018 - 5g is having pointing problems. Removing from snap2.
# Dec 22, 2018 - 5h is having pointing problems. Removing from snap1.
# Jan 12, 2019 - 4k not positioning, removing from list, snap1
# Jan 23, 2019 - 1g keeps going down and up. I put it in maint. Removing from snap2
# Feb 10, 2019 - Added back in 1g 4k 5b 5g 5h
snaps = {
            "snap0" : ['2a','2b','2e','3l','5c','4l','4g','1f'],
            "snap1" : ['2j','2d','1d','2f','3j','3e','4k','5h'],
            "snap2" : ['1a','1b','1h','2k','4j','5e','2c','2l','1g','5g','5b']
        }
snaps = {
            "snap0" : ['2a','2b'],
            "snap1" : ['2j','3e'],
            "snap2" : ['1a','4j']
        }
# New feeds minus 1k, plus '2d','1d','2f' to even out snap1
# 5b still bad!
snaps = {
            "snap0" : ['2a','2b','2e','3l'],
            "snap1" : ['2j','2d','1d','2f'],
            "snap2" : ['1h','4j','1g']
        }
snaps = {
            "snap0" : ['2a','2b','2e','3l','5c','4l','4g','1f'],
            "snap1" : ['2j','2d','1d','2f','3j','3e','4k','5h'],
            "snap2" : ['1a','1b','1h','2k','4j','5e','2c','2l','1g','5g']
        }

#Removed 4j
# 5c removed,  will run atapointer on it independently. From snap0
snaps = {
            "snap0" : ['2a','2b','2e','3l','4l','4g','1f'],
            "snap1" : ['2j','2d','1d','2f','3j','3e','4k','5h'],
            "snap2" : ['1a','1b','1h','2k','5e','2c','2l','1g','5g','4j']
        }
#snaps = {
#            "snap0" : ['2e'],
#            "snap1" : ['2j'],
#            "snap2" : ['4j']
#        }

#Nov 18, 2018 - By request of Jack and Andrew, this would be the best, but then decided to to switch
# around all the pols
#snaps = {
#        snap0 : ['1b', '2b', '2k', '5e', '2c', '2l'],
#        snap1 : ['1h', '1g', '4e', '1d', '2f', '5g'],
#        snap2 : ['1a', '2a', '2d', '2j', '2e', '3l', '4g', '4j', '1f', '5c', '4k', '4l', '2f', '3e', '3j', '5h']
#        }
snaps = {
            "snap0" : ['2a','2b'],
            "snap1" : ['2j','3e'],
            "snap2" : ['1c','2h']
        }

defaultAntArg = (str(snaps["snap0"]) + "," + str(snaps["snap1"]) + "," + str(snaps["snap2"])).replace(" ","").replace("'","")
defaultSNAPHostnames = "snap0,snap1,snap2"
hostnamesHelpString = "List of SNAP hostnames: %s (no quotes necessary)" %  \
    (defaultSNAPHostnames)

# Init the logger
#logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
#logger.setLevel(logging.INFO)
#sh = logging.StreamHandler(sys.stdout)
#fmt = logging.Formatter('[%(asctime)-15s] %(message)s')
#sh.setFormatter(fmt)
#logger.addHandler(sh)
logger = ata_control.setup_logger()

# Define the argumants
parser = argparse.ArgumentParser(description='Run an observation with multiple antennas and pointings',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('hosts', type=str, help = hostnamesHelpString)
parser.add_argument('-fp', dest='fpga_file', type=str, default=default_fpga_file,
                    help = '.fpgfile to program')
#parser.add_argument('-s', dest='srate', type=float, default=900.0,
#                    help ='Sample rate in MHz for non-interleaved band. Used for spectrum axis scales')
parser.add_argument('-n', dest='ncaptures', type=int, default=16,
                    help ='Number of data captures (for each correlation product)')
parser.add_argument('-r', dest='repetitions', type=int, default=3,
                    help ='Number of repetitions of on-off pairs')
parser.add_argument('-a', dest='ants', type=str, default=defaultAntArg,
                    help ='Comma separated array list of ATA antennas, eg: \"[2j,2d,4k],[2a,2b,2e]\"')
parser.add_argument('-p', dest='pointings', type=str, default=None,
                    help ='Comma separated list of pointings, eg: \"casa,vira,moon\"')
parser.add_argument('-o', dest='off', type=str, default="0.0,10",
                    help ='Specify the off az,el in degrees, eg: 10,0')
parser.add_argument('-f', dest='freqs', type=str, default=None,
                    help ='Comma separated list of sky tuning frequencies, in MHz. Only one set of frequencies, eg: \"2000,3000,4000\"')

args = parser.parse_args()

logger.info( "")
logger.info( "##################################")
logger.info( "%s started!" % (str(__file__)))
logger.info( "##################################")
logger.info( "")

logger.info("### Started %s" % str(__file__))
email_string = "### Started %s\n" % str(__file__)
logger.info("Parameters:")
email_string += "Parameters:\n"

ant_groups = snap_array_helpers.string_to_array(args.ants);
logger.info("Ant groups are: %s, len=%d" % (str(ant_groups), len(ant_groups)))
email_string += "Ant groups are: %s, len=%d\n" % (str(ant_groups), len(ant_groups))
pointings = snap_array_helpers.string_to_array(args.pointings);
logger.info("Sources are: %s" % str(pointings))
email_string += "Sources are: %s\n" % str(pointings)
freq_list = snap_array_helpers.string_to_numeric_array(args.freqs);
logger.info("Frequency list is: %s" % str(freq_list))
email_string += "Frequency list is: %s\n" % str(freq_list)
snap_list = snap_array_helpers.string_to_array(args.hosts)
logger.info("SNAP host list is: %s" % str(snap_list))
email_string += "SNAP host list is: %s\n" % str(snap_list)
full_ant_list_string = snap_array_helpers.array_to_string(ant_groups)
full_ant_list = remove_dups(full_ant_list_string.split(","))
full_ant_list_string = snap_array_helpers.array_to_string(full_ant_list)

logger.info("Full ant list is: %s" % full_ant_list_string )
email_string += "Full ant list is: %s\n" % full_ant_list_string
offs = snap_array_helpers.string_to_numeric_array(args.off)
az_offset = offs[0]
el_offset = offs[1]
logger.info("Off position: Az=%3.2f, El=%2.2f" % (offs[0], offs[1]))
email_string += "Off position: Az=%3.2f, El=%2.2f\n" % (offs[0], offs[1])
logger.info("Repetitions = %d" % args.repetitions)
email_string += "Repetitions = %d\n" % args.repetitions
logger.info("Captures = %d" % args.ncaptures)
email_string += "Captures = %d\n" % args.ncaptures

# Create the list of antenna, merging all antenna groups
#ants = snap_array_helpers.flatten(ant_groups)
#ant_list_temp = remove_dups(ants)
#ants = snap_array_helpers.array_to_string(ant_list_temp)
logger.info("Ant list: %s" % snap_array_helpers.array_to_string(full_ant_list))
email_string += "Ant list: %s\n" % snap_array_helpers.array_to_string(full_ant_list)

ata_control.send_email("SNAP Obs started", email_string)
# For each SNAP. set the minicircuits attenuators to 12.0
# To do this, get a list of the first antenna in each snap group
default_atten_db = 12 # Suggested by jack
for a in ant_groups:
    ata_control.set_atten_thread(["%sx"%a[0],"%sy"%a[0]], [default_atten_db, default_atten_db], False)

# Reserve the antennas
logger.info("Reserving antennas %s in bfa antgroup" % full_ant_list_string)
ata_control.reserve_antennas(full_ant_list)

# Set the PAMs
logger.info("Setting antenna attenuators to 15dB")
for ant in full_ant_list:
   logger.info("Setting PAM for ant %s to %d" % (ant, 15))
    # with no pol, both x and y will be set. Faster
   ata_control.set_pam_atten(ant, "", 15)

#test the rf switch
#ata_control.rf_switch_thread(ant_list_for_attenuators, True);

#test the release
#ata_control.release_antennas(full_ant_list, True)

#exit here in testing
#sys.exit()

current_source = ""
current_obsid = -1
current_freq = 0.0
obsid = -1

snap_cli.set_state(snap_cli.PROGRAM_STATE_RUN)
snap_cli.server_thread()

RedisManager.get_instance().set_and_pub('onoff_antlist', { 'ants' : snaps }, 'onoff_antlist')

try:
    while(1):

        if(snap_cli.get_state() == snap_cli.PROGRAM_STATE_QUIT):
            logger.info("QUIT")
            break
        elif(snap_cli.get_state() == snap_cli.PROGRAM_STATE_PAUSE):
            logger.info("PAUSING")
            while(snap_cli.get_state() == snap_cli.PROGRAM_STATE_PAUSE):
                time.sleep(1)
            if(snap_cli.get_state() == snap_cli.PROGRAM_STATE_QUIT):
                logger.info("QUIT")
                break
            else:
                logger.info("Back up and runnung")

        # Get the next source, freq and ants to observe.
        # get_next will return like:
        #   {'source': 'casa', 'freq': 1000.0, 'ants': ['2b', '2j']}
        # or if not up:
        #   { "status" : "none_up", "next_up" : "casa", "minutes" : 200 }
        # or if there are none up, all are below the horizon, sun and moon angle: None
        print snap_list
        print pointings
        print ant_groups
        print freq_list
        obs_params = snap_obs_selector.get_next(snap_list, pointings, ant_groups, freq_list, dt.datetime.now())

        logger.info(obs_params)
        if(obs_params == None or obs_params['status'] == "none_up"):
            if(obs_params == None):
                logger.info("Attempted to get next up, None was returned. Waiting and trying again...")
            else:
                logger.info( "No sources up yet, next is %s in %d minutes" % (obs_params['next_up'], obs_params['minutes']))
                RedisManager.get_instance().set_and_pub('onoff_state', { 'state' : 'waiting_for_source_up', 'source' : obs_params['next_up'], 'minutes' : obs_params['minutes'] }, 'onoff_state')

            secs_to_wait = 60*60 # wait 1 hour max
            if(secs_to_wait > obs_params['minutes']*60): 
                secs_to_wait = obs_params['minutes']*60 + 70
            ata_control.send_email("SNAP obs: no source up", "No sources up yet, next is %s in %d minutes" % (obs_params['next_up'], obs_params['minutes']))
            while(secs_to_wait > 0):
                time.sleep(1)
                secs_to_wait -= 1
            continue
        else:
            logger.info("get_next returned: %s" % obs_params)

        # Record in the database a new obsid
        # Only if the source changed or the frequency changed
        if(current_source != obs_params['source'] or current_freq != obs_params['freq']):

            this_ant_string = snap_array_helpers.array_to_string(obs_params['ants'])
            full_ant_string = snap_array_helpers.array_to_string(full_ant_list)
            print this_ant_string
            #status = snap_obs_db.start_new_obs(this_ant_string, obs_params['freq'], obs_params['source'], az_offset, el_offset)
            status = snap_obs_db.start_new_obs(full_ant_list_string, obs_params['freq'], obs_params['source'], az_offset, el_offset)
            ata_control.set_output_dir()

            obsid = -1
            if "obsid" in status:
                obsid = status["obsid"]
                current_freq = obs_params['freq']
                # Set the freq and fcus tha ants
                logger.info(ata_control.set_freq(current_freq, this_ant_string))

                ata_control.send_email("SNAP Obs status, new source/freq", "Source %s, freq %.2f" % (obs_params['source'], float(obs_params['freq'])))

            else:
                logger.info("ERROR: Tried to create and record in db, got error. Tring again after a 10 seconds wait...")
                time.sleep(10)
                continue

        # get the source name
        source = obs_params['source']
        freq = obs_params['freq']

        # Create the ephemers files ahead of time for this source and on,off pointing
        if(current_source != source): 
            logger.info("Create ephems: %s" % ata_control.create_ephems(source, offs[0], offs[1]));
            RedisManager.get_instance().set_and_pub('onoff_moving', { 'state' : 'moving' }, 'onoff_state')
            logger.info("Move all ants on target: %s" % ata_control.point_ants("on", full_ant_list_string ));
            current_source = source

        ants_to_observe = str(obs_params['ants']).replace("'", "").replace("[", "").replace("]", "").replace(" ","")

        print "ANTS_TO_OBSERVE=%s, snaps = %s" % (ants_to_observe, args.hosts)

        #RedisManager.get_instance().set_and_pub('onoff_params', { 'ants' : obs_params['ants'], 'freq' : float(freq), 'source' : source }, 'onoff_params')
        RedisManager.get_instance().set_and_pub('onoff_state', { 'state' : 'observing' }, 'onoff_state')

        snap_control.do_onoff_obs(args.hosts, \
                "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-06-23_1048.fpg", \
                source, args.ncaptures, args.repetitions, ants_to_observe, freq, obsid, 0.0, 10.0)

        snap_obs_db.end_most_recent_obs()


except KeyboardInterrupt:
    snap_obs_db.end_most_recent_obs()
    snap_cli.server_close()

RedisManager.get_instance().set_and_pub('onoff_state', { 'state' : 'idle' }, 'onoff_state')
RedisManager.get_instance().set_and_pub('onoff_position', { 'position' : 'parked' }, 'onoff_position')
logger.info("Releasing ants")
ata_control.release_antennas(full_ant_list, True)
logger.info("Closing cli server")
snap_cli.server_close()
logger.info("Shut down - done.")

exit()


