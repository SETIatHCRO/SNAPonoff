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
import argparse
import logging
import ata_control
import snap_array_helpers
import snap_onoffs_contants
import snap_obs_selector
import snap_obs_db
import snap_control

default_fpga_file = "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-06-23_1048.fpg"

#SNAP ant connections. SNAP2 not working yet
snaps = {
            "snap0" : ['2a','2b','2e','3l','1f','5c','4l','4g'],
            "snap1" : ['2j','2d','4k','1d','2f','5h','3j','3e']
            #"snap2" : "one-ax,1bx,1gx,1hx,2kx,2mx,3dx,4jx,1ay,1by,1gy,1hy,2ky,2my,3dy,4jy"
        }
defaultAntArg = (str(snaps["snap0"]) + "," + str(snaps["snap1"])).replace(" ","").replace("'","")
defaultSNAPHostnames = "snap0,snap1"
hostnamesHelpString = "List of SNAP hostnames: %s,%s (no quotes necessary), or just one like %s" %  \
    (defaultSNAPHostnames[0], defaultSNAPHostnames[1], defaultSNAPHostnames[0])

# Init the logger
logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler(sys.stdout)
fmt = logging.Formatter('[%(asctime)-15s] %(message)s')
sh.setFormatter(fmt)
logger.addHandler(sh)

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

print ""
print "##################################"
print "%s started!" % (str(__file__))
print "##################################"
print ""

logger.info("### Started %s" % str(__file__))
logger.info("Parameters:")

ant_groups = snap_array_helpers.string_to_array(args.ants);
logger.info("Ant groups are: %s, len=%d" % (str(ant_groups), len(ant_groups)))
pointings = snap_array_helpers.string_to_array(args.pointings);
logger.info("Sources are: %s" % str(pointings))
freq_list = snap_array_helpers.string_to_numeric_array(args.freqs);
logger.info("Frequency list is: %s" % str(freq_list))
snap_list = snap_array_helpers.string_to_array(args.hosts)
logger.info("SNAP host list is: %s" % str(snap_list))
full_ant_list_string = snap_array_helpers.array_to_string(ant_groups)
logger.info("Full ant list is: %s" % full_ant_list_string )
offs = snap_array_helpers.string_to_numeric_array(args.off)
az_offset = offs[0]
el_offset = offs[1]
logger.info("Off position: Az=%3.2f, El=%2.2f" % (offs[0], offs[1]))

# Create the list of antenna, merging all antenna groups
ants = snap_array_helpers.flatten(ant_groups)
logger.info("Ant list: %s" % snap_array_helpers.array_to_string(ants))

# For each SNAP. set the minicircuits attenuators to 0.0
# To do this, get a list of the first antenna in each snap group, x and y pol
ant_list_for_attenuators = []
db_list = []
for name, snap_antlist in snaps.items():
    db_list.append(0.0)
    ant = snap_antlist[0];
    if(len(ant) == 2):
        ant_list_for_attenuators.append(ant + 'x')
        ant_list_for_attenuators.append(ant + 'y')
        db_list.append(0.0)
    else:
        ant_list_for_attenuators.append(ant)
ata_control.set_atten_thread(ant_list_for_attenuators, db_list, False)

# Reserve the antennas
logger.info("Reserving antennas %s in bfa antgroup" % full_ant_list_string)
ata_control.reserve_antennas(ants)

# Set the PAMs
logger.info("Setting antenna attenuators to 15dB")
for ant in ants:
    # with no pol, both x and y will be set. Faster
   ata_control.set_pam_atten(ant, "", 15)

#test the rf switch
#ata_control.rf_switch_thread(ant_list_for_attenuators, True);

#test the release
#ata_control.release_antennas(ants, True)

#exit here in testing
#sys.exit()

current_source = ""
current_obsid = -1
current_freq = 0.0
obsid = -1

try:
    while(1):

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
        obs_params = snap_obs_selector.get_next(snap_list, pointings, ant_groups, freq_list)

        logger.info(obs_params)
        if(obs_params == None or obs_params['status'] == "none_up"):
            if(obs_params == None):
                logger.info("Attempted to get next up, None was returned. Waiting and trying again...")
            else:
                logger.info( "No sources up yet, next is %s in %d minutes" % (obs_params['next_up'], obs_params['minutes']))
            secs_to_wait = 10
            while(secs_to_wait > 0):
                time.sleep(1)
                secs_to_wait -= 1
            continue
        else:
            logger.info("get_next returned: %s" % obs_params)

        # Record in the database a new obsid
        # Only if the source changed or the frequency changed
        if(current_source != obs_params['source'] or current_freq != obs_params['freq']):

            this_ant_string = str(obs_params['ants']).replace("'", "").replace("]","").replace("[","").replace(" ","")
            print this_ant_string
            status = snap_obs_db.start_new_obs(this_ant_string, obs_params['freq'], obs_params['source'], az_offset, el_offset)
            obsid = -1
            if "obsid" in status:
                obsid = status["obsid"]
                current_freq = obs_params['freq']
            else:
                logger.info("ERROR: Tried to create and record in db, got error. Tring again after a 10 seconds wait...")
                time.sleep(10)
                continue

        # get the source name
        source = obs_params['source']

        # Create the ephemers files ahead of time for this source and on,off pointing
        if(current_source != source): 
            logger.info("Create ephems: %s" % ata_control.create_ephems(source, offs[0], offs[1]));
            logger.info("Move all ants on target: %s" % ata_control.point_ants("on", full_ant_list_string ));
            current_source = source

        ants_to_observe = str(obs_params['ants']).replace("'", "").replace("[", "").replace("]", "").replace(" ","")

        print "ANTS_TO_OBSERVE=%s, snaps = %s" % (ants_to_observe, args.hosts)

        snap_control.do_onoff_obs(args.hosts, \
                "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-06-23_1048.fpg", \
                source, 16, 2, ants_to_observe, 1000.0, obsid, 0.0, 10.0)

        snap_obs_db.end_most_recent_obs()


except KeyboardInterrupt:
    snap_obs_db.end_most_recent_obs()
    exit()




