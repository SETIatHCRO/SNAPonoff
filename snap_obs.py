#!/usr/bin/python

'''
 Name: snap_obs.py
 Author: Jon Richards, SETI Instiotute
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
import argparse
import logging
import ata_control
import snap_array_helpers
import snap_onoffs_contants
import snap_obs_selector

#SNAP ant connections. SNAP2 not working yet
snaps = {
            "snap0" : ['2a','2b','2e','3l','1f','5c','4l','4g'],
            "snap1" : ['2j','2d','4k','1d','2f','5h','3j','3e']
            #"snap2" : "1ax,1bx,1gx,1hx,2kx,2mx,3dx,4jx,1ay,1by,1gy,1hy,2ky,2my,3dy,4jy"
        }
defaultAntArg = snap_array_helpers.dict_values_to_string(snaps, True)
defaultSNAPHostnames = snap_array_helpers.dict_keys_to_array(snaps, True)
hostnamesHelpString = "Hostnames / IP of SNAPs, eg: %s,%s (no quotes necessary), or just one like %s" %  \
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
parser.add_argument('fpgfile', type=str,
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
freqs = snap_array_helpers.string_to_numeric_array(args.freqs);
logger.info("Frequency list is: %s" % str(freqs))
hosts = snap_array_helpers.string_to_array(args.hosts)
logger.info("SNAP host list is: %s" % str(hosts))
ant_list_string = snap_array_helpers.array_to_string(ant_groups)
logger.info("Full ant list is: %s" % ant_list_string )
offs = snap_array_helpers.string_to_numeric_array(args.off)
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
logger.info("Reserving antennas %s in bfa antgroup" % ant_list_string)
ata_control.reserve_antennas(ants)

# Set the PAMs
logger.info("Setting antenna attenuators to 15dB")
for ant in ants:
    # with no pol, both x and y will be set. Faster
   ata_control.set_pam_atten(ant, "", 15)

#test the rf switch
#ata_control.rf_switch_thread(ant_list_for_attenuators, True);

#test the release
ata_control.release_antennas(ants, True)

#exit here is testing
sys.exit()

while(0):

    pointing = snap_obs_selector.get_next(snap_list, source_list, ant_list, freq_list)


try:
    for pointing in pointings:
        pointing_spl = pointing.split('_')
        if len(pointing_spl) == 1:
            source = pointing
            az_offset = 0
            el_offset = 0
        else:
            source = pointing_spl[0]
            az_offset = pointing_spl[1]
            el_offset = pointing_spl[2]
        logger.info("Requested pointing is source: %s, az_offset: %.1f, el_offset: %.1f" % (source, az_offset, el_offset))
        for freq in freqs:
            logger.info("Requested tuning is %.2f" % freq)
            ata_control.write_obs_to_db(source, freq, az_offset, el_offset, ants)
            obsid = ata_control.get_latest_obs()
            logger.info("Obs ID is %d" % obsid)
            for antn, ant in enumerate(ants):
                for repetition in range(args.repetitions):
                    if args.off is not None:
                        off_az_off, off_el_off = map(float, args.off.split('_'))
                        for onoff in ["on", "off"]:
                            logger.info("Capturing data for antenna %s, %s iteration %d" % (ant, onoff, repetition))
                            if onoff == "on":
                                ata_control.point(source, freq)
                            elif onoff == "off":
                                ata_control.point(source, freq, off_az_off, off_el_off)

                            ata_control.set_rf_switch(0, anum)
                            ata_control.set_rf_switch(1, anum)
                            #ata_control.set_rf_switch(0, antn)
                            #ata_control.set_rf_switch(1, antn)
                            proc = Popen(["python", "/usr/local/bin/snap_take_spec_data.py", args.host, args.fpgfile, "-n", "%d" % args.ncaptures, "-a", ant, "-c", "%s_%s_%03d_ant_%s_%.2f_obsid%d" % (source, onoff, repetition, ant, freq, obsid)])
                            proc.wait()
                    else:
                        onoff = "on"
                        logger.info("Capturing data for antenna %s, %s iteration %d" % (ant, onoff, repetition))
                        ata_control.point(source, freq)
                        ata_control.rf_switch_ant(ant, 'x')
                        ata_control.rf_switch_ant(ant, 'y')
                        proc = Popen(["python", "/usr/local/bin/snap_take_spec_data.py", args.host, args.fpgfile, "-n", "%d" % args.ncaptures, "-a", ant, "-c", "%s_%s_%03d_ant_%s_%.2f_obsid%d" % (source, onoff, repetition, ant, freq, obsid)])
                        proc.wait()
            ata_control.end_obs()
except KeyboardInterrupt:
    exit()

