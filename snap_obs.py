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
import logging
from optparse import OptionParser
import snap_control
#import snap_cli
from  ATATools import ata_control,logger_defaults,snap_array_helpers,obs_db,ata_positions
import ATAComm 
import onoff_db
from SNAPobs import snap_defaults

default_fpga_file = snap_defaults.spectra_snap_file
default_captures = 16
default_repetitions = 3
default_pointings = "0.0,10"

def remove_dups(duplicate): 
    final_list = list(set(duplicate))
    return final_list 

def main():

    # Define the argumants
    parser = OptionParser(usage= 'Usage %prog options',
            description='Run an observation with multiple antennas and pointings')

    #parser.add_argument('hosts', type=str, help = hostnamesHelpString)
    parser.add_option('--fpga', dest='fpga_file', type=str, action="store", default=default_fpga_file,
                    help = '.fpgfile to program')
    #parser.add_argument('-s', dest='srate', type=float, default=900.0,
    #                    help ='Sample rate in MHz for non-interleaved band. Used for spectrum axis scales')
    parser.add_option('-n', dest='ncaptures', type=int, action="store", default=default_captures,
                        help ='Number of data captures (for each correlation product)')
    parser.add_option('-r', dest='repetitions', type=int, action="store", default=default_repetitions,
                        help ='Number of repetitions of on-off pairs')
    parser.add_option('-i', dest='obs_set', type=long, action="store", default=None,
                        help ='Observation set ID. If present it will continue previous observations')
    parser.add_option('-a', dest='ants', type=str, action="store", default=None,
                        help ='Comma separated array list of ATA antennas, eg: \"2j,2d,4k\"')
    parser.add_option('-p', dest='pointings', type=str, action="store", default=None,
                        help ='Comma separated list of on off sources, eg: \"casa,vira,moon\"')
    parser.add_option('-o', dest='off', type=str, action="store", default=default_pointings,
                        help ='Specify the off az,el in degrees, eg: 10,0')
    parser.add_option('-f', dest='freqs', type=str, action="store", default=None,
                        help ='Comma separated list of sky tuning frequencies, in MHz. Only one set of frequencies, eg: \"2000,3000,4000\"')
    parser.add_option('-c', '--config', dest='configfile', type=str, action="store", default=None,
                        help ="config file with measurement parameters")
    parser.add_option('-v', '--verbose', dest='verbose', action="store_true", default=False,
                        help ="More on-screen information")
    parser.add_option('-m', '--mail', dest='mail', action="store", default=None,
                        help ="The recipient e-mail address (if different from default)")

    (options,args) = parser.parse_args()

    if(options.verbose):
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    logger = logger_defaults.getModuleLogger("SNAP_ON_OFF_OBS")

    if len(sys.argv) <= 1:
        logger.warning("no options provided")
        parser.print_help()
        sys.exit(1)

    try:
        if options.configfile:
            import ConfigParser
            configParser = ConfigParser.RawConfigParser()   
            configParser.read(options.configfile)
            
            ant_str = configParser.get('measurement', 'antennas')
            freq_str = configParser.get('measurement', 'freq')
            pointings_str = configParser.get('measurement', 'sources')
    except:
        logger.exception("config file exception")
        raise

    if options.mail:
        ATAComm.setRecipient(options.mail)

    if options.ants:
        ant_str = options.ants

    if options.obs_set: 
        #todo: check if that ID exits
        try: 
            obs_set_id = options.obs_set
            obs_db.getSetData(obs_set_id)
        except:
            logger.error("Data set id {} does not exist".format(obs_set_id))
            raise 
    else:
        obs_set_id = obs_db.getNewObsSetID("OnOff observation")

    if options.freqs:
        freq_str = options.freqs

    if options.pointings:
        pointings_str = options.pointings

    offs = snap_array_helpers.string_to_numeric_array(options.off)
    az_offset = offs[0]
    el_offset = offs[1]

    repetitions = options.repetitions
    ncaptures = options.ncaptures
    
    #TODO: we may need to modify this to add a verbosity/send mail/slack flags
    doOnOffObservations(ant_str,freq_str, pointings_str,az_offset,el_offset,repetitions,ncaptures,obs_set_id,options.fpga_file)

    exit()

def doOnOffObservations(ant_str,freq_str, pointings_str,az_offset,el_offset,repetitions,ncaptures,obs_set_id,fpga_file):

    logger = logger_defaults.getModuleLogger(__name__)

    ant_list = snap_array_helpers.string_to_array(ant_str);
    pointings = snap_array_helpers.string_to_array(pointings_str);
    freq_list = snap_array_helpers.string_to_numeric_array(freq_str);
    ant_list = remove_dups(ant_list)
    full_ant_str = snap_array_helpers.array_to_string(ant_list)


    info_string = ("OnOff Started\nDataset ID {7}\n\nAnts: {0!s}\nFreq: {1!s}\n"
            "Pointings: {2!s}\nOff positions: Az={3:3.2f} El={4:3.2f}\n"
            "Repetitions {5:d}\nCaptures {6:d}").format(full_ant_str, freq_str, pointings_str,az_offset,el_offset,repetitions,ncaptures,obs_set_id)

    logger.info(info_string)
    logger.warning("Communication disabled, edit code")
    #ATAComm.sendMail("SNAP Obs started",info_string)
    #ATAComm.postSlackMsg(info_string)

    try:
        ant_groups = ata_control.get_snap_dictionary(ant_list)
    except:
        logstr = "unable to match antennas with snaps"
        logger.exception(logstr)
        ATAComm.sendMail("SNAP Obs exception",logstr)
        raise

    #getting the antennas. From now on we can modify any antenna parameters
    # Reserve the antennas
    logger.info("Reserving antennas %s in bfa antgroup" % full_ant_str)
    try:
        ata_control.reserve_antennas(ant_list)
    except:
        logstr = "unable to reserve the antennas"
        logger.exception(logstr)
        ATAComm.sendMail("SNAP Obs exception",logstr)
        raise

    # For each SNAP. set the minicircuits attenuators to 12.0
    # To do this, get a list of the first antenna in each snap group
    thread_list = []
    try:
        default_atten_db = 12 # Suggested by jack
        for a in ant_groups.keys():
            t=ata_control.set_atten_thread(["%sx"%ant_groups[a][0],"%sy"%ant_groups[a][0]], [default_atten_db, default_atten_db], False)
            thread_list.append(t)
    except:
        logstr = "unable to set attenuators"
        logger.exception(logstr)
        ATAComm.sendMail("SNAP Obs exception",logstr)
        ata_control.release_antennas(ant_list, True)
        raise

    ata_control.wait_for_threads(thread_list)
    
    current_source = None
    new_antennas = True
    #snap_cli.set_state(snap_cli.PROGRAM_STATE_RUN)
    #snap_cli.server_thread()

    logger.info("starting observations")
    
    try:
        while(1):
            new_antennas = True
            #gets a antenna dictionary and 
            curr_ant_dict,curr_freq_list = onoff_db.get_obs_params(obs_set_id,pointings,ant_groups,freq_list)
            
            #if None, it meast that all was measured
            if not curr_ant_dict:
                logger.info("all seems to be measured")
                break

            for curr_freq in curr_freq_list:

                current_source,was_changed = ata_positions.ATAPositions.getPreferedSourceUp(current_source,pointings)

                if not current_source:
                    errormsg = 'no source is up ({}). terminating observation set {}'.format(','.join(pointings),obs_set_id)
                    logger.error(errormsg)
                    ATAComm.sendMail("SNAP Obs Error",errormsg)
                    ATAComm.postSlackMsg(info_string)
                    raise RuntimeError(errormsg)


                #we either changed antennas or changed source.
                #need to generate the ephemeris and autotune PAMs
                if was_changed:
                    #if we only switched the antennas, we don't need to regenerate
                    # the ephemeris
                    ata_control.create_ephems(current_source, az_offset, el_offset);

                if( was_changed or new_antennas):
                    logger.info("need to (re)run autotune")
                    curr_ant_string = snap_array_helpers.dict_values_to_comma_string(curr_ant_dict)
                    ata_control.point_ants("on", curr_ant_string );
                    ata_control.autotune(curr_ant_string)
                
                new_antennas = False

                ata_control.set_freq(curr_freq, curr_ant_string)

                snap_control.onoff_observations(curr_ant_dict,obs_set_id,curr_freq,fpga_file,current_source,repetitions,ncaptures,az_offset,el_offset)
                #snap_control.do_onoff_obs(args.hosts, \
                #    "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-06-23_1048.fpg", \
                #    source, args.ncaptures, args.repetitions, ants_to_observe, freq, obsid, 0.0, 10.0)
    
            #now, we believe we have measured all frequencies for curr_ant_dict, so we may
            #remove the content of it from our original ant_groups. Note that this function
            #alters the ant_groups!
            onoff_db.remove_antennas_from_dict(ant_groups,curr_ant_dict);
            
    
    except KeyboardInterrupt:
        logger.info("Keyboard interuption")
    except:
        logger.exception("something went wrong")
        raise
    finally: 
        logger.info("shutting down")
        ATAComm.sendMail("SNAP Obs End","Finishing measurements")
        ata_control.release_antennas(ant_list, True)
        #snap_cli.server_close()

if __name__== "__main__":
    main()

