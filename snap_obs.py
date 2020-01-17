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
import snap_array_helpers
import snap_obs_selector
import snap_obs_db
import snap_control
import snap_cli
from  ATATools import ata_control,logger_defaults,snap_array_helpers
import ATAComm 

default_fpga_file = "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-06-23_1048.fpg"
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
    parser.add_option('-a', dest='ants', type=str, action="store", default=None,
                        help ='Comma separated array list of ATA antennas, eg: \"2j,2d,4k\"')
    parser.add_option('-p', dest='pointings', type=str, action="store", default=None,
                        help ='Comma separated list of pointings, eg: \"casa,vira,moon\"')
    parser.add_option('-o', dest='off', type=str, action="store", default=default_pointings,
                        help ='Specify the off az,el in degrees, eg: 10,0')
    parser.add_option('-f', dest='freqs', type=str, action="store", default=None,
                        help ='Comma separated list of sky tuning frequencies, in MHz. Only one set of frequencies, eg: \"2000,3000,4000\"')
    parser.add_option('-c', '--config', dest='configfile', type=str, action="store", default=None,
                        help ="config file with measurement parameters")
    parser.add_option('-v', '--verbose', dest='verbose', action="store_true", default=False,
                        help ="More on-screen information")

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

    if options.ants:
        ant_str = options.ants

    if options.freqs:
        freq_str = options.freqs

    if options.pointings:
        pointings_str = options.pointings

    ant_list = snap_array_helpers.string_to_array(ant_str);
    pointings = snap_array_helpers.string_to_array(pointings_str);
    freq_list = snap_array_helpers.string_to_numeric_array(freq_str);
    ant_list = remove_dups(ant_list)
    full_ant_str = snap_array_helpers.array_to_string(ant_list)

    offs = snap_array_helpers.string_to_numeric_array(options.off)
    az_offset = offs[0]
    el_offset = offs[1]

    info_string = ("OnOff Started\n\nAnts: {0!s}\nFreq: {1!s}\n"
            "Pointings: {2!s}\nOff positions: Az={3:3.2f} El={4:3.2f}\n"
            "Repetitions {5:d}\nCaptures {6:d}").format(full_ant_str, freq_str, pointings_str,az_offset,el_offset,options.repetitions,options.ncaptures)

    logger.info(info_string)
    ATAComm.sendMail("SNAP Obs started",info_string)

    try:
        ant_groups = ata_control.get_snap_dictionary(ant_list)
        snap_list = ant_groups.keys()
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
    try:
        default_atten_db = 12 # Suggested by jack
        for a in ant_groups.keys():
            ata_control.set_atten_thread(["%sx"%ant_groups[a][0],"%sy"%ant_groups[a][0]], [default_atten_db, default_atten_db], False)
    except:
        logstr = "unable to set attenuators"
        logger.exception(logstr)
        ATAComm.sendMail("SNAP Obs exception",logstr)
        ata_control.release_antennas(ant_list, True)
        raise


    current_source = ""
    current_obsid = -1
    current_freq = 0.0
    obsid = -1

    snap_cli.set_state(snap_cli.PROGRAM_STATE_RUN)
    snap_cli.server_thread()

    logger.info("starting observations")

    try:
        raise RuntimeError("test")
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
            logger.info(pointings)
            logger.info(ant_groups)
            logger.info(freq_list)
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
        logger.info("Keyboard interuption")
        snap_obs_db.end_most_recent_obs()
    finally: 
        logger.info("shutting down")
        ATAComm.sendMail("SNAP Obs End","Finishing measurements")
        ata_control.release_antennas(ant_list, True)
        snap_cli.server_close()
    
    exit()


if __name__== "__main__":
    main()

