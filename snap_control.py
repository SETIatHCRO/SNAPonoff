#!/usr/bin/python

# snap_control.py
# Module containing methods to control the SNAP boards
# Author: Jon Richards, SETI Institute
#         Sep 05, 2018

from ATATools import snap_array_helpers,ata_control,logger_defaults,obs_db
from SNAPobs import snap_observations

#RECORDER_PATH = "/home/sonata/jr/SNAPonoff2/snap_record.py"
#DEFAULT_DATA_DIR = "~/data"

defaultrms = 12

def onoff_observations(ant_dict,obs_set_id,freq,fpga_file,source,repetitions,ncaptures,az_offset,el_offset):
    """
    do a series of On Off observations for given set ID
    """
    logger= logger_defaults.getModuleLogger(__name__)

    if not obs_set_id:
        logger.error("no set id for On Off observations")
        raise RuntimeError("no set id")

    obsids = []
    for rep in range(repetitions):
        for on_or_off in ["on", "off"]:
            ants = snap_array_helpers.dict_values_to_comma_string(ant_dict)
            logger.info("pointing antennas {} to position {}".format(ants,on_or_off))
            ata_control.point_ants(on_or_off, ants)
            desc = "{} repetition {}".format(on_or_off.upper(),rep)
            filefragment = "{0!s}_{1:03d}".format(on_or_off,rep)
            if(on_or_off == "on" and rep == 0):
                rms = defaultrms
            else:
                rms = None
            cobsid = snap_observations.observe_same(ant_dict,freq,source,ncaptures,
                    "ON-OFF","ataonoff",desc,filefragment,rms,az_offset,el_offset,fpga_file,obs_set_id)
            obsids.append(cobsid)
    
    #if we got to this point without raising an exception, we are marking all measurements as OK
    logger.info("marking observations {} as OK".format(', '.join(obsids)))
    obs_db.markObservationsOK(obsids)


if __name__== "__main__":

    #logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
    #logger.setLevel(logging.INFO)
    #sh = logging.StreamHandler(sys.stdout)
    #fmt = logging.Formatter('[%(asctime)-15s] %(module)s %(message)s')
    #sh.setFormatter(fmt)
    #logger.addHandler(sh)
    ata_control.setup_logger()
    ata_control.set_output_dir()
    logger = logging.getLogger()

    ants = snap_array_helpers.array_to_string(['2a', '2j'])
    snaps = snap_array_helpers.array_to_string(['snap0', 'snap1'])

    logger.info(ata_control.create_ephems("moon", 0.0, 5.0))
    logger.info(ata_control.point_ants("on", ants))

    print "Start test %s" % ants
    print snap_obs_db.start_new_obs(ants, 1000.0, "moon", 0.0, 10.0)
    obsid = snap_obs_db.get_most_recent_obsid()

    do_onoff_obs("snap0", \
            "/home/sonata/dev/ata_snap/snap_adc5g_spec/outputs/snap_adc5g_spec_2018-06-23_1048.fpg", \
            "moon", 16,  3, "2a", 1000.0, obsid, 0.0, 10.0)

#def do_onoff_obs_threaded(snap, fpga_file, on_or_off, num_captures, ant, source, rep, obsid, freq, data_dir):
#
#    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
#    logger.info("Starting %s %s obs for %s, freq: %.2f, %d rep, obsid: %d, source: %s, caps: %d, rep=%d" % \
#                (snap, on_or_off, ant, freq, rep, obsid, source, num_captures, rep))
#
#    args = ["python", RECORDER_PATH,\
#            snap, fpga_file, "-n", \
#            "%d" % num_captures, "-a", ant, "-p", data_dir, \
#            "-x", source, "-o", "%d"%int(obsid), \
#            "-c", "%s_%s_%03d_ant_%s_%.2f_obsid%d" % \
#            (source, on_or_off, rep, ant, freq, obsid)]
#
#    if(on_or_off == "on" and rep == 0):
#        args.append("-t")
#        args.append("12.0")
#
#    proc = Popen(args)
#    proc.wait()
#
#    if(proc.returncode == 0):
#        logger.info("Finished %s obs for %s, freq: %.2f, %d rep, obsid: %d, source: %s, returncode: %d" % \
#                    (on_or_off, ant, freq, rep, obsid, source, proc.returncode))
#        logger.info(snap_obs_db.record_on_off_obs(snap, ant, source, freq, on_or_off, rep))
#    else:
#        logger.info("ERROR: Fnished %s obs for %s, freq: %.2f, %d rep, obsid: %d, source: %s, returncode: %d" % \
#                    (on_or_off, ant, freq, rep, obsid, source, proc.returncode))
#        ata_control.send_email("SNAP Problems", "%s failed, exiting" % RECORDER_PATH)
#        sys.exit(1)
#
#    # snaps and ants are csv strings
#def do_onoff_obs(snaps, fpga_file, source, num_captures, repetitions, ants, freq, obsid, az_offset, el_offset):
#
#    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
#
#    logger.info("do_onoff_obs(%s, %s, %s, %d, %d, %s, %.2f, %d, %.2f, %.2f)" % \
#            (snaps, fpga_file, source, num_captures, repetitions, ants, freq, obsid, az_offset, el_offset))
#
#    # set the RF switches
#    try:
#        ata_control.rf_switch_thread(ants, True)
#    except RuntimeError, err:
#        logger.info("Error in snap_control.py setting rfswitch, exiting: %s" % err)
#        ata_control.send_email("RF switch problem", err)
#        sys.exit(1)
#    #except Exception, err:
#    #    logger.info("Error in snap_control.py setting rfswitch, exiting: %s" % err)
#    #    ata_control.send_email("RF switch problem", err)
#    #    sys.exit(1)
#
#    snap_list = snap_array_helpers.string_to_array(snaps)
#    ant_list = snap_array_helpers.string_to_array(ants)
#
#    for rep in range(repetitions):
#
#        for on_or_off in ["on", "off"]:
#
#            RedisManager.get_instance().set_and_pub('onoff_params', { 'ants' : ant_list, 'freq' : float(freq), 'source' : source, 'position' : on_or_off, 'rep' : rep }, 'onoff_params')
#
#            print "ONOFF PARAMS\n\n"
#            print { 'ants' : ant_list, 'freq' : float(freq), 'source' : source, 'position' : on_or_off, 'rep' : rep }
#
#            RedisManager.get_instance().set_and_pub('onoff_moving', { 'state' : 'moving' }, 'onoff_state')
#
#            print "##########"
#            print "%s - %s" % (on_or_off.upper(), ants)
#            print "##########"
#
#            logger.info("Performing %s for %s" % (on_or_off.upper(), ants))
#
#            # Position the ants
#            logger.info("Move all ants %s target: %s" % (on_or_off.upper(), ata_control.point_ants(on_or_off, ants ))); 
#
#            RedisManager.get_instance().set_and_pub('onoff_moving', { 'state' : 'moving_finished' }, 'onoff_state')
#
#            threads = []
#
#            for idx, ant in enumerate(ant_list):
#
#
#                snap = snap_list[idx]
#
#                t = Thread(target=do_onoff_obs_threaded, \
#                        args=(snap, fpga_file, \
#                                on_or_off, num_captures, ant, source, \
#                                rep, obsid, freq, ata_control.get_output_dir()))
#                t.start()
#                time.sleep(1)
#                threads.append(t)
#
#            for t in threads:
#                t.join()
#
#        logger.info("Finished %s for %s" % (on_or_off, ants))

