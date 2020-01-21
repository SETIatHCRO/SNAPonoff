#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
operations on observator (obs) database for onoff measurements

Created Jan 2020

@author: jkulpa
"""

from ATATools import obs_db,logger_defaults


def get_all_meas_dict(setid,antenna_list):
    
    logger= logger_defaults.getModuleLogger(__name__)

    mydb = connect.connectObsDb()
    mycursor = mydb.cursor()

    returndict = {}

    insertcmd_part = ("select observations.freq,observations.description,observations.id, "
            "obs_ants.ant,obs_ants.az,obs_ants.el "
            "from (observations inner join obs_ants on observations.id = obs_ants.id ) "
            "where observations.status = 'OK' and  observations.setid = %s and obs_ants.ant in (%s)")
    in_p=', '.join(map(lambda x: '%s', antenna_list))

    insertcmd = insertcmd_part % in_p

    exec_list = [setid] + antenna_list

    myiterator = mycursor.execute(insertcmd,exec_list)

    import pdb
    pdb.set_trace()

    return returndict


def get_obs_params(setid,sources,ant_snap_dictionary,freq_list):
    all_antennas_list = ant_snap_dictionary.values()

    meas_dictionary = get_all_meas_dict(setid,all_antennas_list)

    import pdb
    pdb.set_trace()

    freq_set = set(freq_list)

    antennas_got = meas_dictionary.keys()





