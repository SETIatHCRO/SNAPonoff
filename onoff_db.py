#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
operations on observator (obs) database for onoff measurements

Created Jan 2020

@author: jkulpa
"""

from ATATools import obs_db,logger_defaults,snap_array_helpers
import ATASQL


def get_all_meas_dict(setid,antenna_list):
    
    logger= logger_defaults.getModuleLogger(__name__)

    mydb = ATASQL.connectObsDb()
    mycursor = mydb.cursor()

    if not antenna_list:
        logger.warning('antenna list empty for id {}'.format(setid))
        return None

    insertcmd_part = ("select recordings.freq,recordings.description,recordings.id, "
            "rec_ants.ant,rec_ants.az,rec_ants.el "
            "from (recordings inner join rec_ants on recordings.id = rec_ants.id ) "
            "where recordings.status = 'OK' and  recordings.setid = %s ")
    cpart2 = (" and rec_ants.ant in (%s)")
    in_p=', '.join(map(lambda x: '%s', antenna_list))

    insertcmd_part2 = cpart2 % in_p

    insertcmd = insertcmd_part + insertcmd_part2

    exec_list = [setid] + antenna_list

    logger.info("getting previous measurements for id {} antennas {}".format(setid,",".join(antenna_list)))
    mycursor.execute(insertcmd,exec_list)
    myiterator = mycursor.fetchall()

    if not myiterator:
        mycursor.close()
        mydb.close()
        return None
    
    returndict = {}
    for (freq,desc,obsid,ant,az,el) in myiterator:
        if ant not in returndict:
            cdict = {'freq':[],'desc':[],'obsid':[],'az':[],'el':[]}
            returndict[ant] = cdict

        returndict[ant]['freq'].append(freq)
        returndict[ant]['desc'].append(desc)
        returndict[ant]['obsid'].append(obsid)
        returndict[ant]['az'].append(az)
        returndict[ant]['el'].append(el)


    mycursor.close()
    mydb.close()

    return returndict


def get_obs_params(setid,sources,ant_snap_dictionary,freq_list):

    all_antennas_list = snap_array_helpers.dict_list_to_list(ant_snap_dictionary)
    logger= logger_defaults.getModuleLogger(__name__)

    meas_dictionary = get_all_meas_dict(setid,all_antennas_list)

    snapKeys = ant_snap_dictionary.keys()
    outputDict = {}
    if meas_dictionary:

        #using sets because it automatically removes duplicates
        #we are allowing to re-measure some frequencies on some 
        #of the antennas. e.g. ant1a was measured on 1,2,3 GHz
        #and ant1c on 1,2 GHz and they belong to different snaps,
        #while order was 1,2,3,4 GHzwe would order to measure
        #1a and 2c on 3 and 4 GHz, duplicating 3GHz for 1a
        freq_set = set(freq_list)
        todo_freq_set = set()
        antennas_got = meas_dictionary.keys()
        for sk in snapKeys:
            clist = ant_snap_dictionary[sk]
            #for each host, we are searching for first antenna that
            #was not measured, or was measured but has still some
            #not measured frequencies
            for cant in clist:
                if cant in antennas_got:
                    #antenna was measured, testing freq list
                    flist = meas_dictionary[cant]['freq']
                    diffset = freq_set - set(flist)
                    #we have some unmeasured freqencies for that antenna
                    if diffset:
                        outputDict[sk] = cant
                        todo_freq_set.update(diffset)
                        break
                else:
                    outputDict[sk] = cant
                    todo_freq_set.update(freq_list)
                    break 


        outputFreqList = list(todo_freq_set)
    else:
        #there were no measurements for those antennas
        #taking first antenna from each list and copying
        #the frequency list
        outputFreqList = freq_list
        for sk in snapKeys:
            clist = ant_snap_dictionary[sk]
            outputDict[sk] = clist[0]
        

    return outputDict,outputFreqList

def remove_antennas_from_dict(ant_groups,curr_ant_dict):
    
    meas_keys = curr_ant_dict.keys()
    for ck in meas_keys:
        ant_groups[ck].remove(curr_ant_dict[ck])
        #if there is no antenna on the list, pop the dictionary entry
        if len(ant_groups[ck]) == 0:
            ant_groups.pop(ck)


