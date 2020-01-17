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
#from plumbum import local
import math
import os
import datetime as dt
import snap_array_helpers
import math
import logging
import MySQLdb
import MySQLdb.cursors

DB_INFO =  [ { 'user':'sonata', 'host':'sonata1', 'db_name':'ants', 'primary':True },
             { 'user':'ata-sensors-rw', 'host':'googlesql', 'db_name':'ants' },
             { 'user':'jrseti', 'host':'googledb', 'db_name':'ants' } ]

DB_INFO_TEST =  [ { 'user':'sonata', 'host':'sonata1', 'db_name':'ants', 'primary':True },
            { 'user':'ata-sensors-rw', 'host':'googlesql', 'db_name':'ants' },
            { 'user':'jrseti', 'host':'googledb', 'db_name':'ants' } ]

MYSQL_USER = "sonata"
MYSQL_HOST = "sonata1"
MYSQL_DATABASE = "ants"


db_info = DB_INFO
snap_onoff_atten = "snap_onoff_atten"
snap_onoff_obs = "snap_onoff_obs"
observations = "observations"
obs_type = "o"

def get_db_info(primary_only=False):

    """ Generator for dabase connection info.
    If primary_only, return only if primary key exists.
    """

    global db_info

    if primary_only:
        for i in range(0, len(db_info)):
            if 'primary' in db_info[i] and db_info[i]['primary'] == True:
                yield db_info[i]
        return

    for i in range(0, len(db_info)):
        yield db_info[i]

def set_test(clear=False):
    global snap_onoff_atten
    global snap_onoff_obs
    global observations
    global db_info
    db_info = DB_INFO_TEST
    snap_onoff_atten = "test_snap_onoff_atten"
    snap_onoff_obs = "test_snap_onoff_obs"
    observations = "test_observations"

    if(clear == True):
        db_query("delete from test_snap_onoff_atten")
        db_query("delete from test_snap_onoff_obs")
        db_query("delete from test_observations")
        db_query("ALTER TABLE test_snap_onoff_atten AUTO_INCREMENT = 1")
        db_query("ALTER TABLE test_snap_onoff_obs AUTO_INCREMENT = 1")
        db_query("ALTER TABLE test_observations AUTO_INCREMENT = 1")


def db_init():

    try:
        for info in get_db_info():
            info['db_cursor'] = None
            my = MySQLdb.connect(host = info['host'], user = info['user'], \
                passwd = '', cursorclass = MySQLdb.cursors.DictCursor)
            my_cursor = my.cursor()
            my_cursor.execute("USE %s" % info['db_name']);
            info['db_cursor'] = my_cursor
            info['db'] = my
            #print("CURSOR " , my_cursor, my)
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print("MYSQL ERROR: init_db(): %s", str(e))

def db_query(query):

    try:

        try:
            my = MySQLdb.connect(host = MYSQL_HOST, user = MYSQL_USER, \
                    passwd = '', cursorclass = MySQLdb.cursors.DictCursor)
            my_cur = my.cursor()
            my_cur.execute("USE %s" % MYSQL_DATABASE);
            my_cur.execute(query);
            my.commit()
        except (MySQLdb.Error, MySQLdb.Warning) as e:
            return { "status" : "error", "details" : str(e) }

        try:
            result_dict = my_cur.fetchall()
            sql_cols = [ col[0] for col in my_cur.description ]

            # This is a list comprehension within a dictionary comprehension
            # which builds the full dictionary in a single line.  
            sql_dict = { k : [ d[k] for d in result_dict ] for k in sql_cols }
            sql_dict['status'] = "OK"

            return sql_dict
        except TypeError as e:
            return { "status" : "OK", "details" : "The query may be of a type that does not return a result"}

    finally:
            my.close()

def get_most_recent_obsid():

    global observations
    global obs_type
    result = db_query("SELECT MAX(id) from %s where obs_type='%s'" % (observations, obs_type));
    if(result['status'] == "OK"):
        return result['MAX(id)'][0]
    else:
        logger = logging.getLogger(__name__)
        logger.info("In get_most_recent_obsid(), returned: %s" % result['status']);
        return -1;

def start_new_obs(antlist, freq, target, az_offset, el_offset):

    global observations
    global obs_type
    result = db_query("INSERT INTO %s set ts_start=now(), ants='%s', \
                        freq=%.3f, target='%s', az_offset=%f, el_offset=%.2f, obs_type='%s'" % \
                        (observations, antlist, freq, target, az_offset, el_offset, obs_type))
    print "INSERT INTO %s set ts_start=now(), ants='%s', \
                        freq=%.3f, target='%s', az_offset=%f, el_offset=%.2f, obs_type='%s'" % \
                        (observations, antlist, freq, target, az_offset, el_offset, obs_type)
    if(result['status'] == "OK"):
        obsid = get_most_recent_obsid()
        return { "status" : "OK", "obsid" : obsid, \
                "details" : "New obs with id = %d recorded as started" % obsid }
    else:
        logger = logging.getLogger(__name__)
        logger.info("In start_new_obs(), returned: %s" % result['status']);
        return result;
    cmd = ""

def end_most_recent_obs():

    obsid = get_most_recent_obsid()
    if(obsid < 0):
        return { 'status' : 'Error: get_most_recent_obsid() returned invalid value' }

    global observations
    result = db_query("UPDATE %s set ts_stop=now() where id=%d" % (observations, obsid))

    if(result['status'] == "OK"):
        return { "status" : "OK", "details" : "New ob with id = %d recorded as ended" % obsid }
    else:
        logger = logging.getLogger(__name__)
        logger.info("In end_most_recent_obs(), returned: %s" % result['status']);
        return result;

def get_most_recent_onoff_obsid(snap):

    global snap_onoff_obs
    result = db_query("SELECT MAX(id) from %s where snap='%s'" % (snap_onoff_obs, snap));
    if(result['status'] == "OK"):
        return result['MAX(id)'][0]
    else:
        logger = logging.getLogger(__name__)
        logger.info("In get_most_recent_obsid(), returned: %s" % result['status']);
        return result;

def record_on_off_obs(snap, ant, source, freq, onoff, rep):

    global snap_onoff_obs

    obsid = get_most_recent_obsid();
    if(obsid < 0):
        return { 'status' : 'Error: record_on_off_obs() returned invalid value' }

    result = db_query ("INSERT into %s VALUES (NULL, now(), '%s', %d, '%s', '%s', %.2f, '%s', %d)" % \
            (snap_onoff_obs, snap, obsid, ant, source, freq, onoff, rep))

    if(result['status'] != "OK"):
        logger = logging.getLogger(__name__)
        logger.info("In record_on_off_obs(), returned: %s" % result['status']);
        return result;

    this_id = get_most_recent_onoff_obsid(snap)
    return { "status" : "OK", "details" : "Recorded in db new on_off for %s, %s, %s, %.2f, obsid=%d, %s id=%d" %
            (snap, ant, source, freq, obsid, snap_onoff_obs, this_id)}
    return db_query(query);

def get_latest_onoff_obs(snap, source):

    global snap_onoff_obs
    result = db_query ("SELECT * from %s where snap='%s' and source='%s' ORDER BY id DESC LIMIT 1" % (snap_onoff_obs, snap, source))

    print result

    if(result['status'] == "OK" and len(result['source']) == 0):
        return { "status" : 'NONE' }
    return result

def record_atten(antpol, obsid, source, freq, db):

    global snap_onoff_atten
    result = db_query ("INSERT INTO %s VALUES (NULL, now(), '%s', '%s', '%s', '%.2f', '%.2f')" % \
            (snap_onoff_atten, antpol, obsid, source, freq, db))

    if(result['status'] != "OK"):
        logger = logging.getLogger(__name__)
        logger.info("In record_atten(), returned: %s" % result['status']);
        return result;

    return { "status" : "OK" }

def get_atten_db(antpol, source, freq):

    global snap_onoff_atten
    result = db_query("SELECT db from %s where ant='%s' and source='%s' and freq=%.2f DESC LIMIT 1" % \
            (snap_onoff_atten, antpol, source, freq))
    if(result['status'] == "OK"):
        return float(result['db'][0])
    else:
        logger = logging.getLogger(__name__)
        logger.info("In get_atten_db(%s, %s, %s), returned: %s" % (antpol, source, str(freq), result['status']));
        return 0.0;



if __name__== "__main__":

    #print get_most_recent_obsid()
    #print start_new_obs("1a,1b,2a", 1000.0, "casa", 0.0, 10.0)
    #print end_most_recent_obs()
    #print record_on_off_obs('snap1', '1a', 'casa', 1000.0, "on", 1)
    #set_test()
    db_init()
    print get_latest_onoff_obs('snap0', 'casa')
    print get_latest_onoff_obs('snap0', 'taua')
    print get_latest_onoff_obs('snap1', 'casa')
    print get_latest_onoff_obs('snap1', 'taua')
    print get_most_recent_onoff_obsid('snap1')

