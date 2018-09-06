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
from plumbum import local
import math
import os
import datetime as dt
import snap_onoffs_contants
import snap_array_helpers
import math
import logging
import MySQLdb
import MySQLdb.cursors

MYSQL_USER = "sonata"
MYSQL_HOST = "sonata1"
MYSQL_DATABASE = "ants"

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

    result = db_query("select MAX(id) from observations");
    if(result['status'] == "OK"):
        return result['MAX(id)'][0]
    else:
        logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
        logger.info("In get_most_recent_obsid(), returned: %s" % result['status']);
        return -1;

def start_new_obs(antlist, freq, target, az_offset, el_offset):

    result = db_query("INSERT INTO observations set ts_start=now(), ants='%s', \
                        freq=%.3f, target='%s', az_offset=%f, el_offset=%.2f" %
                        (antlist, freq, target, az_offset, el_offset));
    if(result['status'] == "OK"):
        obsid = get_most_recent_obsid()
        return { "status" : "OK", "obsid" : obsid, \
                "details" : "New obs with id = %d recorded as started" % obsid }
    else:
        logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
        logger.info("In start_new_obs(), returned: %s" % result['status']);
        return result;
    cmd = ""

def end_most_recent_obs():

    obsid = get_most_recent_obsid()
    if(obsid < 0):
        return { 'status' : 'Error: get_most_recent_obsid() returned invalid value' }

    result = db_query("UPDATE observations set ts_stop=now() where id=%d" % obsid)

    if(result['status'] == "OK"):
        return { "status" : "OK", "details" : "New ob with id = %d recorded as ended" % obsid }
    else:
        logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
        logger.info("In end_most_recent_obs(), returned: %s" % result['status']);
        return result;

def get_most_recent_onoff_obsid(snap):

    result = db_query("select MAX(id) from snap_onoff_obs where snap='%s'" % snap);
    if(result['status'] == "OK"):
        return result['MAX(id)'][0]
    else:
        logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
        logger.info("In get_most_recent_obsid(), returned: %s" % result['status']);
        return result;

def record_on_off_obs(snap, ant, source, freq):

    obsid = get_most_recent_obsid();
    if(obsid < 0):
        return { 'status' : 'Error: record_on_off_obs() returned invalid value' }

    result = db_query ("INSERT into snap_onoff_obs VALUES (NULL, now(), '%s', %d, '%s', '%s', %.2f)" % \
            (snap, obsid, ant, source, freq))

    if(result['status'] != "OK"):
        logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
        logger.info("In record_on_off_obs(), returned: %s" % result['status']);
        return result;

    this_id = get_most_recent_onoff_obsid(snap)

    return { "status" : "OK", "details" : "Recorded in db new on_off for %s, %s, %s, %.2f, obsid=%d, snap_onoff_obs id=%d" %
            (snap, ant, source, freq, obsid, this_id)}
    return db_query(query);

def get_latest_onoff_obs(snap):

    result = db_query ("select * from snap_onoff_obs where snap='%s' ORDER BY id DESC LIMIT 1" % snap)

    if(result['status'] == "OK" and len(result['source']) == 0):
        return { "status" : 'NONE' }
    return result



if __name__== "__main__":

    print get_most_recent_obsid()
    print start_new_obs("1a,1b,2a", 1000.0, "casa", 0.0, 10.0)
    print end_most_recent_obs()
    print record_on_off_obs('snap1', '1a', 'casa', 1000.0)
    print get_latest_onoff_obs('snap1')
    print get_latest_onoff_obs('snap4')

