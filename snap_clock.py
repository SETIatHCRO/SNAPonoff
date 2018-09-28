#!/usr/bin/python

"""
snap_clock.py
This program will query the SNAP frequency generator for the 
output frequency and amplitude.
"""

import os

import telnetlib
import snap_redis

_host = "10.1.49.160"
_port = 5025

def get_clock_settings(store_in_redis=False):


    telnet = telnetlib.Telnet()
    telnet.open(_host, _port)

    telnet.write("FREQ?\n")
    freq = telnet.read_until("\n", 5)
    telnet.write("AMPR?\n")
    ampr = telnet.read_until("\n", 5)

    answer = { "type" : "clock", "freq" : float(freq), "ampr" : float(ampr) }

    if(store_in_redis == True):
        snap_redis.RedisManager.get_instance().set("snap_clock", answer)

    return answer

def set_clock_freq(freq_mhz):

    print ('To set the clock freq, "telnet %s %d" then "FREQ %f MHz"' % (_host, _port, float(freq_mhz)))

def set_ampr(ampr_db):

    print ('To set rhe amplitude of the RF output, "telnet %s %d" then "AMPR %f"' % (_host, _port, float(ampr_db)))


if __name__== "__main__":

    get_clock_settings(True)
    get_clock_settings(False)
    set_clock_freq(1800)
    set_ampr(16.0)

    print (snap_redis.RedisManager.get_instance().get("snap_clock"))


