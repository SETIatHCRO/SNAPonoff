#!/usr/bin/python

"""
Python wrappers for redis. 
"""

import sys

import redis
from plumbum import local, ProcessExecutionError, BG
from threading import Thread
import time

class RedisManager:
    __instance = None
    __r = None
    @staticmethod
    def get_instance():
        """ Static access method. """
        if RedisManager.__instance == None:
            RedisManager()
        return RedisManager.__instance
    def __init__(self):
        """ Virtually private constructor. """
        if RedisManager.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            self._setupTunnel()
            RedisManager.__instance = self
            RedisManager.__r = redis.StrictRedis(host='localhost', port=6379, db=0)

    def _setupTunnel(self):

        try:
            ssh = local["ssh"]
            cmd = ssh[("-NfL", 
                "6379:localhost:6379", 
                "sonata@antfeeds.setiquest.info", 
                "-o", 
                "ExitOnForwardFailure yes",
                ">",
                "/dev/null",
                "2>&1")] & BG
            #run()
        except ProcessExecutionError:
            pass
        except TypeError:
            pass

        time.sleep(1)


    def get(self, key):
        result = self.__r.get(key)
        return { "redis" : "get",  "key" : key, "value" : result }

    def set(self, key, value):
        result = self.__r.set(key, value)
        return { "redis" : "set",  "key" : key, "value" : value, "result" : result }

    def delete(self, key):
        result = self.__r.delete(key)
        return { "redis" : "del",  "key" : key, "result" : result }

    def get_pubsub(self):
        return self.__r.pubsub()

    def pub(self, channel, value):
        # Note: the redis module does not support publish, need to use redis-cli
        redis_cli = local["redis-cli"]
        cmd = redis_cli[("publish", channel, value)]
        result = cmd()
        return { "redis" : "pub",  "channel" : channel, "value" : value, "result" : result }

    def pubsub(self, channel_name_list, handle_pub_result_method):

        p = self.__r.pubsub()
        for channel_name in channel_name_list:
            p.subscribe(channel_name)
        t = Thread(target=handle_pub_result_method, args=(p, )).start()
        return p

if __name__== "__main__":

    def handle_pub_result(p):
        for item in p.listen(): 
            print item

    print "redis test"
    print RedisManager.get_instance().delete("test")
    print RedisManager.get_instance().delete("test")
    print RedisManager.get_instance().set("test", "3")
    print RedisManager.get_instance().get("test")
    
    channel_name_list = ["channel1", "channel2"]
    p = RedisManager.get_instance().get_pubsub()
    for channel_name in channel_name_list:
        p.unsubscribe(channel_name)
    subscribed_channels = RedisManager.get_instance().pubsub(channel_name_list, handle_pub_result)
    time.sleep(1)
    print RedisManager.get_instance().pub("channel1", "4")

    for channel_name in channel_name_list:
        subscribed_channels.unsubscribe(channel_name)

