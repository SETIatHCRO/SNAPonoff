"""
Test the snap_redis module
"""

import sys
import time

import unittest

sys.path.append("..")
from snap_redis import RedisManager 

class RedisManagerTest(unittest.TestCase):

    _test_channel_name_base = "_test_channel"
    _test_channels_list = [
            "%s1"%_test_channel_name_base, 
            "%s2"%_test_channel_name_base,
            "%s3"%_test_channel_name_base,
            "%s4"%_test_channel_name_base,
            "%s5"%_test_channel_name_base,
            "%s6"%_test_channel_name_base,
            "%s7"%_test_channel_name_base]
    _test_key_name = "snap_test_key"

    def setUp(self):

        self.set_to_defaults()

    def tearDown(self):

        self.set_to_defaults()

    def test_1(self):

        """
        {'pattern': None, 'type': 'subscribe', 'channel': 'test_channel1', 'data': 1L}
        {'pattern': None, 'type': 'subscribe', 'channel': 'test_channel2', 'data': 2L}
        {'pattern': None, 'type': 'message', 'channel': 'test_channel1', 'data': '0'}
        {'pattern': None, 'type': 'message', 'channel': 'test_channel2', 'data': '1'}
        {'pattern': None, 'type': 'unsubscribe', 'channel': 'test_channel1', 'data': 1L}
        {'pattern': None, 'type': 'unsubscribe', 'channel': 'test_channel2', 'data': 0L}
        """
        def handle_pub_result(p):
            for item in p.listen():
                for idx, name in enumerate(self._test_channels_list):
                    if(item['type'] == "message" and \
                        item["channel"] == "%s%d" % (self._test_channel_name_base, (idx + 1))):
                            assert(int(item['data']) == idx)

       # Test get and set 

        # {'redis': 'del', 'result': 0, 'key': 'snap_test_key'}
        assert(RedisManager.get_instance().delete(self._test_key_name)["result"] == 0)
        # {'redis': 'set', 'value': '3', 'key': 'snap_test_key', 'result': True}
        result = RedisManager.get_instance().set(self._test_key_name, "3")
        assert(int(result["value"]) == 3 and bool(result["result"]) == True)
        #{'redis': 'get', 'value': '3', 'key': 'snap_test_key'}
        result = RedisManager.get_instance().get(self._test_key_name)
        assert(int(result["value"]) == 3 and result["key"] == self._test_key_name)

        # Test pubsub

        # Create the subscriptions to the channels
        p = RedisManager.get_instance().get_pubsub()
        subscribed_channels = RedisManager.get_instance().pubsub(self._test_channels_list, handle_pub_result)

        # Give some time for the subscriptions to kick in
        time.sleep(1)

        # Publish to the channels. The results will show up in handle_pub_result()
        for idx, channel_name in enumerate(self._test_channels_list):
            RedisManager.get_instance().pub(channel_name, idx)

        # Unsubscribe to the channels. This will exit the subscription thread.
        for channel_name in self._test_channels_list:
            subscribed_channels.unsubscribe(channel_name)

        assert(True);

    def set_to_defaults(self):

         # Clear the channels for testing
        p = RedisManager.get_instance().get_pubsub()
        for channel_name in self._test_channels_list:
            p.unsubscribe(channel_name)

        # Delete the test value
        RedisManager.get_instance().delete(self._test_key_name)

#if __name__ == '__main__':
#    unittest.main()
