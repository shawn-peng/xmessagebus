import unittest

import xmessagebus

from absl import logging

logging.use_absl_handler()

logging.set_verbosity(logging.INFO)


class MyTestCase(unittest.TestCase):

    def test_send_msg_mainbus(self):
        xmessagebus.mainbus.publish('Testing', 'Test message')
        print('msg sent')

    def tearDown(self) -> None:
        xmessagebus.shutdown()


if __name__ == '__main__':
    unittest.main()

print('tests finished')
