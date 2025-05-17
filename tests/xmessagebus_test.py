import unittest

import xmessagebus


class MyTestCase(unittest.TestCase):

    def test_send_msg_mainbus(self):
        xmessagebus.mainbus.publish('Testing')


if __name__ == '__main__':
    unittest.main()
