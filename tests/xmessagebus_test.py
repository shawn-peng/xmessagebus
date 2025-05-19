import sys
import unittest

from absl import logging
from absl import flags

flags.FLAGS.mark_as_parsed()

logging.use_absl_handler()

logging.set_verbosity(logging.INFO)

logging.get_absl_handler().activate_python_handler()

logging.get_absl_handler().python_handler.stream = sys.stderr

import xmessagebus


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        flags.FLAGS.mark_as_parsed()
        logging.use_absl_handler()
        logging.set_verbosity(logging.INFO)
        logging.get_absl_handler().activate_python_handler()
        logging.get_absl_handler().python_handler.stream = sys.stderr

    def test_send_msg_mainbus(self):
        xmessagebus.mainbus.publish('Testing', 'Test message')
        print('msg sent')

    def tearDown(self) -> None:
        xmessagebus.shutdown()


if __name__ == '__main__':
    unittest.main()

print('tests finished')
