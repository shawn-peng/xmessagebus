import asyncio
import sys
import threading
import time
import unittest

from absl import logging
from absl import flags

flags.FLAGS.mark_as_parsed()

logging.use_absl_handler()

logging.set_verbosity(logging.DEBUG)

logging.get_absl_handler().activate_python_handler()

logging.get_absl_handler().python_handler.stream = sys.stderr

import xmessagebus


class TimeoutError(Exception):
    pass


def catch_exceptions(test_func):
    def _f(self: unittest.IsolatedAsyncioTestCase, *args):
        try:
            test_func(self, *args)
        except Exception as e:
            self.assertIsNone(e)

    return _f


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        flags.FLAGS.mark_as_parsed()
        logging.use_absl_handler()
        logging.set_verbosity(logging.DEBUG)
        logging.get_absl_handler().activate_python_handler()
        logging.get_absl_handler().python_handler.stream = sys.stderr
        if xmessagebus.loop_thread.stopped:
            xmessagebus.reinit()
        logging.info(f'loop_thread: {xmessagebus.loop_thread}')

    async def asyncTearDown(self) -> None:
        pass
        # logging.info('shutting down')
        await xmessagebus.shutdown()

    # @catch_exceptions
    async def test_send_msg_mainbus(self):
        _ = self
        xmessagebus.mainbus.publish('Testing', 'Test message')
        print('msg sent')
        await asyncio.sleep(1)

    # @catch_exceptions
    async def test_direct_bus(self):
        seq = []
        # event = threading.Event()
        event = asyncio.Event()
        main_thread = threading.current_thread()

        # timer = threading.Thread(target=timeout)
        # timer.start()
        xmessagebus.subscribe_event('testing|bus_1|event_1', lambda msg: (
            # self.assertEqual(main_thread, threading.current_thread()),
            print('main_thread', main_thread),
            print('current_thread', threading.current_thread()),
            # self.assertEqual('test msg', msg),
            seq.append(1),
            seq.append(msg),
            event.set()))
        bus = xmessagebus.get_bus('testing|bus_1')

        bus.publish('event_1', 'test msg')
        # event.wait()
        logging.info('Waiting event...')
        await event.wait()
        # ret = timer.join()
        self.assertEqual([1, 'test msg'], seq)

    # @catch_exceptions
    async def test_subscribe_w_async_func(self):
        seq = []
        # event = threading.Event()
        event = asyncio.Event()
        main_thread = threading.current_thread()

        # timer = threading.Thread(target=timeout)
        # timer.start()
        async def _callback(msg):
            # self.assertEqual(main_thread, threading.current_thread())
            print('main_thread', main_thread)
            print('current_thread', threading.current_thread())
            # self.assertEqual('test msg', msg)
            seq.append(1)
            seq.append(msg)
            event.set()

        xmessagebus.subscribe_event('testing|bus_1|event_1', _callback)
        bus = xmessagebus.get_bus('testing|bus_1')

        bus.publish('event_1', 'test msg')
        # event.wait()
        logging.info('Waiting event...')
        await event.wait()
        # ret = timer.join()
        self.assertEqual([1, 'test msg'], seq)

    async def test_unsubscribe(self):
        bus = xmessagebus.get_bus('testing|bus_1|event_1')
        def _dummy_callback(msg):
            print('dummy_callback', msg)
        xmessagebus.subscribe_event(
            'testing|bus_1|event_1', _dummy_callback)
        self.assertEqual(1, len(bus.subscribers))
        xmessagebus.unsubscribe_event(
            'testing|bus_1|event_1', _dummy_callback)
        self.assertEqual(0, len(bus.subscribers))


if __name__ == '__main__':
    unittest.main()

print('tests finished')
