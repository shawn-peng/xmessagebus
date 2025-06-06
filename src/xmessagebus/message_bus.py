import asyncio
from absl import logging
from absl import app
from enum import Enum
from collections import deque
# from wrapt import synchronized
from threading import RLock
from copy import deepcopy

from xasyncio import *
import atexit
import sys


# formatter = logging.PythonFormatter("[%(levelname)s] %(asctime)s %(filename)s:%(lineno)d %(message)s")
# logging.get_absl_handler().setFormatter(formatter)

# logging.get_absl_handler().python_handler.stream = sys.stdout

# mainloop = asyncio.new_event_loop()
# mainloop.set_debug(True)
loop_thread = ThreadedEventLoop('bus')
mainloop = loop_thread.loop  # when stopping, call loop_thread.stop()

sources = {}


# def register_event(name):
#     if name in sources:
#         raise KeyError('Duplicated event %s' % name)


class EventHandleStatus(Enum):
    OK = 0  # Event handle ok continue to call next handler
    ERROR = 1  # Error happened but continue to call next handler
    CONSUMED = 2  # Event handle ok, but was consumed, don't call later callbacks
    ABORT = 3  # Error happened, don't call later callbacks


class Dispatcher:
    def __init__(self):
        self.handlers = []

    def register(self, callback, dataargs):
        self.handlers.append((callback, dataargs))

    def dispatch(self, *args):
        i = 0
        for f, dataargs in self.handlers:
            ret = f(*args, *dataargs)
            i += 1
            if ret in [EventHandleStatus.CONSUMED, EventHandleStatus.ABORT]:
                break
        return i

    def __len__(self):
        return len(self.handlers)


class Monitor:
    def __init__(self):
        self.handlers = []

    def register(self, callback, dataargs):
        self.handlers.append((callback, dataargs))

    def notify(self, event, *args):
        i = 0
        for f, dataargs in self.handlers:
            ret = f(event, *args, *dataargs)
            i += 1
            if ret in [EventHandleStatus.CONSUMED, EventHandleStatus.ABORT]:
                break
        return i

    def __len__(self):
        return len(self.handlers)


asyncio_loop_lock = RLock()


class Bus:
    buses = {}
    def __init__(self, name='root'):
        self.name = name
        self.routers = {}
        self.dispatcher = Dispatcher()
        self.monitor = Monitor()
        self.event_queue = asyncio.Queue()
        # self.lock = RLock()
        self.running = False
        self.dilim = '|'
        self.task = None
        self.start()

    def get_bus(self, name):
        space, name = self.split_channel(name)
        if name:
            return self.routers[space].get_bus(name)
        return self.routers[space]

    def split_channel(self, event: str):
        l = event.split(self.dilim, 1)
        if len(l) == 1:
            return event, ''
        return tuple(l)

    def _push_queue_event(self, event, args):
        mainloop.create_task(self.event_queue.put((event, args)))
        # loop_thread.call_async(self.event_queue.put, (event, args))

    def publish(self, event: str, *args):
        """
        publish an event with arguments to the event queue, this is thread safe
        :param event: str, name of event (structured with channels delimited by '|', e.g. log|ui)
        :param args: argument to be pass to call back
        :return:
        """

        # args = deepcopy(args)

        # mainloop.call_soon_threadsafe(_push_queue_event, event, args)
        self._push_queue_event(event, args)

        space, event = self.split_channel(event)
        if not space or space not in self.routers:
            # skip
            return

        self.routers[space].publish(event, *args)

    def subscribe(self, event: str, callback, dataargs):
        """
        if event == '', will subscribe to all event on this bus
        callback need to be thread safe, and finish fast
        """
        if event == '':
            return self.dispatcher.register(callback, dataargs)

        space, event = self.split_channel(event)
        # space, event = event.split(self.dilim, 1)
        if space not in self.routers:
            self.routers[space] = Bus(space)

        return self.routers[space].subscribe(event, callback, dataargs)

    def observe(self, category, callback, dataargs):
        if category == '':
            return self.monitor.register(callback, dataargs)

        # space, category = category.split(self.dilim, 1)
        space, category = self.split_channel(category)
        if space not in self.routers:
            self.routers[space] = Bus(space)

        return self.routers[space].observe(category, callback, dataargs)

    async def run(self):
        self.running = True
        while self.running:
            # events = []
            # while not self.event_queue.empty():
            #     self.event_queue.get_nowait()
            event, args = await self.event_queue.get()
            if event == '':
                self.dispatcher.dispatch(*args)
            # self.monitor.notify('|'.join((self.name, event)), args)
            self.monitor.notify(event, *args)

    def start(self):
        def _start():
            self.task = mainloop.create_task(self.run())

        mainloop.call_soon_threadsafe(_start)

    def stop(self):
        logging.info(f'stopping bus ({self.name})')
        for bus in self.routers.values():
            bus.stop()
        loop_thread.call_sync(self.task.cancel)
        logging.info(f'bus ({self.name}) stopped')

    def __repr__(self):
        return f'<Bus: {self.name}> (Subscribed: {len(self.dispatcher)}, Observing: {len(self.monitor)})'


mainbus = Bus()


def publish_event(event, *args):
    # mainbus.publish(event, args)
    mainloop.call_soon_threadsafe(mainbus.publish, event, *args)


def subscribe_event(event, callback, *dataargs):
    return mainbus.subscribe(event, callback, dataargs)


def observe_bus(category, callback, *dataargs):
    return mainbus.observe(category, callback, dataargs)


def get_bus(name):
    return mainbus.get_bus(name)


def shutdown():
    global loop_thread, mainbus
    if not loop_thread:
        logging.info('loop already stopped')
        return
    logging.info('shutting down xmessagebus module')
    mainbus.stop()
    mainbus = None
    loop_thread.stop()
    loop_thread = None
    # pass
    # mainloop.stop()
    # mainloop


# def main():
#     # global mainloop
#     logging.info('message_bus running')
#
#     # cross_thread_call(mainloop, run)
#     # mainloop
#     # asyncio.ensure_future(mainbus.run())
#     # mainloop.create_task(mainbus.run())
#     mainloop.run_forever()
#     logging.info('message_bus finished')


# def stop():
#     logging.info('stopping message_bus')
#     mainloop.call_soon_threadsafe(shutdown)

def cleanup():
    # Do cleanup actions here
    if sys.stderr != logging.get_absl_handler().python_handler.stream:
        print(sys.stderr)
        print(logging.get_absl_handler().python_handler.stream)
        logging.get_absl_handler().python_handler.stream = sys.stderr
    if not sys.stderr.closed:
        logging.info("Module cleanup")
    shutdown()
    # loop_thread.stop()


logging.info("register cleanup")
atexit.register(cleanup)
