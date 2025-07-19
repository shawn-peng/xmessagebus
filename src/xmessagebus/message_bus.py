import asyncio
import threading

from typing import *

import xasyncio
from absl import logging
from absl import app
from enum import Enum
from collections import deque, defaultdict
# from wrapt import synchronized
from threading import RLock
from copy import deepcopy
import dataclasses

from xasyncio import *
import atexit
import sys

# formatter = logging.PythonFormatter("[%(levelname)s] %(asctime)s %(filename)s:%(lineno)d %(message)s")
# logging.get_absl_handler().setFormatter(formatter)

# logging.get_absl_handler().python_handler.stream = sys.stdout

# mainloop = asyncio.new_event_loop()
# mainloop.set_debug(True)
loop_thread = AsyncThread('message_bus_thread')
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


@dataclasses.dataclass
class Subscriber:
    callback: Callable
    dataargs: Tuple
    owner_async_thread: xasyncio.AsyncThread | xasyncio.AsyncedThread
    queue: asyncio.Queue = asyncio.Queue()

    def __post_init__(self):
        self.listen()

    def enqueue(self, args):
        """Need to be thread-safe"""
        # assert threading.current_thread() == self.owner_async_thread
        async def _enqueue():
            await self.queue.put(args)
            logging.debug(f'put on subscriber({self.owner_async_thread}) queue, {args}')

        logging.debug(f'enqueue in thread {self.owner_async_thread}')
        # self.owner_async_thread.call_sync(_enqueue)
        self.owner_async_thread.await_coroutine(_enqueue())

    def listen(self):
        """Non-blocking"""
        self._assert_thread()
        asyncio.ensure_future(self._watch_queue())
        # self.owner_async_thread.call_async(self._watch_queue)

    def _assert_thread(self):
        if isinstance(self.owner_async_thread, xasyncio.AsyncThread):
            logging.debug('listening in AsyncThread')
            assert threading.current_thread() == self.owner_async_thread
        elif isinstance(self.owner_async_thread, xasyncio.AsyncedThread):
            logging.debug('listening in AsyncedThread')
            assert threading.current_thread() == self.owner_async_thread.thread
        else:
            raise TypeError('Unsupported type', type(self.owner_async_thread))

    async def _watch_queue(self):
        """Blocking watch"""
        self._assert_thread()
        logging.debug('subscriber start listening')
        while True:
            # Listening to self queue
            event = await self.queue.get()
            if event is None:
                break
            self.callback(*event, *self.dataargs)


@dataclasses.dataclass
class Dispatcher:
    callback: Callable
    dataargs: Tuple
    queue: asyncio.Queue = asyncio.Queue()

    # def register(self, callback, dataargs):
    #     # self.handlers.append((callback, dataargs))
    #     self.handlers.append
    #
    # def dispatch(self, *args):
    #     i = 0
    #     for f, dataargs in self.handlers:
    #         ret = f(*args, *dataargs)
    #         i += 1
    #         if ret in [EventHandleStatus.CONSUMED, EventHandleStatus.ABORT]:
    #             break
    #     return i

    # def __len__(self):
    #     return len(self.handlers)
    #     pass


# class Monitor:
#     def __init__(self):
#         self.handlers = []
#
#     def register(self, callback, dataargs):
#         self.handlers.append((callback, dataargs))
#
#     def notify(self, event, *args):
#         i = 0
#         for f, dataargs in self.handlers:
#             ret = f(event, *args, *dataargs)
#             i += 1
#             if ret in [EventHandleStatus.CONSUMED, EventHandleStatus.ABORT]:
#                 break
#         return i
#
#     def __len__(self):
#         return len(self.handlers)


asyncio_loop_lock = RLock()


class MessageBus:
    buses = {}

    def __init__(self, name='root'):
        self.name = name
        self.routers = {}
        self.lock = threading.RLock()
        # self.dispatcher = Dispatcher()
        # self.monitor = Monitor()
        self.event_queue = asyncio.Queue()
        # self.dispatchers = []
        self.subscribers = []
        self.monitors = []
        # self.lock = RLock()
        self.running = False
        self.dilim = '|'
        self.task = None
        logging.info(f'MessageBus({self.name}) created')
        self.start()

    def get_bus(self, name):
        space, name = self.split_channel(name)
        if space not in self.routers:
            self.routers[space] = MessageBus(space)
        if name:
            return self.routers[space].get_bus(name)
        return self.routers[space]

    def split_channel(self, event: str):
        l = event.split(self.dilim, 1)
        if len(l) == 1:
            return event, ''
        return tuple(l)

    @staticmethod
    def logging(level, *args):
        logging.log(level, *args)

    def _push_queue_event(self, event, args):
        # This is also thread safe
        # mainloop.create_task(self.event_queue.put((event, args)))
        self.logging(logging.DEBUG, f'putting {event} on bus...')
        # loop_thread.call_sync(self.event_queue.put, (event, args))
        loop_thread.await_coroutine(self.event_queue.put((event, args)))
        self.logging(logging.DEBUG, f'putted {event} on bus')

    def subscribe(self, event: str, callback, *dataargs):
        """
        if event == '', will subscribe to all event on this bus
        callback need to be thread safe, and finish fast
        """
        # def wrap_thread_callback(callback):
        #     origin_thread = threading.current_thread()
        #
        # wrapped_callback =
        with self.lock:
            if event == '':
                # self.dispatchers.append(Dispatcher(callback, dataargs))
                # return self.dispatcher.register(callback, dataargs)
                # The subscriber belong to the subscriber thread and shouldn't be modified in bus thread
                thread = threading.current_thread()
                if not isinstance(thread, AsyncThread):
                    thread = xasyncio.AsyncedThread(f'wrapped_for_{self.name}', thread)
                self.subscribers.append(Subscriber(callback, dataargs, thread))
                return

            space, event = self.split_channel(event)
            # space, event = event.split(self.dilim, 1)
            if space not in self.routers:
                self.routers[space] = MessageBus(space)

            ret = self.routers[space].subscribe(event, callback, *dataargs)
        return ret

    def publish(self, event: str, *args):
        """
        publish an event with arguments to the event queue, this is thread safe
        :param event: str, name of event (structured with channels delimited by '|', e.g. log|ui)
        :param args: argument to be pass to call back
        :return:
        """
        # This maybe called from other threads
        # args = deepcopy(args)

        # mainloop.call_soon_threadsafe(self._push_queue_event, event, args)
        self._push_queue_event(event, args)

        space, event = self.split_channel(event)
        if not space or space not in self.routers:
            # skip
            return

        self.routers[space].publish(event, *args)

    def monitor(self, path, callback, dataargs):
        if path == '':
            thread = threading.current_thread()
            assert isinstance(thread, AsyncThread)
            self.monitors.append(Subscriber(callback, dataargs, thread))
            return

        space, path = self.split_channel(path)
        if space not in self.routers:
            self.routers[space] = MessageBus(space)

        return self.routers[space].monitor(path, callback, dataargs)

    async def run(self):
        self.running = True
        logging.info(f'MessageBus({self}) running...')
        while self.running:
            # events = []
            # while not self.event_queue.empty():
            #     self.event_queue.get_nowait()
            event, args = await self.event_queue.get()
            if event == '':
                # Send event to queues of subscribers
                for subscriber in self.subscribers:
                    subscriber.enqueue(args)

            # logging.info(f'')
            # if event == '':
            #     self.dispatcher.dispatch(*args)
            # # self.monitor.notify('|'.join((self.name, event)), args)
            # self.monitor.notify(event, *args)

    def start(self):
        def _start():
            self.task = mainloop.create_task(self.run())
            self.logging(logging.DEBUG, f'{self} start running, task: {self.task}')

        mainloop.call_soon_threadsafe(_start)
        # loop_thread.call_async(self.run)

    async def stop(self):
        logging.info(f'stopping bus ({self.name})')
        for bus in self.routers.values():
            await bus.stop()
        await loop_thread.call_sync(self.task.cancel)
        logging.info(f'bus ({self.name}) stopped')

    def __repr__(self):
        # return f'<MessageBus: {self.name}> (Subscribed: {len(self.dispatcher)}, Observing: {len(self.monitor)})'
        return f'<MessageBus: {self.name}> (Subscribed: {len(self.subscribers)}, Observing: {len(self.monitors)})'


mainbus = MessageBus()


def publish_event(event, *args):
    # mainbus.publish(event, args)
    mainloop.call_soon_threadsafe(mainbus.publish, event, *args)


def subscribe_event(event, callback, *dataargs):
    return mainbus.subscribe(event, callback, *dataargs)


def observe_bus(category, callback, *dataargs):
    return mainbus.monitor(category, callback, dataargs)


def get_bus(name):
    return mainbus.get_bus(name)


async def shutdown():
    global loop_thread, mainbus
    if not loop_thread:
        logging.info('loop already stopped')
        return
    logging.info('shutting down xmessagebus module')
    assert isinstance(mainbus, MessageBus)
    await mainbus.stop()
    mainbus = None
    await loop_thread.stop()
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
    # if sys.stderr != logging.get_absl_handler().python_handler.stream:
    #     print(sys.stderr)
    #     print(logging.get_absl_handler().python_handler.stream)
    #     logging.get_absl_handler().python_handler.stream = sys.stderr
    if not sys.stderr.closed:
        logging.info("Module cleanup")
    # await shutdown()
    t = xasyncio.AsyncedThread('main', threading.current_thread())
    t.run_coroutine(shutdown())
    # loop_thread.stop()


# logging.info("register cleanup")
# atexit.register(cleanup)
