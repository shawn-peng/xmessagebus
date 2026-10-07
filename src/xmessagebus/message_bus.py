import asyncio
import threading
from dataclasses import field
from functools import wraps

from typing import *

from absl import logging
from absl import app
from enum import Enum
from collections import deque, defaultdict
# from wrapt import synchronized
from threading import RLock
from copy import deepcopy
import dataclasses

import xasyncio
from xasyncio import *
import atexit
import sys

# formatter = logging.PythonFormatter("[%(levelname)s] %(asctime)s %(filename)s:%(lineno)d %(message)s")
# logging.get_absl_handler().setFormatter(formatter)

# logging.get_absl_handler().python_handler.stream = sys.stdout

# mainloop = asyncio.new_event_loop()
# mainloop.set_debug(True)
loop_thread = xasyncio.AsyncThread('message_bus_thread')

logging.info(sys.version)
logging.info(sys.version_info)

try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
loop.run_until_complete(loop_thread.__aenter__())
# mainloop = loop_thread.loop

sources = {}


def ensure_running(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.running:
            raise RuntimeError(f'MessageBus({self.name}) is not running')
        return method(self, *args, **kwargs)

    return wrapper


def current_loop():
    return hex(id(asyncio.get_event_loop()))


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
    # queue: asyncio.Queue = asyncio.Queue()
    queue: AsyncQueue = field(default_factory=AsyncQueue)

    def __post_init__(self):
        self.owner_loop = self.owner_async_thread.loop
        # if self.queue.loop is not self.owner_loop:
        if self.owner_loop is not xasyncio.current_async_thread().loop:
            raise RuntimeError('initialized in wrong loop')
        if self.queue.async_thread.loop is not self.owner_loop:
            raise RuntimeError('queue initialized in wrong loop')
        self.listen()

    async def enqueue(self, args):
        """Need to be thread-safe"""

        # assert threading.current_thread() == self.owner_async_thread
        async def _enqueue():
            if asyncio.get_event_loop() is not self.owner_loop:
                raise RuntimeError('called in wrong loop')
            await self.queue.put(args)
            MessageBus.logging(logging.DEBUG,
                               f'put on subscriber({hex(id(self.queue.loop))}) '
                               f'queue, {args}')

        MessageBus.logging(logging.DEBUG,
                           f'current thread is {threading.current_thread()}')
        MessageBus.logging(logging.DEBUG,
                           f'will enqueue in thread {self.owner_async_thread}')
        MessageBus.logging(logging.DEBUG, f'async_thread loop is '
                                          f'{hex(id(self.owner_async_thread.loop))}')
        # self.owner_async_thread.call_sync(_enqueue)
        # self.owner_async_thread.await_coroutine(_enqueue())
        # await self.owner_async_thread.run_coroutine(_enqueue())
        await self.owner_async_thread.run_coroutine(_enqueue())

    def listen(self):
        """Non-blocking"""
        self._assert_thread()
        # asyncio.ensure_future(self._watch_queue())
        asyncio.ensure_future(
            self.owner_async_thread.run_coroutine(self._watch_queue()))
        # self.owner_async_thread.call_async(self._watch_queue)

    def _assert_thread(self):
        if isinstance(self.owner_async_thread, xasyncio.AsyncThread):
            MessageBus.logging(logging.DEBUG, 'listening in AsyncThread')
            assert threading.current_thread() == self.owner_async_thread
        elif isinstance(self.owner_async_thread, xasyncio.AsyncedThread):
            MessageBus.logging(logging.DEBUG, 'listening in AsyncedThread')
            assert threading.current_thread() == self.owner_async_thread.thread
        else:
            raise TypeError('Unsupported type', type(self.owner_async_thread))

    async def _watch_queue(self):
        """Blocking watch"""
        self._assert_thread()
        MessageBus.logging(logging.DEBUG, 'subscriber start listening')
        while True:
            # Listening to self queue
            MessageBus.logging(logging.DEBUG,
                               f'waiting queue in thread {threading.current_thread()}')
            MessageBus.logging(logging.DEBUG,
                               f'in loop {hex(id(asyncio.get_event_loop()))}')
            if asyncio.get_event_loop() is not self.owner_async_thread.loop:
                raise RuntimeError('called in wrong loop')
            event = await self.queue.get()
            if event is None:
                break
            if asyncio.iscoroutinefunction(self.callback):
                await self.callback(*event, *self.dataargs)
            else:
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
        # self.event_queue = asyncio.Queue()
        self.event_queue: AsyncQueue | None = None
        # self.dispatchers = []
        self.subscribers = []
        self.monitors = []
        # self.lock = RLock()
        self.running = False
        self.dilim = '|'
        self.task = None
        logging.info(f'MessageBus({self.name}) created')
        self.start()

    def in_thread_init(self):
        self.event_queue = AsyncQueue()

    @ensure_running
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

    async def _push_queue_event(self, event, args):
        # This is also thread safe
        # mainloop.create_task(self.event_queue.put((event, args)))
        self.logging(logging.DEBUG, f'putting {event} on bus...')
        # loop_thread.call_sync(self.event_queue.put, (event, args))
        # loop_thread.run_coroutine(self.event_queue.put((event, args)))
        await self.event_queue.put((event, args))
        self.logging(logging.DEBUG, f'putted {event} on bus')

    @ensure_running
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
                thread = xasyncio.current_async_thread()
                self.subscribers.append(Subscriber(callback, dataargs, thread))
                return None

            space, event = self.split_channel(event)
            # space, event = event.split(self.dilim, 1)
            if space not in self.routers:
                self.routers[space] = MessageBus(space)

            ret = self.routers[space].subscribe(event, callback, *dataargs)
        return ret

    @ensure_running
    def unsubscribe(self, event: str, callback):
        with self.lock:
            if event == '':
                for i in range(len(self.subscribers)):
                    if callback == self.subscribers[i].callback:
                        dataargs = self.subscribers[i].dataargs
                        del self.subscribers[i]
                        self.logging(logging.DEBUG, "Event unsubscribed")
                        return dataargs
                raise ValueError(f'callback {callback} not found on the bus')

            space, event = self.split_channel(event)
            if space not in self.routers:
                raise KeyError(f'{space} not in routers')

            return self.routers[space].unsubscribe(event, callback)

    @ensure_running
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
        # self._push_queue_event(event, args)
        loop_thread.ensure_coroutine(self._push_queue_event(event, args))

        space, event = self.split_channel(event)
        if not space or space not in self.routers:
            # skip
            return

        self.routers[space].publish(event, *args)

    @ensure_running
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
        self.in_thread_init()

        self.running = True
        logging.info(f'MessageBus({self}) running...')
        try:
            while self.running:
                # events = []
                # while not self.event_queue.empty():
                #     self.event_queue.get_nowait()
                event, args = await self.event_queue.get()
                if event == '':
                    # Send event to queues of subscribers
                    for subscriber in self.subscribers:
                        await subscriber.enqueue(args)
        finally:
            self.running = False

    def start(self):
        self.running = True

        def _start():
            if not self.running:
                return
            # self.task = mainloop.create_task(self.run())
            self.task = loop_thread.ensure_coroutine(self.run())
            self.logging(logging.DEBUG,
                         f'{self} start running, task: {self.task}')

        # mainloop.call_soon_threadsafe(_start)
        loop_thread.async_call(_start)
        # loop_thread.call_async(self.run)

    async def stop(self):
        logging.info(f'stopping bus ({self.name})')
        self.running = False
        print('made to this far 1')
        for bus in self.routers.values():
            await bus.stop()
        print('made to this far 2')
        if self.task:
            await loop_thread.sync_call(self.task.cancel)
            self.task = None
        print('made to this far 3')
        logging.info(f'bus ({self.name}) stopped')
        print('made to this far 4')

    def __repr__(self):
        # return f'<MessageBus: {self.name}> (Subscribed: {len(self.dispatcher)}, Observing: {len(self.monitor)})'
        return f'<MessageBus: {self.name}> (Subscribed: {len(self.subscribers)}, Observing: {len(self.monitors)})'


mainbus = MessageBus()


def reinit():
    global loop_thread, mainbus
    loop_thread = AsyncThread('message_bus_thread')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop_thread.__aenter__())
    # mainloop = loop_thread.loop  # when stopping, call loop_thread.stop()
    mainbus = MessageBus()


def publish_event(event, *args):
    # mainbus.publish(event, args)
    # mainloop.call_soon_threadsafe(mainbus.publish, event, *args)
    loop_thread.async_call(mainbus.publish, event, *args)


def subscribe_event(event, callback, *dataargs):
    return mainbus.subscribe(event, callback, *dataargs)


def unsubscribe_event(event, callback):
    return mainbus.unsubscribe(event, callback)


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
    # mainbus = None
    await loop_thread.__aexit__(*sys.exc_info())
    logging.info(f'loop_thread {loop_thread} stopped')
    # loop_thread = None
    # mainloop = None
    # pass
    # mainloop.stop()


def _shutdown():
    print('_shutdown called')
    loop.run_until_complete(shutdown())


atexit.register(_shutdown)

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
