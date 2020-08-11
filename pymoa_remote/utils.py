"""Utilities
===============

Module that provides helpful classes and functions.
"""
import trio
import math
from threading import get_ident, Lock
from collections import deque
import contextlib
import sys
from functools import wraps

__all__ = (
    'QueueFull', 'get_class_bases', 'MaxSizeSkipDeque', 'MaxSizeErrorDeque',
    'asynccontextmanager')


class QueueFull(Exception):
    pass


def get_class_bases(cls):
    """Gets all the base-classes of the class.

    :param cls:
    :return:
    """
    for base in cls.__bases__:
        if base.__name__ == 'object':
            break
        for cbase in get_class_bases(base):
            yield cbase
        yield base


class MaxSizeSkipDeque:
    """Async queue that skips appends when full, but indicates to consumer that
    packets were skipped.
    """

    send_channel: trio.MemorySendChannel = None

    receive_channel: trio.MemoryReceiveChannel = None

    size: int = 0

    packet: int = 0

    max_size = 0

    def __init__(self, max_size=0, **kwargs):
        super(MaxSizeSkipDeque, self).__init__(**kwargs)
        self.send_channel, self.receive_channel = trio.open_memory_channel(
            math.inf)
        self.max_size = max_size

    def __aiter__(self):
        return self

    async def __anext__(self):
        item, packet, size = await self.receive_channel.receive()
        self.size -= size
        return item, packet

    def add_item(self, item, size=1, force=False):
        if not force and self.max_size and self.size + size > self.max_size:
            self.packet += 1
            return

        self.size += size
        self.packet += 1
        self.send_channel.send_nowait((item, self.packet, size))


class MaxSizeErrorDeque:
    """Async queue that raises an error on the read side as soon as the queue
    is full and the queue will silently stop taking items.

    It is thread safe.
    """

    send_channel: trio.MemorySendChannel = None

    receive_channel: trio.MemoryReceiveChannel = None

    size: int = 0

    max_size: int = 0
    """Approximate maximum size (maybe over with multiple threads).
    """

    size_lock: Lock = None

    thread_id = None

    queue: deque = None

    token: trio.lowlevel.TrioToken = None

    stopped: bool = False

    def __init__(self, max_size=0, **kwargs):
        super().__init__(**kwargs)
        self.send_channel, self.receive_channel = trio.open_memory_channel(1)
        self.max_size = max_size
        self.size_lock = Lock()
        self.thread_id = get_ident()
        self.queue = deque()
        self.token = trio.lowlevel.current_trio_token()

    def __aiter__(self):
        return self

    async def __anext__(self):
        queue = self.queue
        while True:
            # first get any items from the queue. We could have already read
            # the notification if multiple adds, so we can't wait first.

            # add/setting stopped always first adds then notifies
            if self.stopped:
                raise QueueFull

            try:
                item, size = queue.popleft()
                # if there was something, return it
                break
            except IndexError:
                pass
            # otherwise, wait for the item to be added
            await self.receive_channel.receive()

        with self.size_lock:
            self.size -= size
        return item

    def _send_nowait(self):
        try:
            self.send_channel.send_nowait(None)
        except trio.WouldBlock:
            pass

    def add_item(self, item, size=1):
        if self.stopped:
            return

        if self.max_size and self.size + size > self.max_size:
            self.stopped = True

            if self.thread_id == get_ident():
                self._send_nowait()
            else:
                self.token.run_sync_soon(self._send_nowait)
            return

        with self.size_lock:
            self.size += size
        self.queue.append((item, size))

        if self.thread_id == get_ident():
            self._send_nowait()
        else:
            self.token.run_sync_soon(self._send_nowait)


# Derived from the one in contextlib
# So this is a derivative work licensed under the PSF License, which requires
# the following notice:
#
# Copyright © 2001-2020 Python Software Foundation; All Rights Reserved
class _PyMoaAsyncGeneratorContextManager(
    contextlib._AsyncGeneratorContextManager):

    async def __aexit__(self, typ, value, traceback):
        if typ is None:
            try:
                await self.gen.__anext__()
            except StopAsyncIteration:
                return
            else:
                raise RuntimeError("generator didn't stop")
        else:
            if value is None:
                value = typ()
            # See _GeneratorContextManager.__exit__ for comments on subtleties
            # in this implementation
            try:
                await self.gen.athrow(typ, value, traceback)
                if not isinstance(value, GeneratorExit):
                    raise RuntimeError("generator didn't stop after throw()")
            except StopAsyncIteration as exc:
                return exc is not value
            except RuntimeError as exc:
                if exc is value:
                    return False
                # Avoid suppressing if a StopIteration exception
                # was passed to throw() and later wrapped into a RuntimeError
                # (see PEP 479 for sync generators; async generators also
                # have this behavior). But do this only if the exception
                # wrapped by the RuntimeError is actully Stop(Async)Iteration
                # (see issue29692).
                if isinstance(value, (StopIteration, StopAsyncIteration)):
                    if exc.__cause__ is value:
                        return False
                raise
            except BaseException as exc:
                if exc is not value:
                    raise


if sys.version_info[:2] >= (3, 8):
    asynccontextmanager = contextlib.asynccontextmanager
else:
    def asynccontextmanager(func):
        """@asynccontextmanager decorator, the same as
        ``contextlib.asynccontextmanager``, but with with
        https://bugs.python.org/issue33786 fixed for 3.7.
        """
        @wraps(func)
        def helper(*args, **kwds):
            return _PyMoaAsyncGeneratorContextManager(func, args, kwds)
        return helper
