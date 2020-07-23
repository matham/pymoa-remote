"""Utilities
===============

Module that provides helpful classes and functions.
"""
import trio
import math

__all__ = (
    'get_class_bases', 'MaxSizeSkipDeque')


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
    [ackets were skipped.
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
