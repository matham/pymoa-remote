"""Multiprocessing Socket Executor
==================================

"""
from trio import socket
import trio
import os
import sys
from typing import Optional
import time

from pymoa_remote.socket.client import SocketExecutor

__all__ = ('MultiprocessSocketExecutor', )


class MultiprocessSocketExecutor(SocketExecutor):
    """Executor that sends all requests to a remote server to be executed
    there, using a websocket.
    """

    _process: Optional[trio.Process] = None

    server: str = ''

    port: int = None

    stream_changes = True

    allow_remote_class_registration = True

    allow_import_from_main = False

    def __init__(
            self, server: str = 'localhost', port: int = 0,
            stream_changes=True,
            allow_remote_class_registration=True,
            allow_import_from_main=False, **kwargs):
        super(MultiprocessSocketExecutor, self).__init__(**kwargs)
        self.server = server
        self.port = port
        self.stream_changes = stream_changes
        self.allow_remote_class_registration = allow_remote_class_registration
        self.allow_import_from_main = allow_import_from_main

    async def decode(self, data):
        raise NotImplementedError

    async def start_executor(self):
        if self._process is not None:
            raise TypeError('Executor already started')

        port = self.port
        if not port:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            await s.bind((self.server, 0))
            s.listen(1)
            port = self.port = s.getsockname()[1]
            s.close()

        env = os.environ.copy()
        env['KIVY_NO_ARGS'] = '1'
        self._process = await trio.lowlevel.open_process(
            [sys.executable, '-m', 'pymoa_remote.app.multiprocessing',
             '--host', str(self.server),
             '--port', str(port),
             '--stream_changes', str(self.stream_changes),
             '--remote_class_registration',
             str(self.allow_remote_class_registration),
             '--import_from_main', str(self.allow_import_from_main)],
            env=env)

        # wait until the process is ready
        ts = time.perf_counter()
        while True:
            try:
                async with self.create_socket_context():
                    break
            except OSError:
                if time.perf_counter() - ts >= 5:
                    raise
                await trio.sleep(.01)

        await super(MultiprocessSocketExecutor, self).start_executor()

    async def stop_executor(self, block=True):
        try:
            await super(MultiprocessSocketExecutor, self).stop_executor(
                block=block)
        finally:
            if self._process is None:
                return

            with trio.CancelScope(shield=True):
                # todo: handle here and for quart in case process is dead
                try:
                    data = self.encode({'eof': True})
                    async with self.create_socket_context() as sock:
                        await self.write_socket(data, sock)

                finally:
                    with trio.CancelScope(shield=True):
                        await self._process.wait()
                        self._process = None
