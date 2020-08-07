"""Socket Client
================

"""
from typing import AsyncGenerator, Tuple, AsyncContextManager, Optional, \
    Union, Callable, Any, List, Iterable
import time
from async_generator import aclosing
import contextlib
import trio
from trio import socket, SocketStream, TASK_STATUS_IGNORED, open_tcp_stream
from tree_config import apply_config

from pymoa_remote.client import Executor
from pymoa_remote.executor import NO_CALLBACK
from pymoa_remote.exception import raise_remote_exception_from_frames

__all__ = ('SocketExecutor', )


class SocketExecutor(Executor):
    """Executor that sends all requests to a remote server to be executed
    there, using a socket.

    Each request is sent as a dict with metadata. Typically, it'll have a
    ``data`` key that contains the request data dict. This data is generated
    by the corresponding private methods in
    :class:`~pymoa_remote.client.Executor`..

    The socket is opened with :meth:`create_socket_context` or
    :meth:`open_socket`. Data is written with :meth:`write_socket` and
    encoded with :meth:`encode`. It is read and decoded with
    :meth:`read_decode_json_buffers`. All client requests use this basic API.
    """

    server: str = ''

    port: int = None

    socket: Optional[SocketStream] = None

    _packet: int = 0

    _limiter: Optional[trio.Lock]

    def __init__(self, server: str = '', port: int = 0, **kwargs):
        super(SocketExecutor, self).__init__(**kwargs)
        self.server = server
        self.port = port

    @contextlib.asynccontextmanager
    async def _create_socket_context(self) -> AsyncContextManager[SocketStream]:
        sock = await open_tcp_stream(self.server, self.port)
        try:
            yield sock
        finally:
            await sock.aclose()

    def create_socket_context(self) -> AsyncContextManager:
        return self._create_socket_context()

    async def open_socket(self) -> SocketStream:
        """Opens socket, sends channel=None, reads the response and returns
        the socket stream.

        :return:
        """
        data = self.encode({'stream': None})

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (self.server, self.port)
        await sock.connect(server_address)

        stream = SocketStream(sock)
        try:
            await self.write_socket(data, stream)
            await self.read_decode_json_buffers(stream)
        except Exception:
            await stream.aclose()
            raise

        return stream

    def encode(self, data) -> bytes:
        return self.registry.encode_json_buffers(data)

    async def decode(self, data):
        raise NotImplementedError

    async def write_socket(self, data: bytes, sock: SocketStream):
        await sock.send_all(data)

    async def read_decode_json_buffers(self, stream: SocketStream):
        header = b''
        while len(header) < 4 * 4:
            header += await stream.receive_some(16 - len(header))

        msg_len, json_bytes, num_buffers = \
            self.registry.decode_json_buffers_header(header)

        data = []
        n = 0
        while n < msg_len:
            buff = await stream.receive_some(msg_len - n)
            data.append(buff)
            n += len(buff)

        if len(data) == 1:
            data = data[0]
        else:
            data = b''.join(data)

        return self.registry.decode_json_buffers(data, json_bytes, num_buffers)

    def raise_return_value(self, data: dict, packet: int = None):
        exception = data.get('exception', None)
        if exception is not None:
            raise_remote_exception_from_frames(**exception)

        # todo: implement reading errors when server fails
        if packet is not None:
            packet_ = data['packet']
            if packet_ != packet:
                raise ValueError(
                    f"Packet mismatch when reading: {packet} is not {packet_}")

    async def _vanilla_write_read(self, cmd: str, data: dict):
        packet = self._packet
        self._packet += 1
        data = {
            'data': data,
            'cmd': cmd,
            'packet': packet,
        }
        data = self.encode(data)

        await self.write_socket(data, self.socket)
        res = await self.read_decode_json_buffers(self.socket)
        self.raise_return_value(res, packet)

        return res

    async def remote_import(self, module):
        data = self._get_remote_import_data(module)
        await self._vanilla_write_read('remote_import', data)

    async def register_remote_class(self, cls):
        data = self._get_register_remote_class_data(cls)
        await self._vanilla_write_read('register_remote_class', data)

    async def ensure_remote_instance(
            self, obj, hash_name, *args, auto_register_class=True, **kwargs):
        cls = obj.__class__
        if auto_register_class and not self.registry.is_class_registered(
                class_to_register=cls):
            self.registry.register_class(cls)

        self.registry.add_instance(obj, hash_name)

        data = self._get_ensure_remote_instance_data(
            obj, args, kwargs, hash_name, auto_register_class)
        await self._vanilla_write_read('ensure_remote_instance', data)

    async def delete_remote_instance(self, obj):
        await self._vanilla_write_read(
            'delete_remote_instance',
            self._get_delete_remote_instance_data(obj)
        )

        self.registry.delete_instance(obj)

    async def start_executor(self):
        self.socket = await self.open_socket()
        self._limiter = trio.Lock()

    async def stop_executor(self, block=True):
        if self.socket is not None:
            await self.socket.aclose()
            self.socket = None
        self._limiter = None

    async def execute(
            self, obj, fn: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None):
        data = self._get_execute_data(obj, fn, args, kwargs, callback)

        async with self._limiter:
            res = await self._vanilla_write_read('execute', data)

            ret_val = res['data']
            if callback is not NO_CALLBACK:
                self.call_execute_callback(obj, ret_val, callback)
        return ret_val

    async def execute_generator(
            self, obj, gen: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None,
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        read = self.read_decode_json_buffers
        write = self.write_socket
        raise_return_value = self.raise_return_value
        callback = self.get_execute_callback_func(obj, callback)
        call_callback = self.call_execute_callback_func

        packet = self._packet
        self._packet += 1
        data = {
            'data': self._get_execute_data(obj, gen, args, kwargs, callback),
            'cmd': 'execute_generator',
            'packet': packet,
        }
        data = self.encode(data)

        header = self.encode({})
        async with self._limiter:
            # it needs to be its own socket so that we can close it from our
            # side and stop reading
            async with self.create_socket_context() as sock:
                await write(header, sock)
                await read(sock)

                await write(data, sock)
                task_status.started()
                while True:
                    res = await read(sock)
                    raise_return_value(res, packet)

                    if res['done_execute']:
                        return
                    ret_val = res['data']
                    call_callback(ret_val, callback)
                    yield ret_val

    async def get_remote_objects(self):
        res = await self._vanilla_write_read(
            'get_remote_objects', self._get_remote_objects_data())
        return res['data']

    async def get_remote_object_config(self, obj: Optional[Any]):
        res = await self._vanilla_write_read(
            'get_remote_object_config',
            self._get_remote_object_config_data(obj)
        )
        return res['data']

    async def apply_config_from_remote(self, obj):
        config = await self.get_remote_object_config(obj)
        apply_config(obj, config)

    async def get_remote_object_property_data(
            self, obj: Any, properties: List[str]) -> dict:
        res = await self._vanilla_write_read(
            'get_remote_object_property_data',
            self._get_remote_object_property_data_data(obj, properties)
        )
        return res['data']

    async def apply_property_data_from_remote(
            self, obj: Any, properties: List[str]):
        props = await self.get_remote_object_property_data(obj, properties)
        for key, value in props.items():
            setattr(obj, key, value)

    async def _generate_stream_events(self, data, task_status):
        read = self.read_decode_json_buffers
        data = self.encode(data)

        async with self.create_socket_context() as stream:
            await self.write_socket(data, stream)
            await read(stream)
            task_status.started()

            last_packet = None
            while True:
                res = await read(stream)

                exception = res.get('exception', None)
                if exception is not None:
                    raise_remote_exception_from_frames(**exception)

                packet = res['packet']
                if last_packet is not None and last_packet + 1 != packet:
                    raise ValueError(
                        f'Packets were skipped {last_packet} -> {packet}')
                last_packet = packet

                raw_res = res['data']
                assert type(raw_res) == bytes
                yield self.registry.decode_json_buffers_raw(raw_res)

    @contextlib.asynccontextmanager
    async def get_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            initial_properties: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        data = self._get_remote_object_data_data(
            obj, trigger_names, triggered_logged_names, logged_names,
            initial_properties)
        data = {'stream': 'data', 'data': data}

        async with aclosing(self._generate_stream_events(
                data, task_status)) as aiter:
            yield aiter

    async def apply_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            initial_properties: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED):
        data = self._get_remote_object_data_data(
            obj, trigger_names, triggered_logged_names, logged_names,
            initial_properties)
        data = {'stream': 'data', 'data': data}

        await self._apply_data_from_remote(
            obj, self._generate_stream_events(data, task_status))

    @contextlib.asynccontextmanager
    async def get_channel_from_remote(
            self, obj: Optional[Any], channel: str,
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        data = self._get_remote_object_channel_data(obj, channel)
        data = {'stream': channel, 'data': data}

        async with aclosing(self._generate_stream_events(
                data, task_status)) as aiter:
            yield aiter

    async def apply_execute_from_remote(
            self, obj, exclude_self=True, task_status=TASK_STATUS_IGNORED):
        data = self._get_remote_object_channel_data(obj, 'execute')
        data = {'stream': 'execute', 'data': data}

        await self._apply_execute_from_remote(
            obj, self._generate_stream_events(data, task_status), exclude_self)

    async def get_echo_clock(self) -> Tuple[int, int, int]:
        start_time = time.perf_counter_ns()
        res = await self._vanilla_write_read(
            'get_echo_clock', self._get_clock_data())

        return start_time, res['data']['server_time'], time.perf_counter_ns()
