"""Multiprocessing app
======================

"""

from collections import defaultdict
from async_generator import aclosing
import argparse
import trio
from trio import socket, SocketStream, SocketListener, serve_listeners

from pymoa_remote.utils import MaxSizeErrorDeque
from pymoa_remote.threading import ThreadExecutor
from pymoa_remote.client import ExecutorContext
from pymoa_remote.exception import serialize_exception
from pymoa_remote.server import SimpleExecutorServer, \
    dispatch_stream_channel_to_queues

__all__ = ('run_app', 'ProcessSocketServer')

MAX_QUEUE_SIZE = 50 * 1024 * 1024


class ProcessSocketServer(SimpleExecutorServer):
    """Quart server side socket handler.
    """

    stream_clients = {}

    def __init__(self, **kwargs):
        super(ProcessSocketServer, self).__init__(**kwargs)
        self.stream_clients = defaultdict(dict)

    def encode(self, data):
        return self.registry.encode_json_buffers(data)

    async def decode(self, data):
        raise NotImplementedError

    def post_stream_channel(self, data, channel, hash_name):
        dispatch_stream_channel_to_queues(
            data, channel, hash_name, self.stream_clients, self.encode)

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


async def socket_handler(executor: ProcessSocketServer, stream: SocketStream):
    # write any response
    await executor.write_socket(executor.encode({'data': 'hello'}), stream)

    while True:
        try:
            msg = await executor.read_decode_json_buffers(stream)
            cmd = msg['cmd']
            packet = msg['packet']
            data = msg['data']

            ret_data = {
                'cmd': cmd,
                'packet': packet,
            }

            if cmd == 'ensure_remote_instance':
                res = await executor.ensure_instance(data)
            elif cmd == 'delete_remote_instance':
                res = await executor.delete_instance(data)
            elif cmd == 'execute':
                res = await executor.execute(data)
            elif cmd == 'execute_generator':
                ret_data['done_execute'] = False
                async with aclosing(executor.execute_generator(data)) as aiter:
                    # if this raises an error it stops the underlying generator
                    async for res in aiter:
                        ret_data['data'] = res
                        await executor.write_socket(
                            executor.encode(ret_data), stream)

                res = None
                ret_data['done_execute'] = True
            elif cmd == 'get_remote_objects':
                res = await executor.get_objects(data)
            elif cmd == 'get_remote_object_config':
                res = await executor.get_object_config(data)
            elif cmd == 'get_remote_object_property_data':
                res = await executor.get_object_data(data)
            elif cmd == 'get_echo_clock':
                res = await executor.get_echo_clock(data)
            else:
                raise Exception(f'Unknown command "{cmd}"')

            ret_data['data'] = res
            encoded_ret = executor.encode(ret_data)
        except Exception as e:
            # todo: ignore write_socket in generator
            ret_data = {'exception': serialize_exception(e)}
            encoded_ret = executor.encode(ret_data)

        await executor.write_socket(encoded_ret, stream)


async def socket_data_stream_handler(
        executor: ProcessSocketServer, stream: SocketStream, data):
    queue = MaxSizeErrorDeque(max_size=MAX_QUEUE_SIZE)

    def add_to_queue(item):
        encoded_data = executor.encode(item)
        queue.add_item(encoded_data, len(encoded_data))

    try:
        binding, initial = await executor.start_logging_object_data(
            data, add_to_queue)

        try:
            # write any response
            await executor.write_socket(
                executor.encode({'data': 'hello'}), stream)

            msg_data = {'packet': 0, 'data': executor.encode(initial)}
            await executor.write_socket(executor.encode(msg_data), stream)

            packet = 1
            async for data_item in queue:
                msg_data = {'packet': packet, 'data': data_item}
                packet += 1

                await executor.write_socket(executor.encode(msg_data), stream)
        finally:
            await executor.stop_logging_object_data(binding)
    except Exception as e:
        ret_data = {'exception': serialize_exception(e)}
        await executor.write_socket(executor.encode(ret_data), stream)


async def socket_stream_handler(
        executor: ProcessSocketServer, stream: SocketStream, channel, data):
    queue = MaxSizeErrorDeque(max_size=MAX_QUEUE_SIZE)
    client_key = object()
    hash_key = None

    try:
        hash_key = channel, data['hash_name']
        executor.stream_clients[hash_key][client_key] = queue

        # write any response
        await executor.write_socket(executor.encode({'data': 'hello'}), stream)

        packet = 0
        async for item_data, item_channel, item_hash in queue:
            msg_data = {
                'packet': packet, 'data': {
                    'data': item_data, 'stream': item_channel,
                    'hash_name': item_hash}
            }
            packet += 1

            await executor.write_socket(executor.encode(msg_data), stream)
    except Exception as e:
        ret_data = {'exception': serialize_exception(e)}
        await executor.write_socket(executor.encode(ret_data), stream)
    finally:
        if hash_key is not None:
            del executor.stream_clients[hash_key][client_key]
            if not executor.stream_clients[hash_key]:
                del executor.stream_clients[hash_key]


async def serve(
        host, port, stream_changes, allow_remote_class_registration,
        allow_import_from_main, max_queue_size):
    # todo: catch and send back errors (ignoring socket closing errors?)
    global MAX_QUEUE_SIZE
    MAX_QUEUE_SIZE = max_queue_size
    thread_executor = ThreadExecutor()

    executor = ProcessSocketServer(executor=thread_executor)
    executor.stream_changes = stream_changes
    executor.allow_remote_class_registration = allow_remote_class_registration
    executor.allow_import_from_main = allow_import_from_main

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (host, port)
    await sock.bind(server_address)
    sock.listen()

    async def handler(stream: SocketStream):
        data = await executor.read_decode_json_buffers(stream)
        if data.get('eof', False):
            # todo: close in a nicer way by not accepting new requests
            nursery.cancel_scope.cancel()
            return

        channel = data.get('stream', None)

        if channel is None:
            await socket_handler(executor, stream)
        elif channel == 'data':
            await socket_data_stream_handler(executor, stream, data['data'])
        else:
            await socket_stream_handler(
                executor, stream, channel, data['data'])

    with ExecutorContext(thread_executor):
        async with thread_executor:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(
                    serve_listeners, handler, [SocketListener(sock)])


def run_app():
    parser = argparse.ArgumentParser(description='PyMoa process server.')

    def to_bool(val):
        if val.lower() in ('1', 'true', 'yes', 'y', 't'):
            return True
        if val.lower() in ('0', 'false', 'no', 'n', 'f'):
            return False
        raise ValueError(f'{val} not recognized')

    parser.add_argument(
        '--host', dest='host', action='store', default="127.0.0.1")
    parser.add_argument(
        '--port', dest='port', action='store', default=5000, type=int)
    parser.add_argument(
        '--stream_changes', dest='stream_changes', action='store',
        default=True, type=to_bool)
    parser.add_argument(
        '--remote_class_registration',
        dest='allow_remote_class_registration', action='store', default=True,
        type=to_bool)
    parser.add_argument(
        '--import_from_main', dest='allow_import_from_main', action='store',
        default=False, type=to_bool)
    parser.add_argument(
        '--max_queue_size', dest='max_queue_size', action='store',
        default=MAX_QUEUE_SIZE, type=int)

    args = parser.parse_args()

    trio.run(
        serve, args.host, args.port, args.stream_changes,
        args.allow_remote_class_registration, args.allow_import_from_main,
        args.max_queue_size)


if __name__ == '__main__':
    run_app()
