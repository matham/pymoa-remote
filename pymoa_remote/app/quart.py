"""Quart App
============

"""
# todo: investigate compression and no-cache for data
# todo: immediately close connection for sse/stream if full
from quart_trio import QuartTrio
from quart import make_response, request, jsonify, websocket
from functools import wraps
from collections import defaultdict
from async_generator import aclosing
import argparse
import json
import os
import trio

from pymoa_remote.threading import ThreadExecutor
from pymoa_remote.utils import MaxSizeErrorDeque
from pymoa_remote.server import SimpleExecutorServer, \
    dispatch_stream_channel_to_queues
from pymoa_remote.exception import serialize_exception

__all__ = (
    'create_app', 'start_app', 'run_app', 'QuartRestServer',
    'QuartSocketServer')

MAX_QUEUE_SIZE = 50 * 1024 * 1024


def convert_io(func):
    @wraps(func)
    async def inner(self: 'QuartRestServer'):
        try:
            data = (await request.get_data()).decode('utf8')
            decoded = self.decode(data)

            result = await func(self, decoded)

            encoded = self.encode({'data': result})
        except Exception as e:
            # todo: ignore write_socket in generator
            ret_data = {'exception': serialize_exception(e)}
            encoded = self.encode(ret_data)

        return await make_response(
            encoded, {'Content-Type': 'application/json'})

    return inner


class QuartRestServer(SimpleExecutorServer):
    """Quart server side rest handler.
    """

    quart_app = None

    stream_clients = {}

    def __init__(self, quart_app, stream_clients, **kwargs):
        super(QuartRestServer, self).__init__(**kwargs)
        self.quart_app = quart_app
        self.stream_clients = stream_clients

    def post_stream_channel(self, data, channel, hash_name):
        dispatch_stream_channel_to_queues(
            data, channel, hash_name, self.stream_clients, self.encode)

    @convert_io
    async def remote_import(self, data: dict):
        return await super().remote_import(data)

    @convert_io
    async def register_remote_class(self, data: dict):
        return await super().register_remote_class(data)

    @convert_io
    async def ensure_instance(self, data: dict) -> None:
        return await super().ensure_instance(data)

    @convert_io
    async def delete_instance(self, data: dict) -> None:
        return await super().delete_instance(data)

    @convert_io
    async def execute(self, data: dict):
        return await super().execute(data)

    async def rest_execute_generator(self):
        data = (await request.get_data()).decode('utf8')
        decoded = self.decode(data)

        async def send_events():
            resp_data = json.dumps('alive')
            message = f"data: {resp_data}\n\n"
            yield message.encode('utf-8')

            try:
                id_data = json.dumps(False)
                async with aclosing(self.execute_generator(decoded)) as aiter:
                    async for item in aiter:
                        resp_data = self.encode({'data': item})
                        message = f"data: {resp_data}\nid: {id_data}\n\n"
                        yield message.encode('utf-8')

                resp_data = self.encode({})
                id_data = json.dumps(True)
                message = f"data: {resp_data}\nid: {id_data}\n\n"
                yield message.encode('utf-8')
            except Exception as e:
                ret_data = {'exception': serialize_exception(e)}
                resp_data = self.encode(ret_data)
                id_data = json.dumps(False)

                message = f"data: {resp_data}\nid: {id_data}\n\n"
                yield message.encode('utf-8')

        response = await make_response(
            send_events(),
            {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Transfer-Encoding': 'chunked',
            },
        )
        response.timeout = None
        return response

    @convert_io
    async def get_objects(self, data: dict):
        return await super().get_objects(data)

    @convert_io
    async def get_object_config(self, data: dict):
        return await super().get_object_config(data)

    @convert_io
    async def get_object_data(self, data: dict):
        return await super().get_object_data(data)

    @convert_io
    async def get_echo_clock(self, data: dict):
        return await super().get_echo_clock(data)

    async def sse_data(self):
        # todo: send alive with timeout in case skipped packets
        async def send_events():
            queue = MaxSizeErrorDeque(max_size=MAX_QUEUE_SIZE)

            def add_to_queue(item):
                encoded_data = self.encode(item)
                queue.add_item(encoded_data, len(encoded_data))

            try:
                # todo: make sure in all the apps decoding will raise user
                #  error, not system error like here
                req_data = (await request.get_data()).decode('utf8')
                req_data = self.decode(req_data)

                binding, initial = await self.start_logging_object_data(
                    req_data, add_to_queue)

                try:
                    data = json.dumps('alive')
                    message = f"data: {data}\n\n"
                    yield message.encode('utf-8')

                    data = self.encode({'data': self.encode(initial)})
                    id_data = json.dumps((0, ))
                    message = f"data: {data}\nid: {id_data}\n\n"
                    yield message.encode('utf-8')

                    packet = 1
                    async for data_item in queue:
                        data = self.encode({'data': data_item})
                        id_data = json.dumps((packet, ))
                        message = f"data: {data}\nid: {id_data}\n\n"
                        yield message.encode('utf-8')

                        packet += 1
                finally:
                    await self.stop_logging_object_data(binding)
            except Exception as e:
                msg_data = {'exception': serialize_exception(e)}
                data = self.encode(msg_data)
                id_data = json.dumps((None, ))

                message = f"data: {data}\nid: {id_data}\n\n"
                yield message.encode('utf-8')

        response = await make_response(
            send_events(),
            {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Transfer-Encoding': 'chunked',
            },
        )
        response.timeout = None
        return response

    async def sse_channel(self, channel):
        # todo: send alive with timeout in case skipped packets
        async def send_events():
            queue = MaxSizeErrorDeque(max_size=MAX_QUEUE_SIZE)
            client_key = object()
            hash_key = None

            try:
                req_data = (await request.get_data()).decode('utf8')
                req_data = self.decode(req_data)

                hash_key = channel, req_data['hash_name']
                self.stream_clients[hash_key][client_key] = queue

                data = json.dumps('alive')
                message = f"data: {data}\n\n"
                yield message.encode('utf-8')

                packet = 0
                async for item_data, item_channel, item_hash in queue:
                    msg_data = {
                        'data': {
                            'data': item_data, 'stream': item_channel,
                            'hash_name': item_hash}
                    }

                    data = self.encode(msg_data)
                    id_data = json.dumps((packet, item_channel, item_hash))
                    message = f"data: {data}\nid: {id_data}\n\n"
                    yield message.encode('utf-8')

                    packet += 1
            except Exception as e:
                msg_data = {'exception': serialize_exception(e)}
                data = self.encode(msg_data)
                id_data = json.dumps((None, None, None))

                message = f"data: {data}\nid: {id_data}\n\n"
                yield message.encode('utf-8')
            finally:
                if hash_key is not None:
                    del self.stream_clients[hash_key][client_key]
                    if not self.stream_clients[hash_key]:
                        del self.stream_clients[hash_key]

        response = await make_response(
            send_events(),
            {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Transfer-Encoding': 'chunked',
            },
        )
        response.timeout = None
        return response

    async def sse_channel_ensure(self):
        return await self.sse_channel('ensure')

    async def sse_channel_delete(self):
        return await self.sse_channel('delete')

    async def sse_channel_execute(self):
        return await self.sse_channel('execute')

    async def sse_channel_all(self):
        return await self.sse_channel('')


class QuartSocketServer(SimpleExecutorServer):
    """Quart server side socket handler.
    """

    quart_app = None

    stream_clients = {}

    def __init__(self, quart_app, stream_clients, **kwargs):
        super(QuartSocketServer, self).__init__(**kwargs)
        self.quart_app = quart_app
        self.stream_clients = stream_clients

    def encode(self, data):
        return self.registry.encode_json_buffers(data)

    async def decode(self, data):
        raise NotImplementedError

    def decode_json_buffers(self, data) -> dict:
        return self.registry.decode_json_buffers_raw(data)

    def post_stream_channel(self, data, channel, hash_name):
        dispatch_stream_channel_to_queues(
            data, channel, hash_name, self.stream_clients, self.encode)

    async def websocket_handler(self):
        await websocket.send(self.encode({'data': 'hello'}))

        while True:
            try:
                msg = self.decode_json_buffers(await websocket.receive())
                cmd = msg['cmd']
                packet = msg['packet']
                data = msg['data']

                ret_data = {
                    'cmd': cmd,
                    'packet': packet,
                }

                if cmd == 'ensure_remote_instance':
                    res = await self.ensure_instance(data)
                elif cmd == 'delete_remote_instance':
                    res = await self.delete_instance(data)
                elif cmd == 'execute':
                    res = await self.execute(data)
                elif cmd == 'execute_generator':
                    ret_data['done_execute'] = False
                    async with aclosing(self.execute_generator(data)) as aiter:
                        async for res in aiter:
                            ret_data['data'] = res
                            await websocket.send(self.encode(ret_data))

                    res = None
                    ret_data['done_execute'] = True
                elif cmd == 'get_remote_objects':
                    res = await self.get_objects(data)
                elif cmd == 'get_remote_object_config':
                    res = await self.get_object_config(data)
                elif cmd == 'get_remote_object_property_data':
                    res = await self.get_object_data(data)
                elif cmd == 'get_echo_clock':
                    res = await self.get_echo_clock(data)
                else:
                    raise Exception(f'Unknown command "{cmd}"')

                ret_data['data'] = res
                encoded_ret = self.encode(ret_data)
            except Exception as e:
                # todo: ignore write_socket in generator
                ret_data = {'exception': serialize_exception(e)}
                encoded_ret = self.encode(ret_data)

            await websocket.send(encoded_ret)

    async def websocket_data_stream_handler(self, data):
        queue = MaxSizeErrorDeque(max_size=MAX_QUEUE_SIZE)

        def add_to_queue(item):
            encoded_data = self.encode(item)
            queue.add_item(encoded_data, len(encoded_data))

        try:
            binding, initial = await self.start_logging_object_data(
                data, add_to_queue)

            try:
                # write any response
                await websocket.send(self.encode({'data': 'hello'}))

                msg_data = {'packet': 0, 'data': self.encode(initial)}
                await websocket.send(self.encode(msg_data))

                packet = 1
                async for data_item in queue:
                    msg_data = {'packet': packet, 'data': data_item}
                    packet += 1

                    await websocket.send(self.encode(msg_data))
            finally:
                await self.stop_logging_object_data(binding)
        except Exception as e:
            ret_data = {'exception': serialize_exception(e)}
            await websocket.send(self.encode(ret_data))

    async def websocket_stream_handler(self, channel, data):
        queue = MaxSizeErrorDeque(max_size=MAX_QUEUE_SIZE)
        client_key = object()
        hash_key = None

        try:
            hash_key = channel, data['hash_name']
            self.stream_clients[hash_key][client_key] = queue

            # write any response
            await websocket.send(self.encode({'data': 'hello'}))

            packet = 0
            async for item_data, item_channel, item_hash in queue:
                msg_data = {
                    'packet': packet, 'data': {
                        'data': item_data, 'stream': item_channel,
                        'hash_name': item_hash}
                }
                packet += 1

                await websocket.send(self.encode(msg_data))
        except Exception as e:
            # todo: ignore error when socket is closed remotely
            ret_data = {'exception': serialize_exception(e)}
            await websocket.send(self.encode(ret_data))
        finally:
            if hash_key is not None:
                del self.stream_clients[hash_key][client_key]
                if not self.stream_clients[hash_key]:
                    del self.stream_clients[hash_key]

    async def ws(self):
        data = self.decode_json_buffers(await websocket.receive())
        channel = data.get('stream', None)

        if channel is None:
            await self.websocket_handler()
        elif channel == 'data':
            await self.websocket_data_stream_handler(data['data'])
        else:
            await self.websocket_stream_handler(channel, data['data'])


def handle_unexpected_error(error):
    message = [str(x) for x in error.args]
    status_code = 500
    success = False
    response = {
        'success': success,
        'error': {
            'type': error.__class__.__name__,
            'message': f'An unexpected error has occurred: "{message}".'
        }
    }

    return jsonify(response), status_code


def create_app(
        stream_changes=True, allow_remote_class_registration=True,
        allow_import_from_main=False, max_queue_size=MAX_QUEUE_SIZE
) -> QuartTrio:
    """Creates the quart app.
    """
    global MAX_QUEUE_SIZE
    MAX_QUEUE_SIZE = max_queue_size

    app = QuartTrio(__name__)

    thread_executor = ThreadExecutor()

    stream_clients = defaultdict(dict)

    rest_executor = app.rest_executor = QuartRestServer(
        quart_app=app, executor=thread_executor, stream_clients=stream_clients)
    rest_executor.stream_changes = stream_changes
    rest_executor.allow_remote_class_registration = \
        allow_remote_class_registration
    rest_executor.allow_import_from_main = allow_import_from_main

    # the objects are shared between the two executors
    socket_executor = app.socket_executor = QuartSocketServer(
        quart_app=app, registry=rest_executor.registry,
        executor=thread_executor, stream_clients=stream_clients)
    socket_executor.stream_changes = stream_changes
    socket_executor.allow_remote_class_registration = \
        allow_remote_class_registration
    socket_executor.allow_import_from_main = allow_import_from_main

    app.add_url_rule(
        '/api/v1/objects/import', view_func=rest_executor.remote_import,
        methods=['POST'])
    app.add_url_rule(
        '/api/v1/objects/register_class',
        view_func=rest_executor.register_remote_class, methods=['POST'])
    app.add_url_rule(
        '/api/v1/objects/create_open', view_func=rest_executor.ensure_instance,
        methods=['POST'])
    app.add_url_rule(
        '/api/v1/objects/delete', view_func=rest_executor.delete_instance,
        methods=['POST'])
    app.add_url_rule(
        '/api/v1/objects/execute', view_func=rest_executor.execute,
        methods=['POST'])
    app.add_url_rule(
        '/api/v1/objects/execute_generator/stream',
        view_func=rest_executor.rest_execute_generator, methods=['POST'])
    app.add_url_rule(
        '/api/v1/objects/list', view_func=rest_executor.get_objects,
        methods=['GET'])
    app.add_url_rule(
        '/api/v1/objects/config', view_func=rest_executor.get_object_config,
        methods=['GET'])
    app.add_url_rule(
        '/api/v1/objects/properties', view_func=rest_executor.get_object_data,
        methods=['GET'])
    app.add_url_rule(
        '/api/v1/echo_clock', view_func=rest_executor.get_echo_clock,
        methods=['GET'])

    app.add_url_rule(
        '/api/v1/stream/data', view_func=rest_executor.sse_data,
        methods=['GET']
    )

    app.add_url_rule(
        '/api/v1/stream/ensure',
        view_func=rest_executor.sse_channel_ensure, methods=['GET'])
    app.add_url_rule(
        '/api/v1/stream/delete',
        view_func=rest_executor.sse_channel_delete, methods=['GET'])
    app.add_url_rule(
        '/api/v1/stream/execute',
        view_func=rest_executor.sse_channel_execute, methods=['GET'])
    app.add_url_rule(
        '/api/v1/stream/all',
        view_func=rest_executor.sse_channel_all, methods=['GET'])

    app.add_websocket('/api/v1/ws', view_func=socket_executor.ws)

    # app.register_error_handler(Exception, handle_unexpected_error)

    return app


async def start_app(app, host='127.0.0.1', port=5000):
    # start/stop thread executor
    async with app.rest_executor.executor:
        await app.run_task(host, port)


def run_app():
    parser = argparse.ArgumentParser(description='PyMoa basic server.')

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

    app = create_app(
        args.stream_changes, args.allow_remote_class_registration,
        args.allow_import_from_main, args.max_queue_size
    )

    trio.run(start_app, app, args.host, args.port)


if __name__ == '__main__':
    os.environ.setdefault('KIVY_NO_ARGS', '1')
    run_app()
