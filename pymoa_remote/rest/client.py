"""Client
=========

"""
from typing import AsyncGenerator, Tuple, Optional, Union, Callable, Any, \
    List, Iterable
from asks import Session
from asks.errors import BadStatus
import time
from trio import TASK_STATUS_IGNORED
import trio
from async_generator import aclosing
import contextlib
from tree_config import apply_config

from pymoa_remote.rest import SSEStream
from pymoa_remote.client import Executor
from pymoa_remote.executor import NO_CALLBACK

__all__ = ('RestExecutor', )


def raise_for_status(response):
    """
    Raise BadStatus if one occurred.
    """
    if 400 <= response.status_code < 500:
        raise BadStatus(
            '{} Client Error: {} for url: {}'.format(
                response.status_code, response.reason_phrase, response.url
            ),
            response,
            response.status_code
        )
    elif 500 <= response.status_code < 600:
        raise BadStatus(
            '{} Server Error: {} for url: {}'.format(
                response.status_code, response.reason_phrase, response.url
            ),
            response,
            response.status_code
        )


class RestExecutor(Executor):
    """Executor that sends all requests to a remote server to be executed
    there, using a rest API.
    """

    _session: Optional[Session] = None

    uri: str = ''

    _limiter: Optional[trio.Lock] = None

    def __init__(self, uri: str, **kwargs):
        super(RestExecutor, self).__init__(**kwargs)
        if uri.endswith('/'):
            uri = uri[:-1]
        self.uri = uri

    async def ensure_remote_instance(self, obj, hash_name, *args, **kwargs):
        self.registry.add_instance(obj, hash_name)

        data = self._get_ensure_remote_instance_data(
            obj, args, kwargs, hash_name)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/objects/create_open'
        response = await self._session.post(
            uri, data=data, headers={'Content-Type': 'application/json'})
        response.raise_for_status()

    async def delete_remote_instance(self, obj):
        data = self._get_delete_remote_instance_data(obj)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/objects/delete'
        response = await self._session.post(
            uri, data=data, headers={'Content-Type': 'application/json'})
        response.raise_for_status()

        self.registry.delete_instance(obj)

    async def start_executor(self):
        self._limiter = trio.Lock()
        self._session = Session(connections=1)

    async def stop_executor(self):
        self._limiter = None
        self._session = None

    async def execute(
            self, obj, fn: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None):
        data = self._get_execute_data(obj, fn, args, kwargs, callback)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/objects/execute'
        async with self._limiter:
            response = await self._session.post(
                uri, data=data, headers={'Content-Type': 'application/json'})
            response.raise_for_status()

            res = self.decode(response.text)
            if callback is not NO_CALLBACK:
                self.call_execute_callback(obj, res, callback)
        return res

    async def execute_generator(
            self, obj, gen: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None,
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        decode = self.decode

        data = self._get_execute_data(obj, gen, args, kwargs, callback)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/objects/execute_generator/stream'
        callback = self.get_execute_callback_func(obj, callback)
        call_callback = self.call_execute_callback_func

        async with self._limiter:
            response = await self._session.post(
                uri, data=data, headers={'Content-Type': 'application/json'},
                stream=True)
            raise_for_status(response)

            async with response.body() as response_body:
                task_status.started()
                async for _, data, id_, _ in SSEStream.stream(response_body):
                    data = decode(data)
                    if data == 'alive':
                        continue

                    done_execute = decode(id_)
                    if done_execute:
                        return

                    return_value = data['return_value']
                    call_callback(return_value, callback)
                    yield return_value

    async def get_remote_objects(self):
        data = self._get_remote_objects_data()
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/objects/list'
        response = await self._session.get(
            uri, data=data, headers={'Content-Type': 'application/json'})
        response.raise_for_status()

        return self.decode(response.text)

    async def get_remote_object_config(self, obj: Optional[Any]):
        data = self._get_remote_object_config_data(obj)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/objects/config'
        response = await self._session.get(
            uri, data=data, headers={'Content-Type': 'application/json'})
        response.raise_for_status()

        return self.decode(response.text)

    async def apply_config_from_remote(self, obj):
        config = await self.get_remote_object_config(obj)
        apply_config(obj, config)

    async def get_remote_object_property_data(
            self, obj: Any, properties: List[str]) -> dict:
        data = self._get_remote_object_property_data_data(obj, properties)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/objects/properties'
        response = await self._session.get(
            uri, data=data, headers={'Content-Type': 'application/json'})
        response.raise_for_status()

        return self.decode(response.text)

    async def apply_property_data_from_remote(
            self, obj: Any, properties: List[str]):
        props = await self.get_remote_object_property_data(obj, properties)
        for key, value in props.items():
            setattr(obj, key, value)

    async def _generate_sse_events(self, response, task_status):
        decode = self.decode
        last_packet = None
        async with response.body() as response_body:
            task_status.started()
            async for _, data, id_, _ in SSEStream.stream(response_body):
                data = decode(data)
                if data == 'alive':
                    continue

                packet, *_ = decode(id_)
                if last_packet is not None and last_packet + 1 != packet:
                    raise ValueError(
                        f'Packets were skipped {last_packet} -> {packet}')
                last_packet = packet

                yield data

    @contextlib.asynccontextmanager
    async def get_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        data = self._get_remote_object_data_data(
            obj, trigger_names, triggered_logged_names, logged_names)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/stream/data'
        response = await self._session.get(
            uri, data=data, headers={'Content-Type': 'application/json'},
            stream=True)
        raise_for_status(response)

        async with aclosing(
                self._generate_sse_events(response, task_status)) as aiter:
            yield aiter

    async def apply_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED):
        data = self._get_remote_object_data_data(
            obj, trigger_names, triggered_logged_names, logged_names)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/stream/data'
        response = await self._session.get(
            uri, data=data, headers={'Content-Type': 'application/json'},
            stream=True)
        raise_for_status(response)

        await self._apply_data_from_remote(
            obj, self._generate_sse_events(response, task_status))

    @contextlib.asynccontextmanager
    async def get_channel_from_remote(
            self, obj: Optional[Any], channel: str,
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        data = self._get_remote_object_channel_data(obj, channel)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/stream/{channel}'
        response = await self._session.get(
            uri, data=data, headers={'Content-Type': 'application/json'},
            stream=True)
        raise_for_status(response)

        async with aclosing(
                self._generate_sse_events(response, task_status)) as aiter:
            yield aiter

    async def apply_execute_from_remote(
            self, obj, exclude_self=True, task_status=TASK_STATUS_IGNORED):
        data = self._get_remote_object_channel_data(obj, 'execute')
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/stream/execute'
        response = await self._session.get(
            uri, data=data, headers={'Content-Type': 'application/json'},
            stream=True)
        raise_for_status(response)

        await self._apply_execute_from_remote(
            obj, self._generate_sse_events(response, task_status),
            exclude_self)

    async def get_echo_clock(self) -> Tuple[int, int, int]:
        start_time = time.perf_counter_ns()
        data = self._get_clock_data()
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/echo_clock'
        response = await self._session.get(
            uri, data=data, headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        server_time = self.decode(response.text)['server_time']

        return start_time, server_time, time.perf_counter_ns()
