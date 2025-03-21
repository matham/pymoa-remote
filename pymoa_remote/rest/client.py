"""REST Executor
================

"""
from typing import AsyncGenerator, Tuple, Optional, Union, Callable, Any, \
    List, Iterable, AsyncContextManager
from httpx import AsyncClient, Limits, Response
import time
from trio import TASK_STATUS_IGNORED
import trio
from async_generator import aclosing
from tree_config import apply_config

from pymoa_remote.rest.sse import SSEStream
from pymoa_remote.client import Executor
from pymoa_remote.executor import NO_CALLBACK
from pymoa_remote.exception import raise_remote_exception_from_frames
from pymoa_remote.utils import asynccontextmanager

__all__ = ('RestExecutor', )


class RestExecutor(Executor):
    """Executor that sends all requests to a remote server to be executed
    there, using a rest API.
    """

    _httpx_client: Optional[AsyncClient] = None

    uri: str = ''

    _limiter: Optional[trio.Lock] = None

    def __init__(self, uri: str, **kwargs):
        super(RestExecutor, self).__init__(**kwargs)
        if uri.endswith('/'):
            uri = uri[:-1]
        self.uri = uri

    async def _vanilla_write_read(
            self, path_suffix: str, data: dict, method: str) -> dict:
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/{path_suffix}'
        response = await self._httpx_client.request(
            method, uri, content=data,
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()

        res = self.decode(response.text)

        exception = res.get('exception', None)
        if exception is not None:
            raise_remote_exception_from_frames(**exception)

        return res

    async def remote_import(self, module):
        await self._vanilla_write_read(
            'objects/import',
            self._get_remote_import_data(module),
            'post'
        )

    async def register_remote_class(self, cls):
        await self._vanilla_write_read(
            'objects/register_class',
            self._get_register_remote_class_data(cls),
            'post'
        )

    async def ensure_remote_instance(
            self, obj, hash_name, *args, auto_register_class=True, **kwargs
    ) -> bool:
        if hash_name in self.registry.hashed_instances:
            return False

        cls = obj.__class__
        if auto_register_class and not self.registry.is_class_registered(
                class_to_register=cls):
            self.registry.register_class(cls)

        self.registry.add_instance(obj, hash_name)

        res = await self._vanilla_write_read(
            'objects/create_open',
            self._get_ensure_remote_instance_data(
                obj, args, kwargs, hash_name, auto_register_class),
            'post'
        )
        assert res['data']
        return True

    async def delete_remote_instance(self, obj):
        await self._vanilla_write_read(
            'objects/delete',
            self._get_delete_remote_instance_data(obj),
            'post'
        )

        self.registry.delete_instance(obj)

    async def start_executor(self):
        self._limiter = trio.Lock()
        self._httpx_client = AsyncClient()

    async def stop_executor(self):
        self._limiter = None
        self._httpx_client = None

    async def execute(
            self, obj, fn: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None):
        data = self._get_execute_data(obj, fn, args, kwargs, callback)

        async with self._limiter:
            res = await self._vanilla_write_read(
                'objects/execute', data, 'post')

            ret_val = res['data']
            if callback is not NO_CALLBACK:
                self.call_execute_callback(obj, ret_val, callback)
        return ret_val

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
            async with self._httpx_client.stream(
                "POST", uri, content=data,
                headers={'Content-Type': 'application/json'},
            ) as response:
                response.raise_for_status()

                # todo: move this and everywhere else it's used to after we
                #  bound or the generator started
                task_status.started()
                async for _, data, id_, _ in SSEStream.stream(
                        response.aiter_bytes()
                ):
                    data = decode(data)
                    if data == 'alive':
                        continue

                    exception = data.get('exception', None)
                    if exception is not None:
                        raise_remote_exception_from_frames(**exception)

                    done_execute = decode(id_)
                    if done_execute:
                        return

                    return_value = data['data']
                    call_callback(return_value, callback)
                    yield return_value

    async def get_remote_objects(self):
        res = await self._vanilla_write_read(
            'objects/list',
            self._get_remote_objects_data(),
            'get'
        )
        return res['data']

    async def get_remote_object_config(self, obj: Optional[Any]):
        res = await self._vanilla_write_read(
            'objects/config',
            self._get_remote_object_config_data(obj),
            'get'
        )
        return res['data']

    async def apply_config_from_remote(self, obj):
        config = await self.get_remote_object_config(obj)
        apply_config(obj, config)

    async def get_remote_object_property_data(
            self, obj: Any, properties: List[str]) -> dict:
        res = await self._vanilla_write_read(
            'objects/properties',
            self._get_remote_object_property_data_data(obj, properties),
            'get'
        )
        return res['data']

    async def apply_property_data_from_remote(
            self, obj: Any, properties: List[str]):
        props = await self.get_remote_object_property_data(obj, properties)
        for key, value in props.items():
            setattr(obj, key, value)

    async def _generate_sse_events(self, response: Response, task_status):
        decode = self.decode
        last_packet = None
        task_status.started()
        async for _, data, id_, _ in SSEStream.stream(response.aiter_bytes()):
            res = decode(data)
            if res == 'alive':
                continue

            exception = res.get('exception', None)
            if exception is not None:
                raise_remote_exception_from_frames(**exception)

            packet, *_ = decode(id_)
            if last_packet is not None and last_packet + 1 != packet:
                raise ValueError(
                    f'Packets were skipped {last_packet} -> {packet}')
            last_packet = packet

            ret_data = res['data']
            ret_data['data'] = self.decode(ret_data['data'])
            yield ret_data

    @asynccontextmanager
    async def get_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            initial_properties: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED
    ) -> AsyncContextManager[AsyncGenerator]:
        data = self._get_remote_object_data_data(
            obj, trigger_names, triggered_logged_names, logged_names,
            initial_properties)
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/stream/data'
        async with self._httpx_client.stream(
            "GET", uri, content=data,
            headers={'Content-Type': 'application/json'}
        ) as response:
            response.raise_for_status()

            async with aclosing(
                    self._generate_sse_events(response, task_status)) as aiter:
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
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/stream/data'
        async with self._httpx_client.stream(
            "GET", uri, content=data,
            headers={'Content-Type': 'application/json'}
        ) as response:
            response.raise_for_status()

            await self._apply_data_from_remote(
                obj, self._generate_sse_events(response, task_status))

    @asynccontextmanager
    async def get_channel_from_remote(
            self, hash_name: str, channel: str,
            task_status=TASK_STATUS_IGNORED
    ) -> AsyncContextManager[AsyncGenerator]:
        data = self._get_remote_object_channel_data(hash_name, channel)
        data = self.encode(data)

        if not channel:
            channel = 'all'
        uri = f'{self.uri}/api/v1/stream/{channel}'
        async with self._httpx_client.stream(
            "GET", uri, content=data,
            headers={'Content-Type': 'application/json'}
        ) as response:
            response.raise_for_status()

            async with aclosing(
                    self._generate_sse_events(response, task_status)) as aiter:
                yield aiter

    async def apply_execute_from_remote(
            self, obj, exclude_self=True, task_status=TASK_STATUS_IGNORED):
        data = self._get_remote_object_channel_data(obj, 'execute')
        data = self.encode(data)

        uri = f'{self.uri}/api/v1/stream/execute'
        async with self._httpx_client.stream(
            "GET", uri, content=data,
            headers={'Content-Type': 'application/json'}
        ) as response:
            response.raise_for_status()

            await self._apply_execute_from_remote(
                obj, self._generate_sse_events(response, task_status),
                exclude_self)

    async def get_echo_clock(self) -> Tuple[int, int, int]:
        start_time = time.perf_counter_ns()
        res = await self._vanilla_write_read(
            'echo_clock',
            self._get_clock_data(),
            'get'
        )

        return start_time, res['data']['server_time'], time.perf_counter_ns()

    async def sleep(self, duration=None, deadline=None) -> int:
        res = await self._vanilla_write_read(
            'sleep',
            self._get_sleep_data(duration, deadline),
            'get'
        )

        return res['data']['server_time']
