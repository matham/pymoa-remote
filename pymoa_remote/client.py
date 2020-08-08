from typing import Optional, Any, List, Iterable
import uuid
import contextlib
import trio
import os
from async_generator import aclosing
from functools import wraps, partial
from asyncio import iscoroutinefunction
from inspect import isgeneratorfunction, isasyncgenfunction, getsourcefile
from tree_config import read_config_from_object
from contextvars import ContextVar

from pymoa_remote.executor import ExecutorBase, InstanceRegistry

__all__ = (
    'ExecutorContext', 'apply_executor', 'apply_generator_executor',
    'Executor', 'LocalRegistry')


current_executor: ContextVar[Optional['Executor']] = ContextVar(
    'current_executor', default=None)


class ExecutorContext:
    # todo: use this in decorator

    executor: 'Executor'

    token = None

    def __init__(self, executor: 'Executor', **kwargs):
        super().__init__(**kwargs)
        self.executor = executor

    def __enter__(self):
        if self.token is not None:
            raise TypeError('Cannot enter ExecutorContext recursively')

        self.token = current_executor.set(self.executor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        current_executor.reset(self.token)
        self.token = None


class Executor(ExecutorBase):
    """Concrete executor that will execute objects remotely."""

    registry: 'LocalRegistry' = None

    _uuid: bytes = None

    def __init__(self, registry: 'LocalRegistry' = None, **kwargs):
        super(Executor, self).__init__(**kwargs)
        if registry is None:
            registry = LocalRegistry()
        self.registry = registry
        self._uuid = uuid.uuid4().bytes

    def encode(self, data):
        return self.registry.encode_json(data)

    def decode(self, data):
        return self.registry.decode_json(data)

    def _get_remote_import_data(self, module):
        if not isinstance(module, str):
            module = module.__name__

        return {
            'module': module,
            'uuid': self._uuid,
        }

    def _get_register_remote_class_data(self, cls):
        if cls.__name__ != cls.__qualname__:
            raise TypeError(f'Cannot register {cls}. Can only register module '
                            f'level classes')

        return {
            'cls_name': cls.__name__,
            'module': cls.__module__,
            'qual_name': cls.__qualname__,
            'uuid': self._uuid,
        }

    def _get_ensure_remote_instance_data(
            self, obj, args, kwargs, hash_name, auto_register_class):
        module = obj.__class__.__module__
        filename = None
        if module == '__main__':
            filename = os.path.abspath(getsourcefile(obj.__class__))
        return {
            'cls_name': obj.__class__.__name__,
            'module': module,
            'qual_name': obj.__class__.__qualname__,
            'mod_filename': filename,
            'args': args,
            'kwargs': kwargs,
            'config': read_config_from_object(obj),
            'hash_name': hash_name,
            'auto_register_class': auto_register_class,
            'uuid': self._uuid,
        }

    def _get_delete_remote_instance_data(self, obj):
        return {
            'hash_name': self.registry.hashed_instances_ids[id(obj)],
            'uuid': self._uuid,
        }

    def _get_execute_data(
            self, obj, sync_fn, args=(), kwargs=None, callback=None):
        hash_name = self.registry.hashed_instances_ids[id(obj)]
        fn_name = sync_fn.__name__

        cb_name = callback
        if cb_name is not None:
            if not isinstance(cb_name, str):
                cb_name = cb_name.__name__

        data = {
            'hash_name': hash_name,
            'method_name': fn_name,
            'args': args,
            'kwargs': kwargs or {},
            'callback': cb_name,
            'uuid': self._uuid,
        }
        return data

    def _get_remote_objects_data(self):
        return {
            'uuid': self._uuid,
        }

    def _get_remote_object_config_data(self, obj: Optional[Any]):
        hash_name = None
        if obj is not None:
            hash_name = self.registry.hashed_instances_ids[id(obj)]

        return {
            'hash_name': hash_name,
            'uuid': self._uuid,
        }

    def _get_remote_object_property_data_data(
            self, obj: Any, properties: List[str]):
        hash_name = self.registry.hashed_instances_ids[id(obj)]

        return {
            'hash_name': hash_name,
            'properties': properties,
            'uuid': self._uuid,
        }

    def _get_remote_object_data_data(
            self, obj: Any, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            initial_properties: Iterable[str] = ()):
        hash_name = self.registry.hashed_instances_ids[id(obj)]

        return {
            'hash_name': hash_name,
            'trigger_names': trigger_names,
            'triggered_logged_names': triggered_logged_names,
            'logged_names': logged_names,
            'initial_properties': initial_properties,
            'uuid': self._uuid,
        }

    async def _apply_data_from_remote(self, obj, gen):
        initial = True
        async with aclosing(gen) as aiter:
            async for data in aiter:
                if initial:
                    initial = False

                    if 'initial_properties' in data:
                        for key, value in data['initial_properties'].items():
                            if key.startswith('on_'):
                                obj.dispatch(key, obj, *value)
                            else:
                                setattr(obj, key, value)

                trigger_name = data['logged_trigger_name']
                trigger_value = data['logged_trigger_value']
                props = data['logged_items']

                for key, value in props.items():
                    if key.startswith('on_'):
                        obj.dispatch(key, obj, *value)
                    else:
                        setattr(obj, key, value)

                if trigger_name:
                    if trigger_name.startswith('on_'):
                        obj.dispatch(trigger_name, *trigger_value)
                    else:
                        setattr(obj, trigger_name, trigger_value)

    def _get_remote_object_channel_data(self, obj: Any, channel: str):
        if channel not in {'ensure', 'delete', 'execute', ''}:
            raise ValueError(
                f'Unrecognized channel {channel}. '
                f'Must be one of ensure, delete, execute')

        hash_name = ''
        if obj is not None:
            hash_name = self.registry.hashed_instances_ids[id(obj)]

        return {
            'hash_name': hash_name,
            'uuid': self._uuid,
        }

    async def _apply_execute_from_remote(self, obj, gen, exclude_self):
        call_execute_callback = self.call_execute_callback
        executor_uuid = self._uuid
        if exclude_self and executor_uuid is None:
            raise ValueError('Cannot exclude self when uuid is not set')

        async with aclosing(gen) as aiter:
            async for data in aiter:
                callback = data['callback']
                return_value = data['return_value']

                if exclude_self and executor_uuid == data['uuid']:
                    continue

                call_execute_callback(obj, return_value, callback)

    def _get_clock_data(self) -> dict:
        return {}


class LocalRegistry(InstanceRegistry):
    """Client side object registry.
    """

    def add_instance(self, obj, hash_name):
        self.hashed_instances[hash_name] = obj
        self.hashed_instances_ids[id(obj)] = hash_name
        return obj

    def delete_instance(self, obj):
        hash_name = self.hashed_instances_ids.pop(id(obj))
        del self.hashed_instances[hash_name]
        return obj


def apply_executor(func=None, callback=None):
    """Decorator that calls the method using the executor.
    """
    if func is None:
        return partial(apply_executor, callback=callback)

    is_coro = iscoroutinefunction(func)

    if isgeneratorfunction(func) or isasyncgenfunction(func):
        raise ValueError(
            f'apply_executor called with generator function "{func}". '
            f'apply_executor does not support generators. Please use '
            f'apply_generator_executor instead')

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        executor: Executor = getattr(
            self, 'pymoa_executor', current_executor.get())
        if executor is None:
            ret_val = func(self, *args, **kwargs)
            Executor.call_execute_callback(self, ret_val, callback)
            return ret_val

        if not executor.supports_non_coroutine:
            raise ValueError(
                f'apply_executor called with normal function "{func}", but '
                f'Executor "{executor}" only supports async coroutines')
        return await executor.execute(self, func, args, kwargs, callback)

    @wraps(func)
    async def wrapper_coro(self, *args, **kwargs):
        executor: Executor = getattr(
            self, 'pymoa_executor', current_executor.get())
        if executor is None:
            ret_val = await func(self, *args, **kwargs)
            Executor.call_execute_callback(self, ret_val, callback)
            return ret_val

        if not executor.supports_coroutine:
            raise ValueError(
                f'apply_executor called with async coroutine "{func}", but '
                f'Executor "{executor}" does not support coroutines')
        return await executor.execute(self, func, args, kwargs, callback)

    if is_coro:
        return wrapper_coro
    return wrapper


def apply_generator_executor(func=None, callback=None):
    """Decorator that calls the generator method using the executor.
    """
    if func is None:
        return partial(apply_generator_executor, callback=callback)

    is_coro = iscoroutinefunction(func)

    if not isgeneratorfunction(func) and not isasyncgenfunction(func):
        raise ValueError(
            f'apply_generator_executor called with non-generator function '
            f'"{func}". apply_generator_executor only supports generators. '
            f'Please use apply_executor instead')

    @contextlib.asynccontextmanager
    @wraps(func)
    async def wrapper_gen(self, *args, **kwargs):
        executor: Executor = getattr(
            self, 'pymoa_executor', current_executor.get())
        if executor is None:
            async def eat_generator():
                callback_fn = Executor.get_execute_callback_func(
                    self, callback)
                call_callback = Executor.call_execute_callback_func

                for yield_val in func(self, *args, **kwargs):
                    call_callback(yield_val, callback_fn)
                    yield yield_val
                    await trio.lowlevel.checkpoint()

            gen = eat_generator()
        else:
            if not executor.supports_non_coroutine:
                raise ValueError(
                    f'apply_executor called with normal function "{func}", but '
                    f'Executor "{executor}" only supports async coroutines')

            gen = executor.execute_generator(
                self, func, args, kwargs, callback)

        async with aclosing(gen) as aiter:
            yield aiter

    @contextlib.asynccontextmanager
    @wraps(func)
    async def wrapper_coro_gen(self, *args, **kwargs):
        executor: Executor = getattr(
            self, 'pymoa_executor', current_executor.get())
        if executor is None:
            async def eat_generator():
                callback_fn = Executor.get_execute_callback_func(
                    self, callback)
                call_callback = Executor.call_execute_callback_func

                async for yield_val in func(self, *args, **kwargs):
                    call_callback(yield_val, callback_fn)
                    yield yield_val

            gen = eat_generator()
        else:
            if not executor.supports_coroutine:
                raise ValueError(
                    f'apply_executor called with async coroutine "{func}", but'
                    f' Executor "{executor}" does not support coroutines')

            gen = executor.execute_generator(
                self, func, args, kwargs, callback)

        async with aclosing(gen) as aiter:
            yield aiter

    if is_coro:
        return wrapper_coro_gen
    return wrapper_gen
