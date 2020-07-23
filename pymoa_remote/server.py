from typing import Dict, List, Any, Callable, Tuple, Set, AsyncGenerator, \
    Iterable, Optional
from async_generator import aclosing
import time
from tree_config import apply_config, read_config_from_object

from pymoa_remote.executor import InstanceRegistry, ExecutorBase

__all__ = (
    'ExecutorServerBase', 'ExecutorServer', 'DataLogger', 'RemoteRegistry')


class ExecutorServerBase:
    """Base class for the server side handling of remote object method
    execution.
    """

    async def ensure_instance(self, *args, **kwargs):
        raise NotImplementedError

    async def delete_instance(self, *args, **kwargs):
        raise NotImplementedError

    async def execute(self, *args, **kwargs):
        raise NotImplementedError

    async def execute_generator(self, *args, **kwargs):
        raise NotImplementedError

    async def get_object_config(self, *args, **kwargs):
        raise NotImplementedError

    async def get_object_data(self, *args, **kwargs):
        raise NotImplementedError

    def encode(self, data):
        raise NotImplementedError

    def decode(self, data):
        raise NotImplementedError


class ExecutorServer(ExecutorServerBase):
    """Concrete server side handler of remote object method execution.
    """

    executor: Optional[ExecutorBase] = None

    registry: 'RemoteRegistry' = None

    stream_data_logger: 'DataLogger' = None

    def __init__(self, registry: 'RemoteRegistry' = None, **kwargs):
        super(ExecutorServer, self).__init__(**kwargs)
        if registry is None:
            registry = RemoteRegistry()
        self.registry = registry

        self.stream_data_logger = DataLogger()

    async def call_instance_method(
            self, obj: Any, method_name: str, args: tuple, kwargs: dict
    ) -> Any:
        return await getattr(obj, method_name)(*args, **kwargs)

    def call_instance_method_gen(
            self, obj: Any, method_name: str, args: tuple, kwargs: dict
    ) -> Any:
        return getattr(obj, method_name)(*args, **kwargs)

    def encode(self, data):
        return self.registry.encode_json(data)

    def decode(self, data):
        return self.registry.decode_json(data)

    async def _create_instance(self, data: dict) -> Tuple[Any, dict]:
        hash_name = data['hash_name']
        triple = data['cls_name'], data['module'], data['qual_name']
        args = data['args']
        kwargs = data['kwargs']
        config = data['config']

        obj = self.registry.create_instance(
            triple, hash_name, args, kwargs, config)
        await self.executor.ensure_remote_instance(obj, *args, **kwargs)
        return obj, data

    async def _delete_instance(self, data: dict) -> Tuple[Any, dict]:
        hash_name = data['hash_name']
        obj = self.registry.delete_instance(hash_name)
        await self.executor.delete_remote_instance(obj)
        return obj, data

    async def _execute(self, data: dict) -> Tuple[Any, dict]:
        method_name = data['method_name']
        args = data.pop('args')
        kwargs = data.pop('kwargs')
        callback = data['callback']

        obj = self.registry.get_instance(data['hash_name'])

        res = await self.executor.execute(
            obj, getattr(obj, method_name), args, kwargs, callback)
        data['return_value'] = res

        return res, data

    async def _execute_generator(self, data: dict):
        method_name = data['method_name']
        args = data.pop('args')
        kwargs = data.pop('kwargs')
        callback = data['callback']

        obj = self.registry.get_instance(data['hash_name'])
        gen = self.executor.execute_generator(
            obj, getattr(obj, method_name), args, kwargs, callback)

        async with aclosing(gen) as aiter:
            async for res in aiter:
                data['return_value'] = res
                yield res, data

    async def _get_objects(self, data: dict) -> List[str]:
        return list(self.registry.hashed_instances.keys())

    async def _get_object_config(self, data: dict) -> dict:
        registry = self.registry
        hash_name = data['hash_name']

        obj = None
        if hash_name is not None:
            obj = registry.get_instance(hash_name)

        if obj is not None:
            data = read_config_from_object(obj)
        else:
            data = {h: read_config_from_object(o)
                    for h, o in registry.hashed_instances.items()}

        return data

    async def _get_object_data(self, data: dict) -> dict:
        properties = data['properties']
        hash_name = data['hash_name']
        obj = self.registry.get_instance(hash_name)

        return {k: getattr(obj, k) for k in properties}

    def _start_logging_object_data(self, data: dict, log_callback: Callable):
        trigger_names = data['trigger_names']
        triggered_logged_names = data['triggered_logged_names']
        logged_names = data['logged_names']
        hash_name = data['hash_name']

        obj = self.registry.get_instance(hash_name)
        binding = self.stream_data_logger.start_logging(
            log_callback, obj, hash_name, trigger_names,
            triggered_logged_names, logged_names)
        return binding

    def _stop_logging_object_data(self, binding):
        self.stream_data_logger.stop_logging(*binding)

    def _get_clock_data(self, data: dict) -> dict:
        return {'server_time': time.perf_counter_ns()}


class DataLogger:
    """Data logger used to log all data updates and stream it to clients.
    """

    def start_logging(
            self, callback: Callable, obj: Any, hash_name: str,
            trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = ()):
        """logged_names cannot have events if trigger is not empty.

        Can't have prop bound as trigger and as name without trigger
        (causes dups in SSELogger).
        """
        binding = []
        add_uid = binding.append

        fbind = obj.fbind
        for name in set(logged_names):
            if name.startswith('on_'):
                uid = fbind(
                    name, self.log_event_callback, name, hash_name, callback)
            else:
                uid = fbind(
                    name, self.log_property_callback, name, hash_name,
                    callback)
            add_uid((name, uid))

        # keep original sort in case it matters
        tracked_props = list({k: None for k in triggered_logged_names})
        for name in set(trigger_names):
            if name.startswith('on_'):
                uid = fbind(
                    name, self.log_trigger_event_callback, name, tracked_props,
                    hash_name, callback)
            else:
                uid = fbind(
                    name, self.log_trigger_property_callback, name,
                    tracked_props, hash_name, callback)
            add_uid((name, uid))
        return obj, binding

    def stop_logging(self, obj, binding):
        unbind_uid = obj.unbind_uid
        for name, uid in binding:
            unbind_uid(name, uid)

    def log_item(
            self, hash_name, props=None, trigger_name=None,
            trigger_value=None):
        return {
            'logged_trigger_name': trigger_name,
            'logged_trigger_value': trigger_value,
            'logged_items': props or {},
            'hash_name': hash_name,
        }

    def log_property_callback(self, name, hash_name, callback, obj, value):
        res = self.log_item(hash_name, props={name: value})
        callback(res)

    def log_event_callback(self, name, hash_name, callback, obj, *args):
        res = self.log_item(hash_name, props={name: args})
        callback(res)

    def log_trigger_property_callback(
            self, name, tracked_props, hash_name, callback, obj, value):
        props = {k: getattr(obj, k) for k in tracked_props if k != name}

        res = self.log_item(
            hash_name, trigger_name=name, trigger_value=value, props=props)
        callback(res)

    def log_trigger_event_callback(
            self, name, tracked_props, hash_name, callback, obj, *args):
        props = {k: getattr(obj, k) for k in tracked_props}

        res = self.log_item(
            hash_name, trigger_name=name, trigger_value=args, props=props)
        callback(res)


class RemoteRegistry(InstanceRegistry):
    """Server side object registry.
    """

    def create_instance(
            self, cls_triple: Tuple[str, str, str], hash_name: str,
            args: tuple, kwargs: dict, config: dict) -> Any:
        obj = self.referenceable_classes[cls_triple](*args, **kwargs)
        apply_config(obj, config)

        self.hashed_instances[hash_name] = obj
        self.hashed_instances_ids[id(obj)] = hash_name
        return obj

    def delete_instance(self, hash_name: str):
        obj = self.hashed_instances.pop(hash_name)
        del self.hashed_instances_ids[id(obj)]
        return obj

    def get_instance(self, hash_name: str):
        return self.hashed_instances[hash_name]
