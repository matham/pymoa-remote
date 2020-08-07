import contextlib
from typing import Dict, List, Any, Callable, Tuple, AsyncGenerator, Union, \
    Iterable, Optional
import json
import struct
from itertools import accumulate
from functools import partial
from trio import TASK_STATUS_IGNORED
import base64

__all__ = (
    'NO_CALLBACK', 'ExecutorBase', 'InstanceRegistry')


NO_CALLBACK = '#@none'
"""Can be used with :func:`apply_executor` to indicate that no callback should
be used.
"""


class ExecutorBase:
    """Executor that can be used to execute a method in a different context,
    e.g. in a different thread or across the network in a server.

    It is not safe to be called concurrently, except execute/execute_generator.

    TODO: test that all obj methods raise exception if called before ensure
    TODO: an error should be raised if using executor outside with block
    """

    name = 'Executor'

    supports_coroutine = False

    supports_non_coroutine = True

    async def start_executor(self):
        raise NotImplementedError

    async def stop_executor(self):
        raise NotImplementedError

    async def execute(
            self, obj, fn: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None):
        # todo: remove sync from api in abstract class
        raise NotImplementedError

    async def execute_generator(
            self, obj, gen: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None,
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        raise NotImplementedError

    @classmethod
    def call_execute_callback(cls, obj, return_value, callback):
        callback = cls.get_execute_callback_func(obj, callback)
        if callback is None:
            return

        callback(return_value)

    @classmethod
    def call_execute_callback_func(cls, return_value, callback):
        if callback is None:
            return

        callback(return_value)

    @classmethod
    def get_execute_callback_func(cls, obj, callback):
        if callback is NO_CALLBACK:
            return None
        if callback is None:
            return None

        if not isinstance(callback, str):
            callback = callback.__name__
        return getattr(obj, callback)

    async def get_echo_clock(self) -> Tuple[int, int, int]:
        raise NotImplementedError

    async def __aenter__(self):
        await self.start_executor()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop_executor()

    async def remote_import(self, module):
        raise NotImplementedError

    async def register_remote_class(self, cls):
        raise NotImplementedError

    async def ensure_remote_instance(
            self, obj, hash_name, *args, auto_register_class=True, **kwargs):
        raise NotImplementedError

    async def delete_remote_instance(self, obj):
        raise NotImplementedError

    async def get_remote_objects(self):
        raise NotImplementedError

    async def get_remote_object_config(self, obj: Optional[Any]):
        raise NotImplementedError

    async def apply_config_from_remote(self, obj):
        raise NotImplementedError

    async def get_remote_object_property_data(
            self, obj: Any, properties: List[str]) -> dict:
        raise NotImplementedError

    async def apply_property_data_from_remote(
            self, obj: Any, properties: List[str]):
        raise NotImplementedError

    @contextlib.asynccontextmanager
    async def get_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            initial_properties: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        raise NotImplementedError

    async def apply_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            initial_properties: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED):
        raise NotImplementedError

    @contextlib.asynccontextmanager
    async def get_channel_from_remote(
            self, obj: Optional[Any], channel: str,
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        raise NotImplementedError

    async def apply_execute_from_remote(
            self, obj, exclude_self=True, task_status=TASK_STATUS_IGNORED):
        raise NotImplementedError

    def encode(self, data):
        raise NotImplementedError

    def decode(self, data):
        raise NotImplementedError


class InstanceRegistry:
    """Registry that contains objects know by the register that can be
    referenced.

    It registers classes for instantiation and uses a base64 name hash for
    identifying them. It registers coders for serializing them.
    """

    referenceable_classes: Dict[Tuple[str, str, str], Callable] = {}

    hashed_instances: Dict[str, Any] = {}

    hashed_instances_ids: Dict[int, str] = {}

    json_coders: Dict[str, Tuple[type, Callable, Callable]] = {}

    def __init__(self, **kwargs):
        super(InstanceRegistry, self).__init__(**kwargs)
        self.hashed_instances = {}
        self.hashed_instances_ids = {}

    @classmethod
    def is_class_registered(
            cls, class_to_register: type = None, class_triple=None):
        if class_triple is None:
            class_triple = (
                class_to_register.__name__, class_to_register.__module__,
                class_to_register.__qualname__)
        return class_triple in cls.referenceable_classes

    @classmethod
    def register_class(cls, class_to_register: type, triple=None):
        """Duplicated register raises error.
        """
        if triple is None:
            triple = (
                class_to_register.__name__, class_to_register.__module__,
                class_to_register.__qualname__)

        if triple in cls.referenceable_classes:
            raise ValueError(f'{class_to_register} already registered')

        cls.referenceable_classes[triple] = class_to_register

    @classmethod
    def convert_hash_to_base64(cls, hash_val: str) -> str:
        return base64.urlsafe_b64encode(
            hash_val.encode('utf8')).decode('ascii')

    @classmethod
    def convert_base64_to_hash(cls, encoded_hash: Union[bytes, str]) -> str:
        return base64.urlsafe_b64decode(encoded_hash).decode('utf8')

    @classmethod
    def register_json_coder(
            cls, name: str, class_to_register: type, encoder: Callable,
            decoder: Callable):
        cls.json_coders[f'__@@{name}'] = class_to_register, encoder, decoder

    def referenceable_json_decoder(self, dct: dict, buffers: list = None):
        if len(dct) != 1:
            return dct

        (name, value), = dct.items()
        if '__@@remote_object' == name:
            return self.hashed_instances[value]
        if '__@@base64' == name:
            return base64.standard_b64decode(value)
        if '__@@buff' == name:
            return buffers[value]

        json_coders = self.json_coders
        if name in json_coders:
            decoder = json_coders[name][2]
            return decoder(value)
        return dct

    def decode_json(self, data: str):
        return json.loads(data, object_hook=self.referenceable_json_decoder)

    def decode_json_buffers_header(self, header: bytes):
        magic, msg_len, json_bytes, num_buffers = struct.unpack('!4I', header)
        if magic != 0xc33f0f68:
            raise ValueError(f'Stream corrupted. Magic number {magic} '
                             f'doe not match 0xc33f0f68')
        return msg_len, json_bytes, num_buffers

    def decode_json_buffers(
            self, data: bytes, json_bytes: int, num_buffers: int):
        json_msg = data[:json_bytes].decode('utf8')
        buffer_lengths = struct.unpack(
            f'!{num_buffers}I',
            data[json_bytes: json_bytes + num_buffers * 4]
        )
        buff_flat = data[json_bytes + num_buffers * 4:]

        indices = [0] + list(accumulate(buffer_lengths))
        buffers = [buff_flat[s:e] for s, e in zip(indices[:-1], indices[1:])]

        decoder = partial(self.referenceable_json_decoder, buffers=buffers)
        return json.loads(json_msg, object_hook=decoder)

    def decode_json_buffers_raw(self, data: bytes):
        if len(data) < 16:
            raise ValueError('Unable to parse message header')

        msg_len, json_bytes, num_buffers = self.decode_json_buffers_header(
            data[:16])

        data = data[16:]
        if len(data) != msg_len:
            raise ValueError('Unable to parse message data')

        return self.decode_json_buffers(data, json_bytes, num_buffers)

    def encode_json_func(self, obj, buffers: list = None):
        hash_val = self.hashed_instances_ids.get(id(obj), None)
        if hash_val is not None:
            return {'__@@remote_object': hash_val}

        if isinstance(obj, (bytes, bytearray)):
            if buffers is None:
                data = base64.standard_b64encode(obj).decode('ascii')
                return {'__@@base64': data}

            i = len(buffers)
            buffers.append(obj)
            return {'__@@buff': i}

        for name, (cls, encoder, _) in self.json_coders.items():
            if isinstance(obj, cls):
                return {name: encoder(obj)}

        raise TypeError(f'Object of type {obj.__class__.__name__} '
                        f'is not JSON serializable')

    def encode_json(self, obj) -> str:
        return json.dumps(obj, default=self.encode_json_func)

    def prepare_json_buffers(self, obj) -> Tuple[bytes, List[bytes]]:
        buffers = []
        s = json.dumps(
            obj, default=partial(self.encode_json_func, buffers=buffers))
        return s.encode('utf8'), buffers

    def encode_json_buffers(self, obj) -> bytes:
        """Message is: magic number, size of dynamic message, size of json,
        number of buffers, json, list of size for each buffer, buffers.
        """
        json_bytes, buffers = self.prepare_json_buffers(obj)

        lengths = list(map(len, buffers))
        var_msg_len = sum(lengths) + len(json_bytes) + len(buffers) * 4

        header = struct.pack(
            '!4I', 0xc33f0f68, var_msg_len, len(json_bytes), len(lengths))
        encoded_lengths = struct.pack(f'!{len(lengths)}I', *lengths)

        return b''.join([header, json_bytes, encoded_lengths] + buffers)
