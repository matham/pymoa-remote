"""Socket Server
================

"""

from pymoa_remote.server import ExecutorServer
from async_generator import aclosing

__all__ = ('SocketServer', )


class SocketServer(ExecutorServer):
    """Rest server side class to handle incoming executor requests.
    """

    def encode(self, data):
        return self.registry.encode_json_buffers(data)

    async def decode(self, data):
        raise NotImplementedError

    async def ensure_instance(self, data: dict) -> None:
        hash_name = data['hash_name']
        if hash_name in self.registry.hashed_instances:
            return

        await self._create_instance(data)

    async def delete_instance(self, data: dict) -> None:
        hash_name = data['hash_name']
        if hash_name not in self.registry.hashed_instances:
            return

        await self._delete_instance(data)

    async def execute(self, data: dict):
        return await self._execute(data)

    async def execute_generator(self, data: dict):
        async with aclosing(self._execute_generator(data)) as aiter:
            async for res, data in aiter:
                yield res

    async def get_objects(self, data: dict):
        return await self._get_objects(data)

    async def get_object_config(self, data: dict):
        return await self._get_object_config(data)

    async def get_object_data(self, data: dict):
        return await self._get_object_data(data)

    async def start_logging_object_data(self, data: dict, log_callback):
        return self._start_logging_object_data(data, log_callback)

    async def stop_logging_object_data(self, binding):
        self._stop_logging_object_data(binding)

    async def get_echo_clock(self, data: dict):
        return self._get_clock_data(data)
