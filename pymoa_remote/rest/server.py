"""Server
=========

"""
from async_generator import aclosing
from pymoa_remote.server import ExecutorServer

__all__ = ('RestServer', )


class RestServer(ExecutorServer):
    """Rest server side class to handle incoming executor requests.
    """

    async def ensure_instance(self, data: str) -> None:
        data = self.decode(data)

        hash_name = data['hash_name']
        if hash_name in self.registry.hashed_instances:
            return

        await self._create_instance(data)

    async def delete_instance(self, data: str) -> None:
        data = self.decode(data)
        hash_name = data['hash_name']
        if hash_name not in self.registry.hashed_instances:
            return

        await self._delete_instance(data)

    async def execute(self, data: str) -> str:
        data = self.decode(data)
        res = await self._execute(data)
        return self.encode(res)

    async def execute_generator(self, data: str):
        data = self.decode(data)

        async with aclosing(self._execute_generator(data)) as aiter:
            async for res in aiter:
                yield res

    async def get_objects(self, data: str) -> str:
        data = self.decode(data)
        res = await self._get_objects(data)
        return self.encode(res)

    async def get_object_config(self, data: str) -> str:
        data = self.decode(data)
        res = await self._get_object_config(data)
        return self.encode(res)

    async def get_object_data(self, data: str) -> str:
        data = self.decode(data)
        res = await self._get_object_data(data)
        return self.encode(res)

    async def start_logging_object_data(self, data: str, log_callback):
        data = self.decode(data)
        return self._start_logging_object_data(data, log_callback)

    async def stop_logging_object_data(self, binding):
        self._stop_logging_object_data(binding)

    async def get_echo_clock(self, data: str) -> str:
        data = self.decode(data)
        data = self._get_clock_data(data)
        return self.encode(data)
