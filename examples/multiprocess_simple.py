from pymoa_remote.socket.multiprocessing_client import \
    MultiprocessSocketExecutor
from pymoa_remote.client import apply_executor, ExecutorContext
import trio
from os import getpid


class Demo:

    def result(self, value):
        print(f'result is "{value}" in process {getpid()}')

    @apply_executor(callback='result')
    def remote_func(self, name):
        # raise ValueError('Arggg')
        print(f'func got "{name}" in process {getpid()}')
        return name


async def main():
    demo = Demo()
    async with MultiprocessSocketExecutor(
            server='localhost', allow_import_from_main=True) as executor:
        with ExecutorContext(executor):
            await executor.ensure_remote_instance(demo, 'demo')
            res = await demo.remote_func("cheese")
            print(f'Executed result is "{res}" in process {getpid()}')
            await executor.delete_remote_instance(demo)


if __name__ == '__main__':
    trio.run(main)
