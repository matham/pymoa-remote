from pymoa_remote.socket.multiprocessing_client import \
    MultiprocessSocketExecutor
from pymoa_remote.client import apply_executor, ExecutorContext, \
    apply_generator_executor
import trio
from os import getpid
from threading import get_ident


class Demo:

    def result(self, value):
        print(f'Callback got "{value}" in process: {getpid()}, '
              f'thread: {get_ident()}')

    @apply_executor(callback='result')
    def echo(self, name):
        print(f'Echo "{name}" in process: {getpid()}, thread: {get_ident()}')
        return name * 2

    @apply_executor(callback='result')
    def crash(self):
        print(f'About to crash in process: {getpid()}, thread: {get_ident()}')
        raise ValueError('Arggg...I crashed')

    @apply_generator_executor(callback='result')
    def grow(self, value):
        print(f'Starting generator with "{value}" in process: {getpid()}, '
              f'thread: {get_ident()}')
        for i in range(20):
            new_value = f'{i}: {value * i}'
            print(f'Yielding "{new_value}" in process: {getpid()}, '
                  f'thread: {get_ident()}')
            yield new_value


async def do_demo(executor: MultiprocessSocketExecutor):
    demo = Demo()
    pid = getpid()
    tid = get_ident()

    async with executor.remote_instance(demo, 'demo'):
        res = await demo.echo("cheese")
        print(f'Executed result is "{res}" in process: {pid}, thread: {tid}')
        counter = 0

        async with demo.grow('*') as aiter:
            async for value in aiter:
                print(f'Received "{value}" in process: {pid}, thread: {tid}')
                counter += 1
                if counter == 15:
                    break

        await demo.crash()


async def main():
    async with MultiprocessSocketExecutor(
            server='127.0.0.1', allow_import_from_main=True) as executor:
        with ExecutorContext(executor):
            await do_demo(executor)


if __name__ == '__main__':
    trio.run(main)
