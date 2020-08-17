from pymoa_remote import MultiprocessSocketExecutor, apply_executor, \
    apply_generator_executor
import trio
from os import getpid


class Demo:

    @apply_executor
    def echo(self, name):
        print(f'Echo "{name}" in process {getpid()}')
        return name * 2

    @apply_executor
    def crash(self):
        print(f'About to crash in process {getpid()}')
        raise ValueError('Arggg...I crashed')

    @apply_generator_executor
    def grow(self, value):
        print(f'Starting generator with "{value}" in process {getpid()}')
        for i in range(20):
            new_value = f'{i}: {value * i}'
            print(f'Yielding "{new_value}" in process {getpid()}')
            yield new_value


async def do_demo(executor: MultiprocessSocketExecutor):
    demo = Demo()
    pid = getpid()

    async with executor.remote_instance(demo, 'demo'):
        res = await demo.echo("cheese")
        print(f'Executed result is "{res}" in process {pid}')
        counter = 0

        async with demo.grow('*') as aiter:
            async for value in aiter:
                print(f'Received "{value}" in process {pid}')
                counter += 1
                if counter == 15:
                    break

        await demo.crash()


async def main():
    async with MultiprocessSocketExecutor(
            server='127.0.0.1', allow_import_from_main=True) as executor:
        await do_demo(executor)


if __name__ == '__main__':
    trio.run(main)
