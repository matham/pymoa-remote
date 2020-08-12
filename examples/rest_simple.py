from pymoa_remote.socket.multiprocessing_client import \
    MultiprocessSocketExecutor
from pymoa_remote.client import apply_executor, ExecutorContext, \
    apply_generator_executor
from pymoa_remote.rest.client import RestExecutor
from pymoa_remote.app.quart import create_app, start_app
import trio
from async_generator import aclosing
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


async def do_demo(executor: RestExecutor):
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


async def main_external_server(host, port):
    """To use an external server, start the server as follows::

        pymoa_quart_app-script.py --import_from_main 1 --host 127.0.0.1 \
--port 5000

    Then start this function with the appropriate host and port.
    """
    async with RestExecutor(uri=f'{host}:{port}') as executor:
        with ExecutorContext(executor):
            await do_demo(executor)


async def main_internal_server():
    app = create_app(allow_import_from_main=True)

    async with app.app_context():
        nursery: trio.Nursery
        async with trio.open_nursery() as nursery:
            await nursery.start(start_app, app, '127.0.0.1', 5001)

            async with RestExecutor(uri='http://127.0.0.1:5001') as executor:
                with ExecutorContext(executor):
                    await do_demo(executor)

            nursery.cancel_scope.cancel()


if __name__ == '__main__':
    trio.run(main_internal_server)
    # to use the external server, it must first be started
    # trio.run(main_external_server, 'http://127.0.0.1', 5000)
