import pytest
import trio
from trio import socket


async def get_socket_port(server):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    await s.bind((server, 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
async def quart_app(nursery):
    from pymoa_remote.app.quart import create_app, start_app
    app = create_app()
    port = await get_socket_port('127.0.0.1')
    app.pymoa_port = port

    async with app.app_context():
        await nursery.start(start_app, app, '127.0.0.1', port)
        yield app


@pytest.fixture
async def quart_rest_executor(quart_app):
    from pymoa_remote.rest.client import RestExecutor
    from pymoa_remote.client import ExecutorContext
    async with RestExecutor(
            uri=f'http://127.0.0.1:{quart_app.pymoa_port}') as executor:
        with ExecutorContext(executor):
            yield executor


@pytest.fixture
async def quart_socket_executor(quart_app, nursery):
    from pymoa_remote.socket.websocket_client import WebSocketExecutor
    from pymoa_remote.client import ExecutorContext
    async with WebSocketExecutor(
            nursery=nursery, server='127.0.0.1',
            port=quart_app.pymoa_port) as executor:
        with ExecutorContext(executor):
            yield executor


@pytest.fixture
async def thread_executor():
    from pymoa_remote.threading import ThreadExecutor
    from pymoa_remote.client import ExecutorContext
    async with ThreadExecutor() as executor:
        with ExecutorContext(executor):
            yield executor


@pytest.fixture
async def process_executor():
    from pymoa_remote.socket.multiprocessing_client import \
        MultiprocessSocketExecutor
    from pymoa_remote.client import ExecutorContext
    async with MultiprocessSocketExecutor(
            server='127.0.0.1', allow_import_from_main=True) as executor:
        with ExecutorContext(executor):
            yield executor


@pytest.fixture
async def quart_rest_device(quart_rest_executor):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with quart_rest_executor.remote_instance(device, 'rand_device_rest'):
        yield device


@pytest.fixture
async def quart_socket_device(quart_socket_executor):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with quart_socket_executor.remote_instance(
            device, 'rand_device_socket'):
        yield device


@pytest.fixture
async def thread_device(thread_executor):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with thread_executor.remote_instance(device, 'rand_device_thread'):
        yield device


@pytest.fixture
async def process_device(process_executor):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with process_executor.remote_instance(device, 'rand_device_process'):
        yield device


@pytest.fixture(params=['thread', 'process', 'websocket', 'rest'])
async def every_executor(request):
    if request.param == 'thread':
        executor, device = 'thread_executor', 'thread_device'
    elif request.param == 'process':
        executor, device = 'process_executor', 'process_device'
    elif request.param == 'websocket':
        executor, device = 'quart_socket_executor', 'quart_socket_device'
    elif request.param == 'rest':
        executor, device = 'quart_rest_executor', 'quart_rest_device'
    else:
        raise ValueError

    executor = request.getfixturevalue(executor)
    yield executor
