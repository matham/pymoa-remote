import pytest
import trio


@pytest.fixture
async def quart_app(nursery):
    from pymoa_remote.app.quart import create_app, run_app
    app = create_app()
    nursery.start_soon(run_app, app)
    await trio.sleep(.01)

    async with app.app_context():
        yield app


@pytest.fixture
async def quart_rest_executor(quart_app):
    from pymoa_remote.rest.client import RestExecutor
    from pymoa_remote.client import ExecutorContext
    async with RestExecutor(uri='http://127.0.0.1:5000') as executor:
        with ExecutorContext(executor):
            yield executor


@pytest.fixture
async def quart_socket_executor(quart_app, nursery):
    from pymoa_remote.socket.websocket_client import WebSocketExecutor
    from pymoa_remote.client import ExecutorContext
    async with WebSocketExecutor(
            nursery=nursery, server='127.0.0.1', port=5000) as executor:
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
    async with thread_executor.remote_instance(device, 'rand_device_process'):
        yield device
