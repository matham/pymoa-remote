import random
import trio
import time

from pymoa_remote.app.quart import create_app, start_app
from pymoa_remote.rest.client import RestExecutor
from pymoa_remote.threading import ThreadExecutor
from pymoa_remote.socket.websocket_client import WebSocketExecutor
from pymoa_remote.socket.multiprocessing_client import \
    MultiprocessSocketExecutor
from pymoa_remote.client import apply_executor, apply_generator_executor, \
    ExecutorContext
from pymoa_remote.executor import ExecutorBase

# monkey patch so we can test async, whatever part is supported
ExecutorBase.supports_coroutine = True


class RandomAnalogChannel:

    state = None

    timestamp = None

    def executor_callback(self, return_value):
        self.state, self.timestamp = return_value

    @apply_executor(callback=executor_callback)
    def read_state(self):
        return random.random(), time.perf_counter()

    @apply_generator_executor(callback=executor_callback)
    def generate_data(self, num_samples):
        for _ in range(num_samples):
            yield random.random(), time.perf_counter()

    @apply_executor(callback=executor_callback)
    async def read_state_async(self):
        return random.random(), time.perf_counter()

    @apply_generator_executor(callback=executor_callback)
    async def generate_data_async(self, num_samples):
        for _ in range(num_samples):
            yield random.random(), time.perf_counter()


async def measure_executor(executor, name):
    cls = executor.__class__
    device = RandomAnalogChannel()
    responses = []

    for _ in range(100):
        ts, t, te = await executor.get_echo_clock()
        responses.append((te - ts) / 1e6)
    response = sum(responses) / len(responses)

    await executor.ensure_remote_instance(device, 'analog_random')

    ts = time.perf_counter_ns()
    for _ in range(100):
        await device.read_state()

    te = time.perf_counter_ns()
    rate = 100 * 1e9 / (te - ts)

    async with device.generate_data(100) as aiter:
        async for _ in aiter:
            pass
    rate_cont = 100 * 1e9 / (time.perf_counter_ns() - te)

    await executor.delete_remote_instance(device)

    print(f'{name} - {cls.__name__}; '
          f'Round-trip lag: {response:.2f}ms. '
          f'Rate: {rate:.2f}Hz. '
          f'Continuous rate: {rate_cont:.2f}Hz')


async def measure_executor_async(executor, name):
    cls = executor.__class__
    device = RandomAnalogChannel()

    if hasattr(executor, 'get_async_echo_clock'):
        responses = []
        for _ in range(100):
            ts, t, te = await executor.get_async_echo_clock()
            responses.append((te - ts) / 1e6)
        response = sum(responses) / len(responses)
    else:
        response = 0

    await executor.ensure_remote_instance(device, 'analog_random')

    ts = time.perf_counter_ns()
    for _ in range(100):
        await device.read_state_async()

    te = time.perf_counter_ns()
    rate = 100 * 1e9 / (te - ts)

    rate_cont = 0

    await executor.delete_remote_instance(device)

    print(f'{name}-async - {cls.__name__}; '
          f'Round-trip lag: {response:.2f}ms. '
          f'Rate: {rate:.2f}Hz. '
          f'Continuous rate: {rate_cont:.2f}Hz')


async def measure_within_process_quart_lag(cls, port):
    app = create_app(allow_import_from_main=True)
    async with trio.open_nursery() as socket_nursery:
        if cls is RestExecutor:
            executor = RestExecutor(uri=f'http://127.0.0.1:{port}')
        else:
            executor = WebSocketExecutor(
                nursery=socket_nursery, server='127.0.0.1', port=port)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(start_app, app, "127.0.0.1", port)
            await trio.sleep(1)

            async with app.app_context():
                async with executor:
                    with ExecutorContext(executor):
                        await measure_executor(executor, 'Quart-internal')
                        await measure_executor_async(
                            executor, 'Quart-internal')

            nursery.cancel_scope.cancel()


async def measure_outside_process_quart_lag(cls, host='127.0.0.1', port=5000):
    async with trio.open_nursery() as socket_nursery:
        if cls is RestExecutor:
            executor = RestExecutor(uri=f'http://{host}:{port}')
        else:
            executor = WebSocketExecutor(
                nursery=socket_nursery, server=host, port=port)

        async with executor:
            with ExecutorContext(executor):
                await trio.sleep(1)
                await measure_executor(executor, 'Quart-external')
                await measure_executor_async(executor, 'Quart-external')


async def measure_multiprocess_lag(host='127.0.0.1', port=5000):
    async with MultiprocessSocketExecutor(
            server=host, port=port, allow_import_from_main=True) as executor:
        with ExecutorContext(executor):
            await trio.sleep(1)
            await measure_executor(executor, 'multiprocess')
            await measure_executor_async(executor, 'multiprocess')


async def measure_thread_lag():
    async with ThreadExecutor() as executor:
        with ExecutorContext(executor):
            await trio.sleep(1)
            await measure_executor(executor, 'local')
            await measure_executor_async(executor, 'local')


async def measure_no_executor_lag():
    device = RandomAnalogChannel()

    ts = time.perf_counter_ns()
    for _ in range(100):
        await device.read_state()
    rate = 100 * 1e9 / (time.perf_counter_ns() - ts)

    ts = time.perf_counter_ns()
    async with device.generate_data(100) as aiter:
        async for _ in aiter:
            pass
    rate_cont = 100 * 1e9 / (time.perf_counter_ns() - ts)

    print(f'No executor; Rate: {rate:.2f}Hz. '
          f'Continuous rate: {rate_cont:.2f}Hz')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='PyMoa performance tester.')
    parser.add_argument(
        '--host', dest='host', action='store', default="127.0.0.1")
    parser.add_argument(
        '--port', dest='port', action='store', default=5000, type=int)
    parser.add_argument(
        '--port_internal', dest='port_internal', action='store', default=5001,
        type=int)
    args = parser.parse_args()

    for cls in (RestExecutor, WebSocketExecutor):
        trio.run(measure_within_process_quart_lag, cls, args.port_internal)
        # only run if quart app is serving externally
        trio.run(measure_outside_process_quart_lag, cls, args.host, args.port)

    trio.run(measure_multiprocess_lag, args.host, args.port_internal)

    trio.run(measure_thread_lag)

    trio.run(measure_no_executor_lag)

    """
Quart-internal - RestExecutor; Round-trip lag: 12.28ms. Rate: 83.58Hz. \
Continuous rate: 1273.38Hz
Quart-internal-async - RestExecutor; Round-trip lag: 0.00ms. Rate: 83.80Hz. \
Continuous rate: 0.00Hz
Quart-external - RestExecutor; Round-trip lag: 10.40ms. Rate: 106.69Hz. \
Continuous rate: 729.84Hz
Quart-external-async - RestExecutor; Round-trip lag: 0.00ms. Rate: 111.63Hz. \
Continuous rate: 0.00Hz
Quart-internal - WebSocketExecutor; Round-trip lag: 1.53ms. Rate: 364.66Hz. \
Continuous rate: 1107.38Hz
Quart-internal-async - WebSocketExecutor; Round-trip lag: 0.00ms. \
Rate: 716.92Hz. Continuous rate: 0.00Hz
Quart-external - WebSocketExecutor; Round-trip lag: 1.91ms. Rate: 689.34Hz. \
Continuous rate: 1406.83Hz
Quart-external-async - WebSocketExecutor; Round-trip lag: 0.00ms. \
Rate: 693.38Hz. Continuous rate: 0.00Hz
multiprocess - MultiprocessSocketExecutor; Round-trip lag: 0.80ms. \
Rate: 988.16Hz. Continuous rate: 3241.82Hz
multiprocess-async - MultiprocessSocketExecutor; Round-trip lag: 0.00ms. \
Rate: 165.85Hz. Continuous rate: 0.00Hz
local - ThreadExecutor; Round-trip lag: 0.72ms. Rate: 2201.51Hz. \
Continuous rate: 5810.68Hz
local-async - ThreadExecutor; Round-trip lag: 0.54ms. Rate: 1573.17Hz. \
Continuous rate: 0.00Hz
No executor; Rate: 547645.13Hz. Continuous rate: 11848.06Hz

When connecting to an external RPi, we got:
Quart-external - RestExecutor; Round-trip lag: 16.56ms. Rate: 59.52Hz. \
Continuous rate: 847.97Hz
Quart-external - WebSocketExecutor; Round-trip lag: 6.14ms. Rate: 153.43Hz. \
Continuous rate: 377.19Hz
    """
