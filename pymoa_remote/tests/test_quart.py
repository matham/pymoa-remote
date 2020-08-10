import pytest
from os import getpid
from threading import get_ident
from pymoa_remote.client import ExecutorContext


@pytest.fixture(params=['quart_socket_executor', 'quart_rest_executor'])
async def device(request, quart_socket_executor, quart_rest_executor):
    from pymoa_remote.tests.device import RandomDigitalChannel

    with ExecutorContext(locals()[request.param]) as context:
        device = RandomDigitalChannel()
        async with context.executor.remote_instance(device, 'rand_device'):
            yield device


async def test_executor(device):
    ident = get_ident()
    pid = getpid()
    counter = 0
    counter_callback = 0
    changes = device.changes

    def verify_changes(method):
        assert changes['callback'][0] == counter_callback
        assert changes[method][0] == 0
        assert changes['callback'][1] == pid
        assert changes[method][1] is None
        assert changes['callback'][2] == ident
        assert changes[method][2] is None

        assert remote_changes['callback'][0] == counter_callback
        assert remote_changes[method][0] == counter
        assert remote_changes['callback'][1] == pid
        assert remote_changes[method][1] == pid
        assert remote_changes['callback'][2] != remote_changes[method][2]

    for _ in range(3):
        assert await device.read_state('sideways') == 'sideways' * 2

        counter += 1
        counter_callback += 1
        remote_changes = await device.get_changes()

        verify_changes('method')

    async with device.generate_data([1, 2, 3]) as aiter:
        async for _ in aiter:
            pass

    counter_callback += 3
    counter = 3
    remote_changes = await device.get_changes()
    verify_changes('method_gen')
