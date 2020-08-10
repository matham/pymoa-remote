from os import getpid
from threading import get_ident


async def test_run_in_thread_executor(thread_device):
    ident = get_ident()
    pid = getpid()

    changes = thread_device.changes
    for i in range(3):
        assert await thread_device.read_state('sideways') == 'sideways' * 2
        assert changes['callback'][0] == i + 1
        assert changes['method'][0] == i + 1
        assert changes['callback'][1] == pid
        assert changes['method'][1] == pid
        assert changes['callback'][2] == ident
        assert changes['method'][2] != ident

    async with thread_device.generate_data([1, 2, 3]) as aiter:
        async for _ in aiter:
            pass

    assert changes['callback'][0] == 6
    assert changes['method_gen'][0] == 3
    assert changes['callback'][1] == pid
    assert changes['method_gen'][1] == pid
    assert changes['callback'][2] == ident
    assert changes['method_gen'][2] != ident


async def test_no_executor():
    from pymoa_remote.tests.device import RandomDigitalChannel
    device = RandomDigitalChannel()

    ident = get_ident()
    pid = getpid()

    changes = device.changes
    for i in range(3):
        assert await device.read_state('sideways') == 'sideways' * 2
        assert changes['callback'][0] == i + 1
        assert changes['method'][0] == i + 1
        assert changes['callback'][1] == pid
        assert changes['method'][1] == pid
        assert changes['callback'][2] == ident
        assert changes['method'][2] == ident

    async with device.generate_data([1, 2, 3]) as aiter:
        async for _ in aiter:
            pass

    assert changes['callback'][0] == 6
    assert changes['method_gen'][0] == 3
    assert changes['callback'][1] == pid
    assert changes['method_gen'][1] == pid
    assert changes['callback'][2] == ident
    assert changes['method_gen'][2] == ident
