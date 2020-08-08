import trio
from threading import get_ident


async def test_run_in_thread_executor(thread_device):
    ident = [None, None, get_ident()]

    remote_count = 0
    local_count = 0

    def remote_callback(*args):
        nonlocal remote_count
        remote_count += 1
        ident[1] = get_ident()

    def local_callback(*args):
        nonlocal local_count
        local_count += 1
        ident[0] = get_ident()

    thread_device.local_callback = local_callback
    thread_device.remote_callback = remote_callback

    assert thread_device.state is None
    assert thread_device.timestamp is None

    await thread_device.read_state()
    assert thread_device.state is not None
    assert local_count == 1
    assert remote_count == 1
    assert ident[0] == ident[2]
    assert ident[1] != ident[2]
    timestamp = thread_device.timestamp
    await trio.sleep(.01)

    await thread_device.read_state()
    assert thread_device.timestamp > timestamp
    assert local_count == 2
    assert remote_count == 2
    assert ident[0] == ident[2]
    assert ident[1] != ident[2]
