from os import getpid
from threading import get_ident


async def test_executor(process_device):
    ident = get_ident()
    pid = getpid()
    counter = 0
    counter_callback = 0
    changes = process_device.changes

    def verify_changes(method):
        assert changes['callback'][0] == counter_callback
        assert changes[method][0] == 0
        assert changes['callback'][1] == pid
        assert changes[method][1] is None
        assert changes['callback'][2] == ident
        assert changes[method][2] is None

        assert remote_changes['callback'][0] == counter_callback
        assert remote_changes[method][0] == counter
        assert remote_changes['callback'][1] != pid
        assert remote_changes[method][1] != pid
        assert remote_changes['callback'][2] != remote_changes[method][2]

    for _ in range(3):
        assert await process_device.read_state('sideways') == 'sideways' * 2

        counter += 1
        counter_callback += 1
        remote_changes = await process_device.get_changes()

        verify_changes('method')

    async with process_device.generate_data([1, 2, 3]) as aiter:
        async for _ in aiter:
            pass

    counter_callback += 3
    counter = 3
    remote_changes = await process_device.get_changes()
    verify_changes('method_gen')
