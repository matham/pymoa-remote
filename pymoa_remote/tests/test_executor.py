import pytest
from time import perf_counter_ns
from itertools import combinations, chain, product
import trio
from threading import get_ident
from pymoa_remote.executor import ExecutorBase
from pymoa_remote.exception import RemoteException
from pymoa_remote.client import ExecutorContext


def compare_dict(named_dict: dict, data: dict):
    for key, value in named_dict.items():
        if isinstance(key, tuple):
            compare_dict(value, data[key[0]])
        else:
            assert value == data[key]


executors = [
    'thread_executor', 'process_executor', 'quart_socket_executor',
    'quart_rest_executor']

remote_executors = [
    'quart_rest_executor']


@pytest.fixture(params=executors)
async def executor(
        request, thread_executor, process_executor, quart_socket_executor,
        quart_rest_executor):
    with ExecutorContext(locals()[request.param]) as context:
        yield context.executor


@pytest.fixture(params=remote_executors)
async def remote_executor(
        request, process_executor, quart_socket_executor, quart_rest_executor):
    with ExecutorContext(locals()[request.param]) as context:
        yield context.executor


async def test_get_objects(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel

    assert await executor.get_remote_objects() == []

    device = RandomDigitalChannel()
    async with executor.remote_instance(device, 'some_device'):
        assert await executor.get_remote_objects() == ['some_device']

    assert await executor.get_remote_objects() == []


async def test_config(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with executor.remote_instance(device, 'some_device'):
        assert await executor.get_remote_object_config(device) == {'name': 55}

        device.name = 'emm'
        config = await executor.get_remote_object_config(device)
        if executor.is_remote:
            assert config == {'name': 55}
        else:
            assert config == {'name': 'emm'}

        assert device.name == 'emm'
        if executor.is_remote:
            await executor.apply_config_from_remote(device)
            assert device.name == 55

        await device.set_name('flanken')
        assert await executor.get_remote_object_config(device) == {
            'name': 'flanken'}


async def test_execute(executor: ExecutorBase):
    # todo: test cancel and for all executor methods
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with executor.remote_instance(device, 'some_device'):
        assert not device.changes['callback'][0]
        assert not device.changes['method'][0]

        assert await device.read_state('sleeping') == 'sleeping' * 2

        assert device.changes['callback'][0] == 1
        assert device.changes['method'][0] == int(not executor.is_remote)

        changes = await device.get_changes()
        assert changes['callback'][0] == 1
        assert changes['method'][0] == 1

        with pytest.raises(
                RemoteException if executor.is_remote else ValueError):
            await device.read_state('sleeping', raise_exception=True)

        assert device.changes['callback'][0] == 1
        assert device.changes['method'][0] == 2 * int(not executor.is_remote)

        changes = await device.get_changes()
        assert changes['callback'][0] == 1
        assert changes['method'][0] == 2


async def test_execute_no_executor():
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    assert not device.changes['callback'][0]
    assert not device.changes['method'][0]

    assert await device.read_state('sleeping') == 'sleeping' * 2

    assert device.changes['callback'][0] == 1
    assert device.changes['method'][0] == 1

    changes = await device.get_changes()
    assert changes is device.changes

    with pytest.raises(ValueError):
        await device.read_state('sleeping', raise_exception=True)

    assert device.changes['callback'][0] == 1
    assert device.changes['method'][0] == 2


async def test_execute_async(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with executor.remote_instance(device, 'some_device'):
        ExecutorBase.supports_coroutine = True
        await device.set_supports_coroutine(True)

        assert not device.changes['callback'][0]
        assert not device.changes['method_async'][0]

        assert await device.read_state_async('sleeping') == 'sleeping' * 2

        assert device.changes['callback'][0] == 1
        assert device.changes['method_async'][0] == int(not executor.is_remote)

        changes = await device.get_changes()
        assert changes['callback'][0] == 1
        assert changes['method_async'][0] == 1

        with pytest.raises(
                RemoteException if executor.is_remote else ValueError):
            await device.read_state_async('sleeping', raise_exception=True)

        assert device.changes['callback'][0] == 1
        assert device.changes['method_async'][0] == 2 * int(
            not executor.is_remote)

        changes = await device.get_changes()
        assert changes['callback'][0] == 1
        assert changes['method_async'][0] == 2

        await device.set_supports_coroutine(False)
        ExecutorBase.supports_coroutine = False


async def test_execute_async_unsupported(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with executor.remote_instance(device, 'some_device'):
        with pytest.raises(ValueError):
            await device.read_state_async('sleeping')


async def test_execute_async_no_executor():
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    assert not device.changes['callback'][0]
    assert not device.changes['method_async'][0]

    assert await device.read_state_async('sleeping') == 'sleeping' * 2

    assert device.changes['callback'][0] == 1
    assert device.changes['method_async'][0] == 1

    changes = await device.get_changes()
    assert changes is device.changes

    with pytest.raises(ValueError):
        await device.read_state_async('sleeping', raise_exception=True)

    assert device.changes['callback'][0] == 1
    assert device.changes['method_async'][0] == 2


async def test_execute_generate(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    values = list(range(5)) + ['hello'] + list(range(5))
    n = len(values)

    async with executor.remote_instance(device, 'some_device'):
        assert not device.changes['callback'][0]
        assert not device.changes['method_gen'][0]

        async with device.generate_data(values) as aiter:
            new_values = []
            async for value in aiter:
                new_values.append(value)
        assert new_values == [v * 2 for v in values]

        assert device.changes['callback'][0] == n
        assert device.changes['method_gen'][0] == n * int(
            not executor.is_remote)

        changes = await device.get_changes()
        assert changes['callback'][0] == n
        assert changes['method_gen'][0] == n

        values[5] = 'exception'
        with pytest.raises(
                RemoteException if executor.is_remote else ValueError):
            async with device.generate_data(values) as aiter:
                new_values = []
                async for value in aiter:
                    new_values.append(value)
        assert new_values == [v * 2 for v in values][:5]

        assert device.changes['callback'][0] == n + 5
        assert device.changes['method_gen'][0] == (n + 6) * int(
            not executor.is_remote)

        changes = await device.get_changes()
        assert changes['callback'][0] == n + 5
        assert changes['method_gen'][0] == n + 6


async def test_execute_generate_async_unsupported(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    values = list(range(5)) + ['hello'] + list(range(5))

    async with executor.remote_instance(device, 'some_device'):
        with pytest.raises(ValueError):
            async with device.generate_data_async(values) as aiter:
                async for value in aiter:
                    pass


async def test_execute_generate_async_no_executor():
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    values = list(range(5)) + ['hello'] + list(range(5))
    n = len(values)

    assert not device.changes['callback'][0]
    assert not device.changes['method_gen_async'][0]

    async with device.generate_data_async(values) as aiter:
        new_values = []
        async for value in aiter:
            new_values.append(value)
    assert new_values == [v * 2 for v in values]

    assert device.changes['callback'][0] == n
    assert device.changes['method_gen_async'][0] == n

    changes = await device.get_changes()
    assert changes is device.changes

    values[5] = 'exception'
    with pytest.raises(ValueError):
        async with device.generate_data_async(values) as aiter:
            new_values = []
            async for value in aiter:
                new_values.append(value)
    assert new_values == [v * 2 for v in values][:5]

    assert device.changes['callback'][0] == n + 5
    assert device.changes['method_gen_async'][0] == (n + 6)


async def test_execute_generate_break(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    values = list(range(5)) + ['hello'] + list(range(5))

    async with executor.remote_instance(device, 'some_device'):
        # make sure breaking in the middle of a gen doesn't break anything so
        # try it multiple times
        for i in range(1, 4):
            g0 = device.changes['method_gen'][0]
            changes = await device.get_changes()
            g1 = changes['method_gen'][0]
            g2 = changes['callback'][0]
            count = 0

            async with device.generate_data(values) as aiter:
                async for value in aiter:
                    assert value == values[count] * 2

                    if count == 5:
                        break
                    count += 1

            # the gen maybe called more times than the client read (as many
            # times as the buffer allows), but the callback must wait
            assert device.changes['callback'][0] == i * 6
            if executor.is_remote:
                assert device.changes['method_gen'][0] == 0
            else:
                assert device.changes['method_gen'][0] - g0 >= 6

            changes = await device.get_changes()
            assert changes['callback'][0] - g2 >= 6
            assert changes['method_gen'][0] - g1 >= 6


async def test_properties(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel

    device = RandomDigitalChannel()
    async with executor.remote_instance(device, 'some_device'):
        assert await executor.get_remote_object_property_data(
            device, ['duration']) == {'duration': 12}

        device.duration = 93
        props = await executor.get_remote_object_property_data(
            device, ['duration'])
        if executor.is_remote:
            assert props == {'duration': 12}
        else:
            assert props == {'duration': 93}

        assert device.duration == 93
        if executor.is_remote:
            await executor.apply_property_data_from_remote(
                device, ['duration'])
            assert device.duration == 12

        await device.set_duration(270)
        assert await executor.get_remote_object_property_data(
            device, ['duration']) == {'duration': 270}


async def test_clock(executor: ExecutorBase):
    t1, t2, t3 = await executor.get_echo_clock()
    assert t3 >= t1


async def test_sleep_duration(executor: ExecutorBase):
    _, ts, _ = await executor.get_echo_clock()
    t = await executor.sleep(duration=.2)
    assert t - ts > 100_000_000


async def test_sleep_deadline(executor: ExecutorBase):
    _, ts, _ = await executor.get_echo_clock()
    t = await executor.sleep(deadline=ts + 200_000_000)
    assert t - ts > 100_000_000


async def test_sleep_no_arg(executor: ExecutorBase):
    with pytest.raises(RemoteException if executor.is_remote else ValueError):
        await executor.sleep()


async def test_register_class(executor: ExecutorBase):
    from pymoa_remote.tests.device import RandomDigitalChannel
    await executor.register_remote_class(RandomDigitalChannel)


async def test_import(executor: ExecutorBase):
    import os.path as mod
    await executor.remote_import(mod)


_channels = 'create', 'delete', 'execute', ''
_devices = 'first device', 'second device', ''
_channels_c = list(chain(*(
    combinations(_channels, i) for i in range(1, 5)
)))
_devices_c = list(chain(*(
    combinations(_devices, i) for i in range(1, 4)
)))


@pytest.mark.parametrize('channels,devices', product(_channels_c, _devices_c))
async def test_stream_channel_create_delete(
        remote_executor: ExecutorBase, nursery, channels, devices):
    from pymoa_remote.tests.device import RandomDigitalChannel
    log = []
    hash_0, hash_1 = _devices[:-1]

    async def read_channel(obj, channel, task_status=trio.TASK_STATUS_IGNORED):
        async with remote_executor.get_channel_from_remote(
                obj, channel, task_status) as aiter:
            async for val in aiter:
                log.append(val)

    for chan in channels:
        for dev in devices:
            await nursery.start(read_channel, dev, chan)

    device_0 = RandomDigitalChannel()
    async with remote_executor.remote_instance(device_0, hash_0):
        assert await device_0.read_state('troy') == 'troy' * 2

        device_1 = RandomDigitalChannel()
        async with remote_executor.remote_instance(device_1, hash_1):
            assert await device_1.read_state('apple') == 'apple' * 2

            assert await device_0.read_state('helen') == 'helen' * 2
            assert await device_1.read_state('pie') == 'pie' * 2

    create_msg = {
        ('data', ): {
            'auto_register_class': True,
            'cls_name': 'RandomDigitalChannel',
            'hash_name': '',
            'mod_filename': None,
            'module': 'pymoa_remote.tests.device',
            'qual_name': 'RandomDigitalChannel',
        },
        'hash_name': '',
        'stream': 'create'
    }

    execute_msg = {
        ('data', ): {
            'callback': 'executor_callback',
            'hash_name': '',
            'method_name': 'read_state',
            'return_value': ''
        },
        'hash_name': '',
        'stream': 'execute'
    }

    delete_msg = {
        ('data', ): {
            'hash_name': '',
        },
        'hash_name': '',
        'stream': 'delete'
    }

    def check_msg(dev_name, channel_name, msg, **kwargs):
        for k, v in kwargs.items():
            msg[('data', )][k] = v

        for d in (dev_name, ''):
            if d not in devices:
                continue

            msg['hash_name'] = msg[('data', )]['hash_name'] = dev_name
            for c in (channel_name, ''):
                if c not in channels:
                    continue
                compare_dict(msg, log.pop(0))

    # wait for all delete messages to arrive
    await trio.sleep(.01)

    # create first device
    check_msg(hash_0, 'create', create_msg)
    # execute first device
    check_msg(hash_0, 'execute', execute_msg, return_value='troy' * 2)

    # create second device
    check_msg(hash_1, 'create', create_msg)
    # execute second device
    check_msg(hash_1, 'execute', execute_msg, return_value='apple' * 2)

    # execute first device
    check_msg(hash_0, 'execute', execute_msg, return_value='helen' * 2)
    # execute second device
    check_msg(hash_1, 'execute', execute_msg, return_value='pie' * 2)

    # delete second device
    check_msg(hash_1, 'delete', delete_msg)
    # delete first device
    check_msg(hash_0, 'delete', delete_msg)


async def test_uuid(
        thread_executor, process_executor, quart_socket_executor,
        quart_rest_executor):
    uuids = {
        thread_executor._uuid: thread_executor,
        process_executor._uuid: process_executor,
        quart_socket_executor._uuid: quart_socket_executor,
        quart_rest_executor._uuid: quart_rest_executor,
    }

    assert len(uuids) == 4


async def test_data_streaming(remote_executor: ExecutorBase, nursery):
    from pymoa_remote.tests.device import BoundChannel

    hash_0, hash_1 = 'first device', 'second device'
    log = []
    expected = []

    def add_expected(hash_name, initial=False, **items):
        item = {
            'hash_name': hash_name,
            'logged_items': {},
            'logged_trigger_name': None,
            'logged_trigger_value': None
        }
        item['initial_properties' if initial else 'logged_items'] = items
        expected.append({'data': item})

    async def stream_data(obj, task_status=trio.TASK_STATUS_IGNORED):
        async with remote_executor.get_data_from_remote(
                obj, logged_names=['name', 'on_event'],
                initial_properties=['name', ],
                task_status=task_status) as aiter:
            async for val in aiter:
                log.append(val)

    device_0 = BoundChannel()
    async with remote_executor.remote_instance(device_0, hash_0):
        await nursery.start(stream_data, device_0)
        add_expected(hash_0, name=55, initial=True)

        await device_0.set_name('troy')
        add_expected(hash_0, name='troy')
        await device_0.dispatch_event('troy')
        add_expected(hash_0, on_event=['troy'])

        device_1 = BoundChannel()
        async with remote_executor.remote_instance(device_1, hash_1):
            await nursery.start(stream_data, device_1)
            add_expected(hash_1, name=55, initial=True)

            await device_1.dispatch_event('apple')
            add_expected(hash_1, on_event=['apple'])
            await device_1.set_name('apple')
            add_expected(hash_1, name='apple')

            await device_0.dispatch_event('helen')
            add_expected(hash_0, on_event=['helen'])
            await device_0.set_name('helen')
            add_expected(hash_0, name='helen')
            await device_1.set_name('pie')
            add_expected(hash_1, name='pie')
            await device_1.dispatch_event('pie')
            add_expected(hash_1, on_event=['pie'])

        await device_0.set_name('uh greek')
        add_expected(hash_0, name='uh greek')
        await device_0.dispatch_event('uh greek')
        add_expected(hash_0, on_event=['uh greek'])

    assert log == expected


async def test_executor_context_init():
    from pymoa_remote.threading import ThreadExecutor
    from pymoa_remote.tests.device import RandomDigitalChannel

    tid = get_ident()
    device = RandomDigitalChannel()

    async with ThreadExecutor(init_context=False) as executor:
        async with executor.remote_instance(device, 'some device'):
            assert await device.get_thread_ident() == tid

            with ExecutorContext(executor):
                executor_tid = await device.get_thread_ident()
                assert executor_tid != tid

    async with ThreadExecutor() as executor:
        async with executor.remote_instance(device, 'some device'):
            executor_tid = await device.get_thread_ident()
            assert executor_tid != tid

            async with ThreadExecutor() as executor2:
                async with executor2.remote_instance(device, 'some device'):
                    executor_tid2 = await device.get_thread_ident()
                    assert executor_tid != tid
                    assert executor_tid2 != executor_tid
                    assert executor_tid2 != tid

                    with ExecutorContext(executor):
                        executor_tid_ = await device.get_thread_ident()
                        assert executor_tid_ == executor_tid
