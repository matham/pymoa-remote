import pytest
from itertools import combinations, chain, product
import trio
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
    'process_executor', 'quart_socket_executor', 'quart_rest_executor']


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
