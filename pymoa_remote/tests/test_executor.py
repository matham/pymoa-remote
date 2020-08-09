import pytest
from pymoa_remote.executor import ExecutorBase
from pymoa_remote.exception import RemoteException
from pymoa_remote.client import ExecutorContext

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

        assert await device.read_state('sleeping') == 'sleeping'

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
        assert new_values == values

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
        assert new_values == values[:5]

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
