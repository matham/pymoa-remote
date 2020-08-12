from os import getpid
from threading import get_ident
from pymoa_remote.client import apply_executor, apply_generator_executor
from pymoa_remote.executor import ExecutorBase
import trio


class RandomDigitalChannel:

    _config_props_ = ('name', )

    changes = {}

    _name = 55

    duration = 12

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.changes = {name: [0, None, None] for name in (
            'init', 'method', 'callback', 'method_gen', 'setter', 'getter',
            'method_async', 'method_gen_async')}
        item = self.changes['init']
        item[0] += 1
        item[1], item[2] = getpid(), get_ident()

    @property
    def name(self):
        item = self.changes['getter']
        item[0] += 1
        item[1], item[2] = getpid(), get_ident()

        return self._name

    @name.setter
    def name(self, value):
        item = self.changes['setter']
        item[0] += 1
        item[1], item[2] = getpid(), get_ident()

        self._name = value

    @apply_executor
    def set_name(self, value):
        self.name = value

    @apply_executor
    def set_duration(self, value):
        self.duration = value

    def executor_callback(self, return_value):
        item = self.changes['callback']
        item[0] += 1
        item[1], item[2] = getpid(), get_ident()

    @apply_executor(callback=executor_callback)
    def read_state(self, value, raise_exception=False):
        item = self.changes['method']
        item[0] += 1
        item[1], item[2] = getpid(), get_ident()

        if raise_exception:
            raise ValueError('Well now...')

        return value * 2

    @apply_executor(callback=executor_callback)
    async def read_state_async(self, value, raise_exception=False):
        item = self.changes['method_async']
        item[0] += 1
        item[1], item[2] = getpid(), get_ident()

        if raise_exception:
            raise ValueError('Well now...')

        return value * 2

    @apply_generator_executor(callback=executor_callback)
    def generate_data(self, values):
        item = self.changes['method_gen']

        for value in values:
            item[0] += 1
            item[1], item[2] = getpid(), get_ident()

            if value == 'exception':
                raise ValueError('Well now...')
            yield value * 2

    @apply_generator_executor(callback=executor_callback)
    async def generate_data_async(self, values):
        item = self.changes['method_gen_async']

        for value in values:
            await trio.sleep(0)

            item[0] += 1
            item[1], item[2] = getpid(), get_ident()

            if value == 'exception':
                raise ValueError('Well now...')
            yield value * 2

    @apply_executor
    def get_changes(self):
        return self.changes

    @apply_executor
    def set_supports_coroutine(self, value):
        # monkey patch so we can test async, whatever part is supported
        ExecutorBase.supports_coroutine = value
