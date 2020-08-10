from os import getpid
from threading import get_ident
from pymoa_remote.client import apply_executor, apply_generator_executor


class RandomDigitalChannel:

    _config_props_ = ('name', )

    remote_callback = None

    local_callback = None

    changes = {}

    _name = 55

    duration = 12

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.changes = {name: [0, None, None] for name in (
            'init', 'method', 'callback', 'method_gen', 'setter', 'getter')}
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
        if self.local_callback is not None:
            self.local_callback(return_value)
        item = self.changes['callback']
        item[0] += 1
        item[1], item[2] = getpid(), get_ident()

    @apply_executor(callback=executor_callback)
    def read_state(self, value, raise_exception=False):
        if self.remote_callback is not None:
            self.remote_callback(value * 2)
        item = self.changes['method']
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

    @apply_executor
    def get_changes(self):
        return self.changes
