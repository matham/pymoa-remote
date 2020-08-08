import time
import random
from pymoa_remote.client import apply_executor, apply_generator_executor


class RandomDigitalChannel:

    state = None

    timestamp = None

    remote_callback = None

    local_callback = None

    def executor_callback(self, return_value):
        if self.local_callback is not None:
            self.local_callback()

        self.state, self.timestamp = return_value

    def _get_state_value(self):
        if self.remote_callback is not None:
            self.remote_callback()

        return random.random() >= 0.5, time.perf_counter()

    @apply_executor(callback=executor_callback)
    def read_state(self):
        return self._get_state_value()

    @apply_generator_executor(callback=executor_callback)
    def generate_data(self, num_samples):
        for _ in range(num_samples):
            yield random.random() >= 0.5, time.perf_counter()
