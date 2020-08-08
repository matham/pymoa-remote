"""Threading
=============
"""

from typing import Tuple, AsyncGenerator, Union, Callable, Optional, Dict, \
    List, Iterable, Any
import threading
import math
import queue as stdlib_queue
import importlib
from asyncio import iscoroutinefunction
from queue import Empty
import contextlib
from async_generator import aclosing
import trio
from trio import TASK_STATUS_IGNORED
import outcome
import time
import logging
from tree_config import read_config_from_object, apply_config


from pymoa_remote.executor import NO_CALLBACK
from pymoa_remote.client import Executor


__all__ = (
    'ThreadExecutor', 'SyncThreadExecutor', 'AsyncThreadExecutor',
    'TrioPortal')


class ThreadExecutor(Executor):

    _obj_executor: Dict[
        int,
        List[Union[None, 'SyncThreadExecutor', 'AsyncThreadExecutor']]] = {}

    _sync_executor: Optional['SyncThreadExecutor'] = None

    _async_executor: Optional['AsyncThreadExecutor'] = None

    def __init__(self, **kwargs):
        super(ThreadExecutor, self).__init__(**kwargs)
        self._obj_executor = {}

    async def start_executor(self):
        if self._sync_executor is not None:
            raise TypeError('Executor already started')

        self._sync_executor = SyncThreadExecutor()
        self._async_executor = AsyncThreadExecutor()
        await self._sync_executor.start()
        await self._async_executor.start()

    async def stop_executor(self):
        items = list(self._obj_executor.values())
        self._obj_executor.clear()

        with trio.CancelScope(shield=True):
            for sync_exec, async_exec in items:
                if sync_exec is not None:
                    await sync_exec.stop()
                if async_exec is not None:
                    await async_exec.stop()

            if self._sync_executor is not None:
                await self._sync_executor.stop()
                self._sync_executor = None

            if self._async_executor is not None:
                await self._async_executor.stop()
                self._async_executor = None

    async def execute(
            self, obj, fn: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None):
        if callback is not None and not isinstance(callback, str):
            callback = callback.__name__

        executors = self._obj_executor[id(obj)]
        if iscoroutinefunction(fn):
            executor = executors[1]
            if executor is None:
                executor = executors[1] = AsyncThreadExecutor()
                await executor.start()
        else:
            executor = executors[0]
            if executor is None:
                executor = executors[0] = SyncThreadExecutor()
                await executor.start()

        return await executor.execute(obj, fn, args, kwargs, callback)

    async def execute_generator(
            self, obj, gen: Union[Callable, str], args=(), kwargs=None,
            callback: Union[Callable, str] = None,
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        task_status.started()

        if callback is not None and not isinstance(callback, str):
            callback = callback.__name__

        executors = self._obj_executor[id(obj)]
        if iscoroutinefunction(gen):
            executor = executors[1]
            if executor is None:
                executor = executors[1] = AsyncThreadExecutor()
                await executor.start()
        else:
            executor = executors[0]
            if executor is None:
                executor = executors[0] = SyncThreadExecutor()
                await executor.start()

        async with aclosing(
                executor.execute_generator(obj, gen, args, kwargs, callback)
        ) as aiter:
            async for value in aiter:
                yield value

    async def get_echo_clock(self) -> Tuple[int, int, int]:
        return await self._sync_executor.get_echo_clock()

    async def get_async_echo_clock(self) -> Tuple[int, int, int]:
        return await self._async_executor.get_echo_clock()

    async def remote_import(self, module):
        if not isinstance(module, str):
            module = module.__name__
        importlib.import_module(module)

    async def register_remote_class(self, cls):
        mod = importlib.import_module(cls.__module__)
        if cls.__name__ != cls.__qualname__:
            raise TypeError(f'Cannot register {cls}. Can only register module '
                            f'level classes')
        self.registry.register_class(getattr(mod, cls.__name__))

    async def ensure_remote_instance(
            self, obj, hash_name, *args, auto_register_class=True, **kwargs):
        if id(obj) in self._obj_executor:
            return

        cls = obj.__class__
        if auto_register_class and not self.registry.is_class_registered(
                class_to_register=cls):
            self.registry.register_class(cls)

        self._obj_executor[id(obj)] = [None, None]
        self.registry.add_instance(obj, hash_name)

    async def delete_remote_instance(self, obj):
        if id(obj) not in self._obj_executor:
            return

        self.registry.delete_instance(obj)
        sync_executor, async_executor = self._obj_executor.pop(id(obj))
        if sync_executor is not None:
            await sync_executor.stop()
        if async_executor is not None:
            await async_executor.stop()

    async def get_remote_objects(self):
        return list(self.registry.hashed_instances.keys())

    async def get_remote_object_config(self, obj: Optional[Any]):
        if obj is not None:
            return read_config_from_object(obj)
        return {h: read_config_from_object(o)
                for h, o in self.registry.hashed_instances.items()}

    async def apply_config_from_remote(self, obj):
        raise NotImplementedError

    async def get_remote_object_property_data(
            self, obj: Any, properties: List[str]) -> dict:
        return {k: getattr(obj, k) for k in properties}

    async def apply_property_data_from_remote(
            self, obj: Any, properties: List[str]):
        raise NotImplementedError

    @contextlib.asynccontextmanager
    async def get_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            initial_properties: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        raise NotImplementedError

    async def apply_data_from_remote(
            self, obj, trigger_names: Iterable[str] = (),
            triggered_logged_names: Iterable[str] = (),
            logged_names: Iterable[str] = (),
            initial_properties: Iterable[str] = (),
            task_status=TASK_STATUS_IGNORED):
        raise NotImplementedError

    @contextlib.asynccontextmanager
    async def get_channel_from_remote(
            self, obj: Optional[Any], channel: str,
            task_status=TASK_STATUS_IGNORED) -> AsyncGenerator:
        raise NotImplementedError

    async def apply_execute_from_remote(
            self, obj, exclude_self=True, task_status=TASK_STATUS_IGNORED):
        raise NotImplementedError


class SyncThreadExecutor:
    """Executor that executes functions in a secondary thread.
    """

    _thread: Optional[threading.Thread] = None

    _exec_queue: Optional[stdlib_queue.Queue] = None

    eof = object()

    _limiter: Optional[trio.Lock] = None

    max_queue_size = 10

    _thread_done_event: Optional[trio.Event] = None

    def __del__(self):
        if self._thread is not None:
            logging.warning(f'stop_executor was not called for "{self}"')

    async def start(self, name='ThreadExecutor'):
        queue = self._exec_queue = stdlib_queue.Queue()
        event = self._thread_done_event = trio.Event()
        self._limiter = trio.Lock()

        thread = self._thread = threading.Thread(
            target=self._worker_thread_fn, name=name,
            args=(queue, event, trio.lowlevel.current_trio_token()))
        thread.start()

    async def stop(self):
        if self._thread is None:
            return

        await self.execute(self.eof, None, callback=NO_CALLBACK)
        await self._thread_done_event.wait()

        self._thread = self._exec_queue = self._limiter = None
        self._thread_done_event = None

    def _worker_thread_fn(self, queue, event, start_token):
        eof = self.eof
        try:
            while True:
                obj, sync_fn, args, kwargs, task, token, gen_queue, \
                    gen_do_eof = queue.get(block=True)

                if obj is eof:
                    try:
                        token.run_sync_soon(trio.lowlevel.reschedule, task)
                    except trio.RunFinishedError:
                        pass
                    return

                if gen_queue is None:
                    # not a generator
                    result = outcome.capture(sync_fn, obj, *args, **kwargs)
                    try:
                        token.run_sync_soon(
                            trio.lowlevel.reschedule, task, result)
                    except trio.RunFinishedError:
                        # The entire run finished, so our particular tasks are
                        # certainly long gone - it must have cancelled.
                        # Continue eating the queue. Currently we cannot cancel
                        pass
                else:
                    # generator
                    send_channel, std_queue = gen_queue

                    def send_nowait():
                        try:
                            send_channel.send_nowait(None)
                        except trio.WouldBlock:
                            pass

                    put = std_queue.put
                    # we send to queue followed by ping on memory channel. We
                    # cannot deadlock, because the ping will result in removing
                    # at least one item, ensuring there will always be at least
                    # one item space free eventually, so put will only block
                    # for a short time until then. And then put will ping again
                    result = outcome.capture(sync_fn, obj, *args, **kwargs)
                    try:
                        # did the gen instantiation fail?
                        if isinstance(result, outcome.Error):
                            put(result, block=True)
                            token.run_sync_soon(send_nowait)
                            continue

                        gen = result.unwrap()  # get the actual gen
                        while gen_do_eof[0] is not eof:
                            result = outcome.capture(next, gen)
                            if isinstance(result, outcome.Error):
                                # stop iteration is signaled with None
                                if isinstance(result.error, StopIteration):
                                    result = None

                                put(result, block=True)
                                token.run_sync_soon(send_nowait)
                                break

                            put(result, block=True)
                            token.run_sync_soon(send_nowait)
                    except trio.RunFinishedError:
                        pass
        finally:
            try:
                start_token.run_sync_soon(event.set)
            except trio.RunFinishedError:
                pass

    @trio.lowlevel.enable_ki_protection
    async def execute(self, obj, sync_fn, args=(), kwargs=None, callback=None):
        """It's guaranteed sequential.
        TODO: if called after stop or thread exited it may block forever.
        """
        async with self._limiter:
            await trio.lowlevel.checkpoint_if_cancelled()
            self._exec_queue.put(
                (obj, sync_fn, args, kwargs or {}, trio.lowlevel.current_task(),
                 trio.lowlevel.current_trio_token(), None, None))

            def abort(raise_cancel):
                # cannot be canceled
                return trio.lowlevel.Abort.FAILED
            res = await trio.lowlevel.wait_task_rescheduled(abort)

            # we must execute under the lock to ensure ordered callbacks by
            # order of thread execution when execute is called from multiple
            # tasks
            if callback is not NO_CALLBACK:
                ThreadExecutor.call_execute_callback(obj, res, callback)
        return res

    async def execute_generator(
            self, obj, sync_gen, args=(), kwargs=None, callback=None
    ) -> AsyncGenerator:
        """Last items may be dropped after getting them.
        TODO: if called after stop is called it may block forever.
        """
        send_channel, receive_channel = trio.open_memory_channel(1)
        # we use this queue for back-pressure
        queue = stdlib_queue.Queue(maxsize=max(self.max_queue_size, 2))
        callback = ThreadExecutor.get_execute_callback_func(obj, callback)
        call_callback = ThreadExecutor.call_execute_callback_func
        do_eof = [None]
        token = trio.lowlevel.current_trio_token()

        async with self._limiter:
            await trio.lowlevel.checkpoint_if_cancelled()

            self._exec_queue.put(
                (obj, sync_gen, args, kwargs or {}, None, token,
                 (send_channel, queue), do_eof))

            try:
                # wait until signalled
                async for _ in receive_channel:
                    # get all items from the queue
                    while True:
                        try:
                            result = queue.get(block=False)
                        except Empty:
                            # wait for the next signal
                            break

                        # None means generator is done
                        if result is None:
                            return

                        result = result.unwrap()

                        call_callback(result, callback)
                        yield result
            finally:
                # if we are canceled, notify thread
                do_eof[0] = self.eof
                # get all the items so that eof is read in case thread is
                # blocking on full queue
                while True:
                    try:
                        queue.get(block=False)
                    except Empty:
                        break

    async def get_echo_clock(self) -> Tuple[int, int, int]:
        def get_time(*args):
            return time.perf_counter_ns()

        ts = time.perf_counter_ns()
        t = await self.execute(None, get_time, callback=NO_CALLBACK)
        return ts, t, time.perf_counter_ns()


class AsyncThreadExecutor:
    """Executor that executes async functions in a trio event loop in a
    secondary thread.
    """

    to_thread_portal: Optional['TrioPortal'] = None

    _thread: Optional[threading.Thread] = None

    _limiter: Optional[trio.Lock] = None

    cancel_nursery: Optional[trio.Nursery] = None

    _thread_done_event: Optional[trio.Event] = None

    def __del__(self):
        if self._thread is not None:
            logging.warning(f'stop_executor was not called for "{self}"')

    async def start(self, name='AsyncThreadExecutor'):
        # daemon=True because it might get left behind if we cancel, and in
        # this case shouldn't block process exit.
        event = trio.Event()
        from_thread_portal = TrioPortal()
        self._limiter = trio.Lock()
        done_event = self._thread_done_event = trio.Event()

        thread = self._thread = threading.Thread(
            target=self._worker_thread_fn, name=name,
            args=(event, from_thread_portal, done_event,
                  trio.lowlevel.current_trio_token()))
        thread.start()
        # wait until class variables are set
        await event.wait()

    async def stop(self):
        if not self._thread:
            return

        async def cancel(*args):
            self.cancel_nursery.cancel_scope.cancel()

        await self.execute(None, cancel, callback=NO_CALLBACK)
        await self._thread_done_event.wait()

        self._thread = self._limiter = self.to_thread_portal = None
        self.cancel_nursery = self._thread_done_event = None

    def _worker_thread_fn(
            self, event, from_thread_portal: 'TrioPortal', done_event,
            start_token):
        # This is the function that runs in the worker thread to do the actual
        # work
        async def runner():
            async with trio.open_nursery() as nursery:
                self.to_thread_portal = TrioPortal()
                self.cancel_nursery = nursery
                await from_thread_portal.run_sync(event.set)

                await trio.sleep(math.inf)

        try:
            trio.run(runner)
        finally:
            try:
                start_token.run_sync_soon(done_event.set)
            except trio.RunFinishedError:
                pass

    async def _execute_function(self, obj, async_fn, args, kwargs):
        return await async_fn(obj, *args, **kwargs)

    async def execute(
            self, obj, async_fn, args=(), kwargs=None, callback=None):
        # TODO: if called after stop or thread exited it may block forever.
        async with self._limiter:
            res = await self.to_thread_portal.run(
                self._execute_function, obj, async_fn, args, kwargs or {})

            if callback is not NO_CALLBACK:
                ThreadExecutor.call_execute_callback(obj, res, callback)
        return res

    async def execute_generator(
            self, obj, sync_gen, args=(), kwargs=None, callback=None
    ) -> AsyncGenerator:
        raise NotImplementedError

    async def get_echo_clock(self) -> Tuple[int, int, int]:
        async def get_time(*args):
            return time.perf_counter_ns()

        ts = time.perf_counter_ns()
        t = await self.execute(None, get_time, callback=NO_CALLBACK)
        return ts, t, time.perf_counter_ns()


class TrioPortal(object):
    """Portal for communicating with trio from a different thread.
    """

    trio_token: trio.lowlevel.TrioToken = None

    def __init__(self, trio_token=None):
        if trio_token is None:
            trio_token = trio.lowlevel.current_trio_token()
        self.trio_token = trio_token

    # This is the part that runs in the trio thread
    def _run_cb_async(self, afn, args, task, token):
        @trio.lowlevel.disable_ki_protection
        async def unprotected_afn():
            return await afn(*args)

        async def await_in_trio_thread_task():
            result = await outcome.acapture(unprotected_afn)
            try:
                token.run_sync_soon(trio.lowlevel.reschedule, task, result)
            except trio.RunFinishedError:
                # The entire run finished, so our particular tasks are certainly
                # long gone - it must have cancelled.
                pass

        trio.lowlevel.spawn_system_task(await_in_trio_thread_task, name=afn)

    def _run_sync_cb_async(self, fn, args, task, token):
        @trio.lowlevel.disable_ki_protection
        def unprotected_fn():
            return fn(*args)

        result = outcome.capture(unprotected_fn)
        try:
            token.run_sync_soon(trio.lowlevel.reschedule, task, result)
        except trio.RunFinishedError:
            # The entire run finished, so our particular tasks are certainly
            # long gone - it must have cancelled.
            pass

    @trio.lowlevel.enable_ki_protection
    async def _do_it_async(self, cb, fn, args):
        await trio.lowlevel.checkpoint_if_cancelled()
        self.trio_token.run_sync_soon(
            cb, fn, args, trio.lowlevel.current_task(),
            trio.lowlevel.current_trio_token())

        def abort(raise_cancel):
            return trio.lowlevel.Abort.FAILED
        return await trio.lowlevel.wait_task_rescheduled(abort)

    async def run(self, afn, *args):
        return await self._do_it_async(self._run_cb_async, afn, args)

    async def run_sync(self, fn, *args):
        return await self._do_it_async(self._run_sync_cb_async, fn, args)
