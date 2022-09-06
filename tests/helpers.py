import asyncio
import logging
import threading
from threading import Thread


def _handle_task_result(task: asyncio.Task) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logging.exception('Exception raised by task = %r', task)


async def run_test_tasks(finite_tasks, infinite_tasks, sleep=None):
    loop = asyncio.get_running_loop()

    infinite = set()
    for task in infinite_tasks:
        loop_task = loop.create_task(task)
        loop_task.add_done_callback(_handle_task_result)
        infinite.add(loop_task)
    await asyncio.sleep(1)

    finite = set()
    for task in finite_tasks:
        loop_task = loop.create_task(task)
        loop_task.add_done_callback(_handle_task_result)
        finite.add(loop_task)

    await asyncio.wait(finite, return_when=asyncio.ALL_COMPLETED)

    exs = [task.exception() for task in finite]

    [task.cancel() for task in infinite]
    if sleep:
        await asyncio.sleep(sleep)
    exs += await asyncio.gather(*infinite, return_exceptions=True)
    for ex in exs:
        if ex and not isinstance(ex, asyncio.exceptions.CancelledError):
            raise ex
    loop.stop()


class ExcThread(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exc = None

    def run(self):
        try:
            super().run()
        except Exception as ex:
            self.exc = ex


def run_test_threads(finite_tasks, infinite_tasks, timeout=20):

    infinite = set()
    for task in infinite_tasks:
        infinite.add(task())

    finite = set()
    for task in finite_tasks:
        tt = ExcThread(target=task)
        tt.start()
        finite.add(tt)

    for task in finite:
        task.join(timeout=timeout)
        if hasattr(task, 'exc') and task.exc:
            raise task.exc


def wrapp(fce):
    """
    This wrap function and return it as callback
    """
    def step(*args, **kwargs):
        return lambda: fce(*args, **kwargs)
    return step


def cleanup_threads(fce):
    def step(*args, **kwargs):
        pre_num_threads = threading.active_count()
        if pre_num_threads != 1:
            logging.warning(
                "The threads from the previous test were not cleanup.")
        try:
            fce(*args, **kwargs)
        finally:
            [th.stop() for th in threading.enumerate()
             if th.isDaemon() and hasattr(th, 'stop')]
            num_threads = threading.active_count()
            if num_threads != pre_num_threads:
                names = [th for th in threading.enumerate()]
                logging.warning(
                    f"The threads from the this test were not cleanup !!! "
                    f"{names}")
    return step
