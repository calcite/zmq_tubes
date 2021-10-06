import asyncio
import logging


def _handle_task_result(task: asyncio.Task) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logging.exception('Exception raised by task = %r', task)


async def run_test_tasks(finite_tasks, infinite_tasks):
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
    exs += await asyncio.gather(*infinite, return_exceptions=True)
    for ex in exs:
        if ex and not isinstance(ex, asyncio.exceptions.CancelledError):
            raise ex
    loop.stop()
