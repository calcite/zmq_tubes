#!/bin/python
import asyncio
import logging
import signal
import sys
import zmq


class LogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            record.taskname = asyncio.current_task().get_name()
        except:
            record.taskname = ''
        return super().format(record)



handler_stderr = logging.StreamHandler(sys.stdout)
handler_stderr.setFormatter(LogFormatter('%(levelname)s:%(taskname)s:%(name)s: %(message)s'))
logging.basicConfig(level=logging.DEBUG, handlers=[handler_stderr])

logger = logging.getLogger()

from manager import Manager

# List of graceful signals
graceful_signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)




async def graceful_shutdown(loop, sig=None):
    """Cleanup tasks tied to the service's shutdown."""
    if sig:
        logger.info(f"Received exit signal {sig.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception:
         pass
    loop.stop()


def handle_exception(loop, context):
    msg = context.get("exception", context["message"])
    thread = context.get('future')
    thread_name = thread.get_name() if thread and hasattr(thread, 'get_name') \
        else '-'
    logger.critical(f"[{thread_name}] Caught exception: {msg}",
                    exc_info=context.get("exception"))
    logger.info("Shutting down...")
    try:
        asyncio.create_task(graceful_shutdown(loop))
    except Exception:
        logging.exception("Close error")
        sys.exit(33)


async def test_loop(manager):
    while True:
        # manager.publish('foo/aaa/xxx', 'ABC')
        await manager.request('foo/aaa/xxx', 'client1')
        print('client1')

async def test_loop2(manager):
    while True:
        # manager.publish('foo/aaa/xxx', 'ABC')
        await manager.request('foo/aaa/xxx', 'client2')
        print('client2')

async def test_loop3(manager):
    while True:
        # manager.publish('foo/aaa/xxx', 'ABC')
        await manager.request('foo/aaa/xxx', 'client3')
        print('client3')

async def test(payload):
    # print(payload)
    if payload == 'client2':
        await asyncio.sleep(3)
        return 'rep2'
    elif payload == 'client3':
        # await asyncio.sleep(20)
        return 'rep3'
    return 'rep1'

def main(args):
    loop = asyncio.get_event_loop()
    logger.info("Start.")
    manager = Manager(args[1])
    # manager.subscribe('foo/#', test)
    manager.register_handler('foo/#', test)
    # Register graceful exit signals
    for sig in graceful_signals:
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(graceful_shutdown(loop, s)))
    loop.set_exception_handler(handle_exception)
    try:
        if args[1] != 'schema2.yml':
            loop.create_task(manager.loop(), name="XXX")
            loop.create_task(manager.loop(), name="XXX2")
        else:
            loop.create_task(test_loop(manager), name="test")
            loop.create_task(test_loop2(manager), name="test2")
        loop.run_forever()
    finally:
        manager.close()
        loop.close()
        logger.info("Successfully shutdown.")


if __name__ == "__main__":
    main(sys.argv)
