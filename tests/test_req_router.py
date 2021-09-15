import asyncio
import zmq

from helpers import run_test_tasks
from tube.manager import TubeSocket, TubeManager

ADDR = 'ipc:///tmp/req_resp.pipe'
TOPIC = 'req'


def test_req():

    async def request_task(tube, topic, name, number=4, timeout=30):
        for it in range(0, number):
            resp = await tube.request(topic, f"request-{name}-{it}",
                                      timeout=timeout)
            assert resp == f"response-{name}-{it}"

    async def response_task(tube, topic):
        async def __process(payload):
            if 'REQ2' in payload:
                await asyncio.sleep(20)
            assert payload[0:8] == 'request-'
            return f'response-{payload[8:]}'
        tube.register_handler(topic, __process)
        await tube.start()

    req_socket1 = TubeSocket(
        name='REQ',
        addr=ADDR,
        socket_type=zmq.REQ
    )
    req_socket2 = TubeSocket(
        name='REQ',
        addr=ADDR,
        socket_type=zmq.REQ
    )
    resp_socket = TubeSocket(
        name='ROUTER',
        addr=ADDR,
        type='server',
        socket_type=zmq.ROUTER
    )
    req_tube1 = TubeManager()
    req_tube1.register_socket(req_socket1, f"{TOPIC}/#")

    req_tube2 = TubeManager()
    req_tube2.register_socket(req_socket2, f"{TOPIC}/#")

    resp_tube = TubeManager()
    resp_tube.register_socket(resp_socket, f"{TOPIC}/#")
    resp_tube.connect()

    asyncio.run(
        run_test_tasks(
            [request_task(req_tube1, TOPIC, 'REQ1', timeout=3),
             request_task(req_tube1, TOPIC, 'REQ2', number=1)],
            [response_task(resp_tube, f'{TOPIC}/#')]
        )
    )
