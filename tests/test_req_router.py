import asyncio
import zmq

from helpers import run_test_tasks
from tube import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/req_router.pipe'
TOPIC = 'req'


def test_req():

    async def request_task(tube, topic, name, number=4, timeout=30):
        asyncio.current_task().set_name(name)
        for it in range(0, number):
            response = await tube.request(topic, f"request-{name}-{it}",
                                          timeout=timeout)
            assert response.payload == f"response-{name}-{it}"

    async def response_task(tube, topic, name):
        async def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            if 'REQ2' in request.payload:
                await asyncio.sleep(15)
            return request.get_response(f'response-{request.payload[8:]}')

        asyncio.current_task().set_name(name)
        tube.register_handler(topic, __process)
        await tube.start()

    tube_req1 = Tube(
        name='REQ1',
        addr=ADDR,
        tube_type=zmq.REQ
    )
    tube_req2 = Tube(
        name='REQ2',
        addr=ADDR,
        tube_type=zmq.REQ
    )
    tube_router = Tube(
        name='ROUTER',
        addr=ADDR,
        type='server',
        tube_type=zmq.ROUTER
    )
    node_req1 = TubeNode()
    node_req1.register_tube(tube_req1, f"{TOPIC}/#")

    node_req2 = TubeNode()
    node_req2.register_tube(tube_req2, f"{TOPIC}/#")

    node_router = TubeNode()
    node_router.register_tube(tube_router, f"{TOPIC}/#")
    node_router.connect()

    asyncio.run(
        run_test_tasks(
            [request_task(node_req1, TOPIC, 'REQ1', timeout=3),
             request_task(node_req2, TOPIC, 'REQ2', number=1)],
            [response_task(node_router, f'{TOPIC}/#', 'ROUTER')]
        )
    )
