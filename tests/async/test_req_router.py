import asyncio
import zmq

from ..helpers import run_test_tasks
from zmq_tubes import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/req_router.pipe'
TOPIC = 'req'


def test_req():

    async def request_task(node, topic, name, number=4, timeout=30):
        asyncio.current_task().set_name(name)
        for it in range(0, number):
            response = await node.request(topic, f"request-{name}-{it}",
                                          timeout=timeout)
            assert response.payload == f"response-{name}-{it}"

    async def response_task(node, topic, name):
        async def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            if 'REQ2' in request.payload:
                await asyncio.sleep(2)
            return request.create_response(f'response-{request.payload[8:]}')

        asyncio.current_task().set_name(name)
        node.register_handler(topic, __process)
        await node.start()

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
        server=True,
        tube_type=zmq.ROUTER
    )
    node_req1 = TubeNode()
    node_req1.register_tube(tube_req1, f"{TOPIC}/#")

    node_req2 = TubeNode()
    node_req2.register_tube(tube_req2, f"{TOPIC}/#")

    node_router = TubeNode()
    node_router.register_tube(tube_router, f"{TOPIC}/#")

    asyncio.run(
        run_test_tasks(
            [request_task(node_req1, TOPIC, 'REQ1', timeout=3),
             request_task(node_req2, TOPIC, 'REQ2', number=1)],
            [response_task(node_router, f'{TOPIC}/#', 'ROUTER')]
        )
    )


def test_req_router_on_same_node():
    """
        The REQ/ROUTER and client on the same node.
    """

    async def request_task(node, topic, name, number=4, timeout=30):
        asyncio.current_task().set_name(name)
        for it in range(0, number):
            response = await node.request(topic, f"request-{name}-{it}",
                                          timeout=timeout)
            assert response.payload == f"response-{name}-{it}"

    async def response_task(node, topic, name):
        async def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            return request.create_response(f'response-{request.payload[8:]}')

        asyncio.current_task().set_name(name)
        node.register_handler(topic, __process)
        await node.start()

    tube1 = Tube(
        name='REQ',
        addr=ADDR,
        tube_type=zmq.REQ
    )

    tube2 = Tube(
        name='ROUTER',
        addr=ADDR,
        server=True,
        tube_type=zmq.ROUTER
    )

    node = TubeNode()
    node.register_tube(tube1, f"{TOPIC}/#")
    node.register_tube(tube2, f"{TOPIC}/#")

    asyncio.run(
        run_test_tasks(
            [request_task(node, TOPIC, 'REQ1', timeout=3)],
            [response_task(node, f'{TOPIC}/#', 'ROUTER')]
        )
    )
