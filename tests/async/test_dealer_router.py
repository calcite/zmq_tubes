import asyncio
import sys

import zmq
import pytest

from ..helpers import run_test_tasks
from zmq_tubes import Tube, TubeNode, TubeMessage

pytestmark = pytest.mark.skipif(sys.version_info < (3, 8),
                                reason='requires python3.8')

ADDR = 'ipc:///tmp/dealer_router.pipe'
TOPIC = 'req'


def test_dealer_router():
    data = ['response-DEALER_REQ-0', 'response-DEALER_REQ-1']

    async def request_task(node, topic, name):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            node.send(topic, f"request-{name}-{it}")
        await asyncio.sleep(3)

    async def response_dealer_task(node, topic, name):
        async def __process(response: TubeMessage):
            assert response.payload in data
            data.remove(response.payload)
        asyncio.current_task().set_name(name)
        node.register_handler(topic, __process)
        await node.start()

    async def response_task(node, topic, name):
        async def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            return request.create_response(f'response-{request.payload[8:]}')

        asyncio.current_task().set_name(name)
        node.register_handler(topic, __process)
        await node.start()

    tube_dealer = Tube(
        name='DEALER',
        addr=ADDR,
        server=True,
        tube_type=zmq.DEALER
    )

    tube_router = Tube(
        name='ROUTER',
        addr=ADDR,
        tube_type=zmq.ROUTER
    )

    node_router = TubeNode()
    node_router.register_tube(tube_router, f"{TOPIC}/#")
    node_router.connect()

    node_dealer = TubeNode()
    node_dealer.register_tube(tube_dealer, f"{TOPIC}/#")
    node_dealer.connect()

    asyncio.run(
        run_test_tasks(
            [request_task(node_dealer, TOPIC, 'DEALER_REQ')],
            [response_dealer_task(node_dealer, f'{TOPIC}/#', 'DEALER_RESP'),
             response_task(node_router, f'{TOPIC}/#', 'ROUTER')]
        )
    )

    assert len(data) == 0


def test_dealer_router_on_same_node():
    data = ['response-DEALER_REQ-0', 'response-DEALER_REQ-1']

    async def request_task(node, topic, name, tube):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            node.send(topic, f"request-{name}-{it}", tube)
        await asyncio.sleep(3)

    async def response_dealer_task(node, topic, name, tube):
        async def __process(response: TubeMessage):
            assert response.payload in data
            data.remove(response.payload)
        asyncio.current_task().set_name(name)
        node.register_handler(topic, __process, tube)

    async def response_task(node, topic, name, tube):
        async def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            return request.create_response(f'response-{request.payload[8:]}')

        asyncio.current_task().set_name(name)
        node.register_handler(topic, __process, tube)
        await asyncio.sleep(0.5)  # Wait for response_dealer_task registered
        await node.start()

    tube_dealer = Tube(
        name='DEALER',
        addr=ADDR,
        tube_type=zmq.DEALER
    )

    tube_router = Tube(
        name='ROUTER',
        addr=ADDR,
        server=True,
        tube_type=zmq.ROUTER
    )

    node = TubeNode()
    node.register_tube(tube_router, f"{TOPIC}/#")
    node.register_tube(tube_dealer, f"{TOPIC}/#")
    node.connect()

    asyncio.run(
        run_test_tasks(
            [request_task(node, TOPIC, 'DEALER_REQ', tube_dealer)],
            [response_dealer_task(node, f'{TOPIC}/#', 'DEALER_RESP',
                                  tube_dealer),
             response_task(node, f'{TOPIC}/#', 'ROUTER',
                           tube_router)]
        )
    )

    assert len(data) == 0
