import asyncio
import sys

import zmq
import pytest

from ..helpers import run_test_tasks
from zmq_tubes import Tube, TubeNode, TubeMessage

pytestmark = pytest.mark.skipif(sys.version_info < (3, 8),
                                reason='requires python3.8')

ADDR = 'ipc:///tmp/dealer_dealer.pipe'
TOPIC = 'req'


def test_dealer_dealer():
    data = ['request-DEALER1_REQ-0', 'request-DEALER1_REQ-1',
            'request-DEALER2_REQ-0', 'request-DEALER2_REQ-1']

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

    tube_dealer1 = Tube(
        name='DEALER1',
        addr=ADDR,
        server=True,
        tube_type=zmq.DEALER
    )

    tube_dealer2 = Tube(
        name='DEALER2',
        addr=ADDR,
        tube_type=zmq.DEALER
    )

    node_dealer1 = TubeNode()
    node_dealer1.register_tube(tube_dealer1, f"{TOPIC}/#")
    node_dealer1.connect()

    node_dealer2 = TubeNode()
    node_dealer2.register_tube(tube_dealer2, f"{TOPIC}/#")
    node_dealer2.connect()

    asyncio.run(
        run_test_tasks(
            [request_task(node_dealer1, TOPIC, 'DEALER1_REQ'),
             request_task(node_dealer2, TOPIC, 'DEALER2_REQ')],
            [response_dealer_task(node_dealer1, f'{TOPIC}/#', 'DEALER1_RESP'),
             response_dealer_task(node_dealer2, f'{TOPIC}/#', 'DEALER2_RESP')]
        )
    )

    assert len(data) == 0


def test_dealer_dealer_on_same_node():
    data = ['request-DEALER1_REQ-0', 'request-DEALER1_REQ-1',
            'request-DEALER2_REQ-0', 'request-DEALER2_REQ-1']

    async def request_task(node, topic, name):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            node.send(topic, f"request-{name}-{it}")
        await asyncio.sleep(3)

    async def response_dealer_task(node, topic, name, tube):
        async def __process(response: TubeMessage):
            assert response.payload in data
            data.remove(response.payload)
        asyncio.current_task().set_name(name)
        node.register_handler(topic, __process, tube)
        await node.start()

    tube_dealer1 = Tube(
        name='DEALER1',
        addr=ADDR,
        server=True,
        tube_type=zmq.DEALER
    )

    tube_dealer2 = Tube(
        name='DEALER2',
        addr=ADDR,
        tube_type=zmq.DEALER
    )

    node_dealer1 = TubeNode()
    node_dealer1.register_tube(tube_dealer1, f"{TOPIC}/#")
    node_dealer1.register_tube(tube_dealer2, f"{TOPIC}/#")
    node_dealer1.connect()

    asyncio.run(
        run_test_tasks(
            [request_task(node_dealer1, TOPIC, 'DEALER1_REQ'),
             request_task(node_dealer1, TOPIC, 'DEALER2_REQ')],
            [response_dealer_task(node_dealer1, f'{TOPIC}/#',
                                  'DEALER1_RESP', tube_dealer1),
             response_dealer_task(node_dealer1, f'{TOPIC}/#',
                                  'DEALER2_RESP', tube_dealer2)]
        )
    )

    assert len(data) == 0
