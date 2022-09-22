import asyncio

import zmq
import sys
import pytest

from ..helpers import run_test_tasks
from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/sub_pub.pipe'
TOPIC = 'sub'


def test_sub_pubs():
    """
        The subscriber is server and two clients publish messages.
    """
    data = ['PUB10', 'PUB11', 'PUB20', 'PUB21']
    data2 = ['PUB10', 'PUB11', 'PUB20', 'PUB21']

    async def pub_task(node, topic, name):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            node.publish(topic, f"{name}{it}")
            await asyncio.sleep(.3)

    async def sub_task(node, topic, name):
        async def __process(response):
            assert response.payload in data
            data.remove(response.payload)

        async def __process2(response):
            assert response.payload in data2
            data2.remove(response.payload)
        asyncio.current_task().set_name(name)
        node.subscribe(topic, __process)
        node.subscribe(topic, __process2)
        await node.start()

    tube = Tube(
        name='SUB',
        addr=ADDR,
        server=True,
        tube_type=zmq.SUB
    )

    tube1 = Tube(
        name='PUB1',
        addr=ADDR,
        tube_type=zmq.PUB
    )

    tube2 = Tube(
        name='PUB2',
        addr=ADDR,
        tube_type=zmq.PUB
    )

    sub_node = TubeNode()
    sub_node.register_tube(tube, f"{TOPIC}/#")

    pub_node1 = TubeNode()
    pub_node1.register_tube(tube1, f"{TOPIC}/#")
    # In the case of publishing with asyncio, the first message is very often
    # lost. The workaround is to connect the tube manually as soon as possible.
    pub_node1.connect()

    pub_node2 = TubeNode()
    pub_node2.register_tube(tube2, f"{TOPIC}/#")
    # In the case of publishing with asyncio, the first message is very often
    # lost. The workaround is to connect the tube manually as soon as possible.
    pub_node2.connect()

    asyncio.run(
        run_test_tasks(
            [
                pub_task(pub_node1, TOPIC, 'PUB1'),
                pub_task(pub_node2, TOPIC, 'PUB2')
            ],
            [sub_task(sub_node, f"{TOPIC}/#", 'SUB')],
        )
    )
    assert len(data) == 0
    assert len(data2) == 0


def test_pub_subs():
    """
        The publisher is server and two clients subscribe messages.
    """
    data = ['PUB0', 'PUB1']
    data2 = ['PUB0', 'PUB1']

    async def pub_task(node, topic, name):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            node.publish(topic, f"PUB{it}")
        await asyncio.sleep(2)

    async def sub_task(node, topic, data, name):
        async def __process(response):
            assert response.payload in data
            data.remove(response.payload)
        asyncio.current_task().set_name(name)
        node.subscribe(topic, __process)
        await node.start()

    tube_sub1 = Tube(
        name='SUB1',
        addr=ADDR,
        tube_type=zmq.SUB
    )

    tube_sub2 = Tube(
        name='SUB2',
        addr=ADDR,
        tube_type=zmq.SUB
    )

    tube_pub = Tube(
        name='PUB',
        addr=ADDR,
        server=True,
        tube_type=zmq.PUB
    )

    node_sub1 = TubeNode()
    node_sub1.register_tube(tube_sub1, f"{TOPIC}/#")

    node_sub2 = TubeNode()
    node_sub2.register_tube(tube_sub2, f"{TOPIC}/#")

    node_pub = TubeNode()
    node_pub.register_tube(tube_pub, f"{TOPIC}/#")
    # In the case of publishing with asyncio, the first message is very often
    # lost. The workaround is to connect the tube manually as soon as possible.
    node_pub.connect()

    asyncio.run(
        run_test_tasks(
            [pub_task(node_pub, TOPIC, 'PUB')],
            [
                sub_task(node_sub1, f"{TOPIC}/#", data, 'SUB1'),
                sub_task(node_sub2, f"{TOPIC}/#", data2, 'SUB2')
            ]
        )
    )
    assert len(data) == 0
    assert len(data2) == 0


def test_pub_sub_on_same_node():
    """
        The publisher and client on the same node.
    """
    data = ['PUB0', 'PUB1']

    async def pub_task(node, topic, name):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            node.publish(topic, f"PUB{it}")
        await asyncio.sleep(2)

    async def sub_task(node, topic, data, name):
        async def __process(response):
            assert response.payload in data
            data.remove(response.payload)
        asyncio.current_task().set_name(name)
        node.subscribe(topic, __process)
        await node.start()

    tube_sub = Tube(
        name='SUB1',
        addr=ADDR,
        tube_type=zmq.SUB
    )

    tube_pub = Tube(
        name='PUB',
        addr=ADDR,
        server=True,
        tube_type=zmq.PUB
    )
    # In the case of publishing with asyncio, the first message is very often
    # lost. The workaround is to connect the tube manually as soon as possible.
    tube_pub.connect()

    node_pub = TubeNode()
    node_pub.register_tube(tube_pub, f"{TOPIC}/#")
    node_pub.register_tube(tube_sub, f"{TOPIC}/#")

    asyncio.run(
        run_test_tasks(
            [pub_task(node_pub, TOPIC, 'PUB')],
            [
                sub_task(node_pub, f"{TOPIC}/#", data, 'SUB1'),
            ]
        )
    )
    assert len(data) == 0
