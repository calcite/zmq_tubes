import asyncio

import zmq

from helpers import run_test_tasks
from tube import Tube, TubeNode

ADDR = 'ipc:///tmp/sub_pub.pipe'
TOPIC = 'sub'


def test_sub_pubs():
    """
        The subscriber is server and two clients publish messages.
    """
    data = ['PUB10', 'PUB11', 'PUB20', 'PUB21']
    data2 = ['PUB10', 'PUB11', 'PUB20', 'PUB21']

    async def pub_task(tube, topic, name):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            tube.publish(topic, f"{name}{it}")
            await asyncio.sleep(1)

    async def sub_task(tube, topic, name):
        async def __process(response):
            assert response.payload in data
            data.remove(response.payload)

        async def __process2(response):
            assert response.payload in data2
            data2.remove(response.payload)
        asyncio.current_task().set_name(name)
        tube.subscribe(topic, __process)
        tube.subscribe(topic, __process2)
        await tube.start()

    tube = Tube(
        name='SUB',
        addr=ADDR,
        type='server',
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

    sub_tube = TubeNode()
    sub_tube.register_tube(tube, f"{TOPIC}/#")
    sub_tube.connect()

    pub_tube1 = TubeNode()
    pub_tube1.register_tube(tube1, f"{TOPIC}/#")
    pub_tube1.connect()

    pub_tube2 = TubeNode()
    pub_tube2.register_tube(tube2, f"{TOPIC}/#")
    pub_tube2.connect()

    asyncio.run(
        run_test_tasks(
            [
                pub_task(pub_tube1, TOPIC, 'PUB1'),
                pub_task(pub_tube2, TOPIC, 'PUB2')
            ],
            [sub_task(sub_tube, f"{TOPIC}/#", 'SUB')],
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

    async def pub_task(tube, topic, name):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            tube.publish(topic, f"PUB{it}")
            await asyncio.sleep(1)

    async def sub_task(tube, topic, data, name):
        async def __process(response):
            assert response.payload in data
            data.remove(response.payload)
        asyncio.current_task().set_name(name)
        tube.subscribe(topic, __process)
        await tube.start()

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
        type='server',
        tube_type=zmq.PUB
    )
    tube_pub.connect()

    node_sub1 = TubeNode()
    node_sub1.register_tube(tube_sub1, f"{TOPIC}/#")
    node_sub1.connect()

    node_sub2 = TubeNode()
    node_sub2.register_tube(tube_sub2, f"{TOPIC}/#")
    node_sub2.connect()

    node_pub = TubeNode()
    node_pub.register_tube(tube_pub, f"{TOPIC}/#")
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
