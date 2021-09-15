import asyncio

import zmq

from helpers import run_test_tasks
from tube.manager import TubeSocket, TubeManager

ADDR = 'ipc:///tmp/sub_pub.pipe'
TOPIC = 'sub'


def test_sub_pubs():
    """
        The subscriber is server and two clients publish messages.
    """
    data = ['PUB10', 'PUB11', 'PUB20', 'PUB21']
    data2 = ['PUB10', 'PUB11', 'PUB20', 'PUB21']

    async def pub_task(tube, topic, name):
        for it in range(0, 2):
            tube.publish(topic, f"{name}{it}")
            await asyncio.sleep(1)

    async def sub_task(tube, topic):
        async def __process(payload):
            assert payload in data
            data.remove(payload)

        async def __process2(payload):
            assert payload in data2
            data2.remove(payload)
        tube.subscribe(topic, __process)
        tube.subscribe(topic, __process2)
        await tube.start()

    sub_socket = TubeSocket(
        name='SUB',
        addr=ADDR,
        type='server',
        socket_type=zmq.SUB
    )
    sub_socket.connect()

    pub_socket1 = TubeSocket(
        name='PUB',
        addr=ADDR,
        socket_type=zmq.PUB
    )

    pub_socket2 = TubeSocket(
        name='PUB',
        addr=ADDR,
        socket_type=zmq.PUB
    )

    sub_tube = TubeManager()
    sub_tube.register_socket(sub_socket, f"{TOPIC}/#")
    sub_tube.connect()

    pub_tube1 = TubeManager()
    pub_tube1.register_socket(pub_socket1, f"{TOPIC}/#")
    pub_tube1.connect()

    pub_tube2 = TubeManager()
    pub_tube2.register_socket(pub_socket2, f"{TOPIC}/#")
    pub_tube2.connect()

    asyncio.run(
        run_test_tasks(
            [
                pub_task(pub_tube1, TOPIC, 'PUB1'),
                pub_task(pub_tube2, TOPIC, 'PUB2')
            ],
            [sub_task(sub_tube, f"{TOPIC}/#")],
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

    async def pub_task(tube, topic):
        for it in range(0, 2):
            tube.publish(topic, f"PUB{it}")
            await asyncio.sleep(1)

    async def sub_task(tube, topic, data):
        async def __process(payload):
            assert payload in data
            data.remove(payload)
        tube.subscribe(topic, __process)
        await tube.start()

    sub_socket1 = TubeSocket(
        name='SUB',
        addr=ADDR,
        socket_type=zmq.SUB
    )

    sub_socket2 = TubeSocket(
        name='SUB',
        addr=ADDR,
        socket_type=zmq.SUB
    )

    pub_socket = TubeSocket(
        name='PUB',
        addr=ADDR,
        type='server',
        socket_type=zmq.PUB
    )
    pub_socket.connect()

    sub_tube1 = TubeManager()
    sub_tube1.register_socket(sub_socket1, f"{TOPIC}/#")
    sub_tube1.connect()

    sub_tube2 = TubeManager()
    sub_tube2.register_socket(sub_socket2, f"{TOPIC}/#")
    sub_tube2.connect()

    pub_tube = TubeManager()
    pub_tube.register_socket(pub_socket, f"{TOPIC}/#")
    pub_tube.connect()

    asyncio.run(
        run_test_tasks(
            [pub_task(pub_tube, TOPIC)],
            [
                sub_task(sub_tube1, f"{TOPIC}/#", data),
                sub_task(sub_tube2, f"{TOPIC}/#", data2)
            ]
        )
    )
    assert len(data) == 0
    assert len(data2) == 0
