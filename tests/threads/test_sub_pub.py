from time import sleep

import zmq

from ..helpers import run_test_threads, wrapp, cleanup_threads
from zmq_tubes.threads import Tube, TubeNode

ADDR = 'ipc:///tmp/sub_pub.pipe'
TOPIC = 'sub'


@cleanup_threads
def test_sub_pubs():
    """
        The subscriber is server and two clients publish messages.
    """
    data = ['PUB10', 'PUB11', 'PUB20', 'PUB21']
    data2 = ['PUB10', 'PUB11', 'PUB20', 'PUB21']

    @wrapp
    def pub_task(node, topic, name):
        sleep(2)
        for it in range(0, 2):
            node.publish(topic, f"{name}{it}")
        sleep(2)

    @wrapp
    def sub_task(node, topic, name):
        def __process(response):
            assert response.payload in data
            data.remove(response.payload)

        def __process2(response):
            assert response.payload in data2
            data2.remove(response.payload)

        node.subscribe(topic, __process)
        node.subscribe(topic, __process2)
        node.start()

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
    ).connect()

    tube2 = Tube(
        name='PUB2',
        addr=ADDR,
        tube_type=zmq.PUB
    ).connect()

    sub_node = TubeNode()
    sub_node.register_tube(tube, f"{TOPIC}/#")

    pub_node1 = TubeNode()
    pub_node1.register_tube(tube1, f"{TOPIC}/#")

    pub_node2 = TubeNode()
    pub_node2.register_tube(tube2, f"{TOPIC}/#")

    run_test_threads(
        [
            pub_task(pub_node1, TOPIC, 'PUB1'),
            pub_task(pub_node2, TOPIC, 'PUB2')
        ],
        [sub_task(sub_node, f"{TOPIC}/#", 'SUB')],
    )

    assert len(data) == 0
    assert len(data2) == 0


@cleanup_threads
def test_pub_subs():
    """
        The publisher is server and two clients subscribe messages.
    """
    data = ['PUB0', 'PUB1']
    data2 = ['PUB0', 'PUB1']

    @wrapp
    def pub_task(tube, topic, name):
        sleep(2)
        for it in range(0, 2):
            tube.publish(topic, f"PUB{it}")
        sleep(2)

    @wrapp
    def sub_task(tube, topic, data, name):
        def __process(response):
            assert response.payload in data
            data.remove(response.payload)
        tube.subscribe(topic, __process)
        tube.start()

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
    ).connect()

    node_sub1 = TubeNode()
    node_sub1.register_tube(tube_sub1, f"{TOPIC}/#")

    node_sub2 = TubeNode()
    node_sub2.register_tube(tube_sub2, f"{TOPIC}/#")

    node_pub = TubeNode()
    node_pub.register_tube(tube_pub, f"{TOPIC}/#")

    run_test_threads(
        [pub_task(node_pub, TOPIC, 'PUB')],
        [
            sub_task(node_sub1, f"{TOPIC}/#", data, 'SUB1'),
            sub_task(node_sub2, f"{TOPIC}/#", data2, 'SUB2')
        ]
    )
    assert len(data) == 0
    assert len(data2) == 0


@cleanup_threads
def test_pub_sub_on_same_node():
    """
        The publisher and client on the same node.
    """
    data = ['PUB0', 'PUB1']

    @wrapp
    def pub_task(tube, topic, name):
        sleep(2)
        for it in range(0, 2):
            tube.publish(topic, f"PUB{it}")
        sleep(2)

    @wrapp
    def sub_task(tube, topic, data, name):
        def __process(response):
            assert response.payload in data
            data.remove(response.payload)
        tube.subscribe(topic, __process)
        tube.start()

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
    ).connect()

    node = TubeNode()
    node.register_tube(tube_pub, f"{TOPIC}/#")
    node.register_tube(tube_sub, f"{TOPIC}/#")

    run_test_threads(
        [pub_task(node, TOPIC, 'PUB')],
        [
            sub_task(node, f"{TOPIC}/#", data, 'SUB1'),
        ]
    )
    assert len(data) == 0
