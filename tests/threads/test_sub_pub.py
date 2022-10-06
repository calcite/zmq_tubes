import time

import pytest

import zmq

from ..helpers import run_test_threads, wrapp
from zmq_tubes.threads import Tube, TubeNode

ADDR = 'ipc:///tmp/sub_pub.pipe'
TOPIC = 'sub'


@pytest.fixture
def data():
    return ['PUB10', 'PUB11', 'PUB20', 'PUB21']


@pytest.fixture
def data2():
    return ['PUB10', 'PUB11', 'PUB20', 'PUB21']


@pytest.fixture(params=[{'server': True}])
def sub_node(data, request):
    def __process(req):
        data.remove(req.payload)

    tube = Tube(
        name='SUB1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.SUB
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': True}])
def sub_node2(data2, request):
    def __process(req):
        data2.remove(req.payload)

    tube = Tube(
        name='SUB2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.SUB
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': False}])
def pub_node1(request):
    tube = Tube(
        name='PUB1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.PUB
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


@pytest.fixture(params=[{'server': False}])
def pub_node2(request):
    tube = Tube(
        name='PUB2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.PUB
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


def test_sub_pubs(sub_node, pub_node1, pub_node2, data):
    """
        The subscriber is server and two clients publish messages.
    """

    @wrapp
    def __process(node, p, d):
        for it in d.copy():
            node.publish(f"{TOPIC}/{p}", it)

    with sub_node, pub_node1, pub_node2:
        run_test_threads(
            __process(pub_node1, 'A', data[0:2]),
            __process(pub_node2, 'B', data[2:]),
        )
        time.sleep(1)

    assert len(data) == 0


@pytest.mark.parametrize("sub_node,sub_node2,pub_node1",
                         [({'server': False}, {'server': False},
                           {'server': True})],
                         indirect=["sub_node", "sub_node2", "pub_node1"])
def test_pub_subs(sub_node, sub_node2, pub_node1, data, data2):
    """
        The subscriber is server and two clients publish messages.
    """

    @wrapp
    def __process(node, p, d):
        for it in d.copy():
            node.publish(f"{TOPIC}/{p}", it)

    with pub_node1, sub_node, sub_node2:
        run_test_threads(
            __process(pub_node1, 'A', data),
        )
        time.sleep(1)

    assert len(data) == 0
    assert len(data2) == 0


def test_pub_sub_on_same_node(sub_node, data):
    """
        The publisher and client on the same node.
    """

    def __process(node, p, d):
        for it in d.copy():
            node.publish(f"{TOPIC}/{p}", it)

    tube = Tube(
        name='PUB',
        addr=ADDR,
        server=False,
        tube_type=zmq.PUB
    )
    sub_node.register_tube(tube, f"{TOPIC}/#")

    with sub_node:
        time.sleep(.1)  # we have to wait for server is ready
        run_test_threads(
            __process(sub_node, 'A', data),
        )
        time.sleep(1)

    assert len(data) == 0
