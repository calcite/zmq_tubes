import os
import time

import pytest

import zmq

from ..helpers import run_test_threads, wrapp, wait_for_result2
from zmq_tubes.threads import Tube, TubeNode, Context

ADDR = 'ipc:///tmp/sub_pub.pipe'
os.path.exists(ADDR.replace('ipc://', '')) and os.remove(ADDR.replace('ipc://', ''))
TOPIC = 'sub'


@pytest.fixture
def data():
    return ['PUB10', 'PUB11', 'PUB20', 'PUB21'].copy()


@pytest.fixture
def data2():
    return ['PUB10', 'PUB11', 'PUB20', 'PUB21'].copy()


@pytest.fixture
def result():
    return []


@pytest.fixture
def result2():
    return []


@pytest.fixture(params=[{'server': True, 'utf8_decoding': True}])
def sub_node(result, request):
    def __process(req):
        result.append(req.payload)

    tube = Tube(
        name='SUB1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.SUB,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': True, 'utf8_decoding': True}])
def sub_node2(result2, request):
    def __process(req):
        result2.append(req.payload)

    tube = Tube(
        name='SUB2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.SUB,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
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
        tube_type=zmq.PUB,
        context=Context.instance()
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
        tube_type=zmq.PUB,
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


################################################################################
#   Tests
################################################################################


def test_sub_pubs(sub_node, pub_node1, pub_node2, data, data2, result):
    """
        The subscriber is server and two clients publish messages.
    """
    @wrapp
    def __process(node, p, d):
        while d:
            node.publish(f"{TOPIC}/{p}", d.pop())
            time.sleep(0.05)

    result.clear()
    with sub_node, pub_node1, pub_node2:
        run_test_threads(
            __process(pub_node1, 'A', data),
            __process(pub_node2, 'B', data2),
        )
        assert wait_for_result2(lambda: len(result) == 8, timeout=5)


@pytest.mark.parametrize("sub_node,sub_node2,pub_node1",
                         [({'server': False, 'utf8_decoding': True},
                           {'server': False, 'utf8_decoding': True},
                           {'server': True})],
                         indirect=["sub_node", "sub_node2", "pub_node1"])
def test_pub_subs(sub_node, sub_node2, pub_node1, data, data2, result, result2):
    """
        The subscriber is server and two clients publish messages.
    """
    @wrapp
    def __process(node, p, d):
        while d:
            node.publish(f"{TOPIC}/{p}", d.pop())

    result.clear()
    result2.clear()
    with pub_node1, sub_node, sub_node2:
        run_test_threads(
            __process(pub_node1, 'A', data),
        )
        assert wait_for_result2(
            lambda: len(result) == 4 and len(result2) == 4,
            timeout=5
        )


def test_pub_sub_on_same_node(sub_node, data, result):
    """
        The publisher and client on the same node.
    """

    def __process(node, p, d):
        while d:
            node.publish(f"{TOPIC}/{p}", d.pop())

    result.clear()
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
        assert wait_for_result2(lambda: len(result) == 4, timeout=5)


@pytest.mark.parametrize("sub_node",
                         [({'server': True, 'utf8_decoding': False})],
                         indirect=["sub_node"])
def test_pub_sub_bytes(sub_node, pub_node1, result):
    result.clear()
    with sub_node, pub_node1:
        time.sleep(.1)  # we have to wait for server is ready
        pub_node1.publish(f"{TOPIC}/A", 'XXX')
        assert wait_for_result2(
            lambda: len(result) > 0 and isinstance(result.pop(), bytes),
            timeout=5
        )
