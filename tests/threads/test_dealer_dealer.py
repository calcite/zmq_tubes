import pytest
import time

import zmq

from zmq_tubes.threads import Tube, TubeNode

ADDR = 'ipc:///tmp/dealer_dealer.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21']


@pytest.fixture(params=[{'server': True}])
def dealer_node1(request, data):
    def __process(req):
        data.remove(req.payload)
        req.tube.send(req.create_response(f'DEALER1-{req.payload[-2:]}'))

    tube = Tube(
        name='DEALER1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.DEALER
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process, tube)
    return node


@pytest.fixture(params=[{'server': False}])
def dealer_node2(request):
    tube = Tube(
        name='DEALER2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.DEALER
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


def test_dealer_dealer(dealer_node1, dealer_node2, data):
    res = []

    def __process(req):
        res.append(req.payload)

    dealer_node2.register_handler(f"{TOPIC}/#", __process)

    with dealer_node1, dealer_node2:
        for it in data.copy():
            dealer_node2.send(f"{TOPIC}/A", it)
        time.sleep(.5)

    assert len(res) == 4
    assert len(data) == 0


def test_dealer_dealer_on_same_node(dealer_node1, data):
    res = []

    def __process(req):
        res.append(req.payload)

    tube = Tube(
        name='DEALER1',
        addr=ADDR,
        server=False,
        tube_type=zmq.DEALER
    )

    dealer_node1.register_tube(tube, f"{TOPIC}/#")
    dealer_node1.register_handler(f"{TOPIC}/#", __process, tube)

    with dealer_node1:
        for it in data.copy():
            dealer_node1.send(f"{TOPIC}/A", it)
        time.sleep(1)

    assert len(res) == 4
    assert len(data) == 0
