import pytest
import time

import zmq

from zmq_tubes.threads import Tube, TubeNode

ADDR = 'ipc:///tmp/dealer_router.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21']


@pytest.fixture(params=[{'server': True}])
def router_node(data, request):
    def __process(req):
        data.remove(req.payload)
        if 'REQ10' in req.payload:
            time.sleep(.3)
        return req.create_response(f'RESP{req.payload[-2:]}')

    tube = Tube(
        name='ROUTER',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.ROUTER
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process, tube)
    return node


@pytest.fixture(params=[{'server': False}])
def dealer_node(request):
    tube = Tube(
        name='DEALER',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.DEALER
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


def test_dealer_router(router_node, dealer_node, data):
    res = []

    def __process(req):
        res.append(req.payload)

    dealer_node.register_handler(f"{TOPIC}/#", __process)

    with router_node, dealer_node:
        for it in data.copy():
            dealer_node.send(f"{TOPIC}/A", it)
            time.sleep(.1)
        time.sleep(.1)

    assert len(res) == 4
    assert len(data) == 0


def test_dealer_router_on_same_node(router_node, data):
    res = []

    def __process(req):
        res.append(req.payload)

    tube = Tube(
        name='DEALER',
        addr=ADDR,
        server=False,
        tube_type=zmq.DEALER
    )
    router_node.register_tube(tube, f"{TOPIC}/#")
    router_node.register_handler(f"{TOPIC}/#", __process, tube)

    with router_node:
        for it in data.copy():
            router_node.send(f"{TOPIC}/A", it)
            time.sleep(.1)
        time.sleep(.1)

    assert len(res) == 4
    assert len(data) == 0
