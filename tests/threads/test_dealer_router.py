import os
import time

import zmq
import pytest

from tests.helpers import wait_for_result2 as wait_for_result
from zmq_tubes.threads import Tube, TubeNode, Context

ADDR = 'ipc:///tmp/dealer_router.pipe'
os.path.exists(ADDR.replace('ipc://', '')) and os.remove(ADDR.replace('ipc://', ''))
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21'].copy()


@pytest.fixture
def result():
    return []


@pytest.fixture(params=[{'server': True, 'utf8_decoding': True}])
def router_node(result, request):
    def __process(req):
        result.append(req.payload)
        if isinstance(req.payload, str) and 'REQ10' in req.payload:
            time.sleep(.3)
        return req.create_response(
            f'RESP1{req.payload[-2:]}' if request.param['utf8_decoding']
            else b'RESP1' + req.payload[-2:])

    tube = Tube(
        name='ROUTER',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.ROUTER,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process, tube)
    return node


@pytest.fixture(params=[{'server': False, 'utf8_decoding': True}])
def dealer_node(request):
    tube = Tube(
        name='DEALER1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.DEALER,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


################################################################################
#   Tests
################################################################################


def test_router_dealer(router_node, dealer_node, data, result):
    res = []

    def __process(req):
        res.append(req.payload)
    dealer_node.register_handler(f"{TOPIC}/#", __process)
    result.clear()
    with router_node, dealer_node:
        while data:
            dealer_node.send(f"{TOPIC}/A", data.pop())
        assert wait_for_result(
            lambda: len(res) == 4 and len(result) == 4,
            timeout=5
        )


def test_dealer_router_on_same_node(router_node, data, result):
    res = []

    def __process(req):
        res.append(req.payload)
    tube = Tube(
        name='DEALER',
        addr=ADDR,
        server=False,
        tube_type=zmq.DEALER
    )
    result.clear()
    router_node.register_tube(tube, f"{TOPIC}/#")
    router_node.register_handler(f"{TOPIC}/#", __process, tube)

    with router_node:
        while data:
            router_node.send(f"{TOPIC}/A", data.pop())
        assert wait_for_result(
            lambda: len(res) == 4 and len(result) == 4,
            timeout=5
        )


@pytest.mark.parametrize("router_node,dealer_node",
                         [({'server': True, 'utf8_decoding': False},
                           {'server': False, 'utf8_decoding': False})],
                         indirect=["router_node", "dealer_node"])
def test_router_dealer_bytes(router_node, dealer_node, result):
    res = []

    def __process(req):
        res.append(req.payload)
    dealer_node.register_handler(f"{TOPIC}/#", __process)
    result.clear()
    with router_node, dealer_node:
        dealer_node.send(f"{TOPIC}/A", 'XXX')
        assert wait_for_result(
            lambda: len(res) == 1 and isinstance(res[0], bytes) and
                    len(result) == 1 and isinstance(result[0], bytes),
            timeout=5
        )
