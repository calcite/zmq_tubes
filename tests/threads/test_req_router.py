import pytest
import time

import zmq

from ..helpers import run_test_threads, wrapp
from zmq_tubes.threads import Tube, TubeNode

ADDR = 'ipc:///tmp/req_router.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21']


@pytest.fixture(params=[{'server': True, 'sleep': None}])
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
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': False}])
def req_node1(request):
    tube = Tube(
        name='REQ1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REQ
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


@pytest.fixture(params=[{'server': False}])
def req_node2(request):
    tube = Tube(
        name='REQ2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REQ
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


def test_resp_router(router_node, req_node1, req_node2, data):

    res = []

    @wrapp
    def __process(node, p, d):
        for it in d.copy():
            r = node.request(f"{TOPIC}/{p}", it, timeout=1)
            res.append(r.payload)

    with router_node:
        run_test_threads(
            __process(req_node1, 'A', data[0:2]),
            __process(req_node2, 'B', data[2:]),
        )

    assert len(res) == 4
    assert len(data) == 0


def test_resp_router_on_same_node(router_node, data):
    tube = Tube(
        name='REQ1',
        addr=ADDR,
        server=False,
        tube_type=zmq.REQ
    )
    router_node.register_tube(tube, f"{TOPIC}/#")

    with router_node:
        for _ in range(len(data)):
            res = router_node.request(f"{TOPIC}/A", data[0], timeout=1)
            assert 'RESP' in res.payload

    assert len(data) == 0
