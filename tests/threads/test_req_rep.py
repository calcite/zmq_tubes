import pytest
import time
import zmq

from zmq_tubes.manager import TubeMessageTimeout
from zmq_tubes.threads import Tube, TubeNode

ADDR = 'ipc:///tmp/req_resp.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21']


@pytest.fixture(params=[{'server': True, 'sleep': None}])
def resp_node(data, request):
    def __process(req):
        data.remove(req.payload)
        if request.param['sleep']:
            time.sleep(request.param['sleep'])
        return req.create_response(f'RESP{req.payload[-2:]}')

    tube = Tube(
        name='REP',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP
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


def test_resp_reqs(resp_node, req_node1, data):

    with resp_node:
        for _ in range(len(data)):
            res = req_node1.request(f"{TOPIC}/A", data[0], timeout=1)
            assert 'RESP' in res.payload

    assert len(data) == 0


def test_resp_reqs_on_same_node(resp_node, data):
    tube = Tube(
        name='REQ1',
        addr=ADDR,
        server=False,
        tube_type=zmq.REQ
    )
    resp_node.register_tube(tube, f"{TOPIC}/#")
    with resp_node:
        for _ in range(len(data)):
            res = resp_node.request(f"{TOPIC}/A", data[0], timeout=1)
            assert 'RESP' in res.payload

    assert len(data) == 0


@pytest.mark.parametrize("resp_node",
                         [({'server': True, 'sleep': 1})],
                         indirect=["resp_node"])
def test_req_resp_timeout(resp_node, req_node1, data):
    with resp_node:
        try:
            req_node1.request(f"{TOPIC}/A", data[0], timeout=.6)
            assert False
        except TubeMessageTimeout:
            assert True
