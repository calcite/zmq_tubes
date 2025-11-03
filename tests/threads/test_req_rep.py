import time
import zmq
import pytest

from tests.helpers import wait_for_result2 as wait_for_result
from zmq_tubes.manager import TubeMessageTimeout
from zmq_tubes.threads import Tube, TubeNode, Context

ADDR = 'ipc:///tmp/req_resp.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21'].copy()


@pytest.fixture
def result():
    return []


@pytest.fixture(params=[{'server': True, 'sleep': None, 'addr': ADDR, 'utf8_decoding': True}])
def resp_node(result, request):
    def __process(req):
        result.append(req.payload)
        if request.param['sleep']:
            time.sleep(request.param['sleep'])
        return req.create_response(
            f'RESP{req.payload[-2:]}' if request.param['utf8_decoding'] else
            b'RESP' + req.payload[-2:]
        )

    tube = Tube(
        name='REP',
        addr=request.param['addr'],
        server=request.param['server'],
        tube_type=zmq.REP,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': False, 'addr': ADDR, 'utf8_decoding': True}])
def req_node1(request):
    tube = Tube(
        name='REQ1',
        addr=request.param['addr'],
        server=request.param['server'],
        tube_type=zmq.REQ,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


@pytest.fixture(params=[{'server': False, 'addr': ADDR, 'utf8_decoding': True}])
def req_node2(request):
    tube = Tube(
        name='REQ2',
        addr=request.param['addr'],
        server=request.param['server'],
        tube_type=zmq.REQ,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


################################################################################
#   Tests
################################################################################


def test_resp_reqs(resp_node, req_node1, data, result):
    res = []
    result.clear()
    with resp_node:
        while data:
            resp = req_node1.request(f"{TOPIC}/A", data.pop(), timeout=1)
            res.append('RESP' in resp.payload)
        assert wait_for_result(
            lambda: len(res) == 4 and all(res) and len(result) == 4,
            timeout=5
        )


def test_resp_reqs_on_same_node(resp_node, data, result):
    res = []
    result.clear()
    tube = Tube(
        name='REQ',
        addr=ADDR,
        server=False,
        tube_type=zmq.REQ
    )
    resp_node.register_tube(tube, f"{TOPIC}/#")
    with resp_node:
        while data:
            resp = resp_node.request(f"{TOPIC}/A", data.pop(), timeout=1)
            res.append('RESP' in resp.payload)
        assert wait_for_result(
            lambda: len(res) == 4 and all(res) and len(result) == 4,
            timeout=5
        )


@pytest.mark.parametrize("resp_node,req_node1",
                         [({'server': True, 'sleep': None, 'addr': ADDR,
                            'utf8_decoding': False},
                           {'server': False, 'utf8_decoding': False, 'addr': ADDR})],
                         indirect=["resp_node", "req_node1"])
def test_req_resp_bytes(resp_node, req_node1, data, result):
    result.clear()
    with resp_node:
        rr = req_node1.request(f"{TOPIC}/A", data.pop())
        assert isinstance(rr.payload, bytes)
        rr = req_node1.request(f"{TOPIC}/A", data.pop(),
                               utf8_decoding=True)
        assert not isinstance(rr.payload, bytes)
        assert wait_for_result(
            lambda: len(result) == 2 and isinstance(result[0], bytes),
            timeout=5
        )


@pytest.mark.parametrize("resp_node,req_node1",
                         [({'server': True, 'sleep': 1,
                            'addr': 'ipc:///tmp/req_resp_tmo.pipe',
                            'utf8_decoding': True},
                           {'server': False, 'utf8_decoding': False,
                            'addr': 'ipc:///tmp/req_resp_tmo.pipe'})],
                         indirect=["resp_node", "req_node1"])
def test_req_resp_timeout(resp_node, req_node1, data):
    with resp_node:
        try:
            req_node1.request(f"{TOPIC}/A", data.pop(), timeout=.1)
            assert False
        except TubeMessageTimeout:
            assert True
