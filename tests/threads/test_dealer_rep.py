import os
import pytest
import zmq

from tests.helpers import wait_for_result2
from zmq_tubes.threads import Tube, TubeNode, Context

ADDR = 'ipc:///tmp/dealer_rep.pipe'
os.path.exists(ADDR.replace('ipc://', '')) and os.remove(ADDR.replace('ipc://', ''))
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11'].copy()


@pytest.fixture
def data2():
    return ['REQ20', 'REQ21'].copy()


@pytest.fixture
def result():
    return []


@pytest.fixture
def result2():
    return []


@pytest.fixture(params=[{'server': True, 'utf8_decoding': True}])
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


@pytest.fixture(params=[{'server': False, 'utf8_decoding': True}])
def resp_node1(result, request):
    def __process(req):
        result.append(req.payload)
        return req.create_response(
            f'RESP1{req.payload[-2:]}' if request.param['utf8_decoding']
            else b'RESP1' + req.payload[-2:])

    tube = Tube(
        name='RESP1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/A")
    node.register_handler(f"{TOPIC}/A", __process, tube)
    return node


@pytest.fixture(params=[{'server': False, 'utf8_decoding': True}])
def resp_node2(result2, request):
    def __process(req):
        result2.append(req.payload)
        return req.create_response(
            f'RESP2{req.payload[-2:]}' if request.param['utf8_decoding']
            else b'RESP2' + req.payload[-2:]
        )

    tube = Tube(
        name='RESP2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP,
        utf8_decoding=request.param['utf8_decoding'],
        context=Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/B")
    node.register_handler(f"{TOPIC}/B", __process, tube)
    return node


################################################################################
#   Tests
################################################################################

def test_dealer_reps(dealer_node, resp_node1, resp_node2, data, data2,
                     result, result2):
    """
    One dealer send request to two resp servers
    """
    res = []

    def __process(req):
        res.append(req.payload)
    dealer_node.register_handler(f"{TOPIC}/#", __process)
    result.clear()
    result2.clear()
    with dealer_node, resp_node1, resp_node2:
        while data:
            dealer_node.send(f"{TOPIC}/A", data.pop())
            dealer_node.send(f"{TOPIC}/B", data2.pop())
        assert wait_for_result2(
            lambda: len(res) == 4 and len(result) == 2 and len(result2) == 2,
            timeout=5
        )


def test_dealer_reps_on_same_node(dealer_node, data, result):
    res = []

    def __process(req):
        res.append(req.payload)
    dealer_node.register_handler(f"{TOPIC}/#", __process, dealer_node.tubes[0])

    def __process_resp(req):
        result.append(req.payload)
        return req.create_response(f'RESP-{req.payload[-2:]}')

    result.clear()
    tube = Tube(
        name='RESP',
        addr=ADDR,
        server=False,
        tube_type=zmq.REP
    )
    dealer_node.register_tube(tube, f"{TOPIC}/#")
    dealer_node.register_handler(f"{TOPIC}/#", __process_resp, tube)

    with dealer_node:
        while data:
            dealer_node.send(f"{TOPIC}/A", data.pop())
        assert wait_for_result2(
            lambda: len(res) == 2 and len(result) == 2,
            timeout=5
        )


@pytest.mark.parametrize("dealer_node,resp_node1",
                         [({'server': True, 'utf8_decoding': False},
                           {'server': False, 'utf8_decoding': False})],
                         indirect=["dealer_node", "resp_node1"])
def test_dealer_reps_bytes(dealer_node, resp_node1, result):
    res = []

    def __process(req):
        res.append(req.payload)

    dealer_node.register_handler(f"{TOPIC}/#", __process)
    result.clear()
    with dealer_node, resp_node1:
        dealer_node.send(f"{TOPIC}/A", 'XXX')
        assert wait_for_result2(
            lambda: len(res) == 1 and isinstance(res[0], bytes) and
                    len(result) == 1 and isinstance(result[0], bytes),
            timeout=5
        )
