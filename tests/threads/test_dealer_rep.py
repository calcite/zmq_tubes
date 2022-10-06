import pytest
import time
import zmq

from zmq_tubes.threads import Tube, TubeNode

ADDR = 'ipc:///tmp/dealer_rep.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11']


@pytest.fixture
def data2():
    return ['REQ20', 'REQ21']


@pytest.fixture(params=[{'server': True}])
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


@pytest.fixture(params=[{'server': False}])
def resp_node1(data, request):
    def __process(req):
        if req.payload in data:
            data.remove(req.payload)
            return req.create_response(f'RESP{req.payload[-2:]}')

    tube = Tube(
        name='REP1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': False}])
def resp_node2(data2, request):
    def __process(req):
        if req.payload in data2:
            data2.remove(req.payload)
            return req.create_response(f'RESP{req.payload[-2:]}')

    tube = Tube(
        name='REP2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


def test_dealer_reps(dealer_node, resp_node1, resp_node2, data, data2):
    res = []

    def __process(req):
        res.append(req.payload)

    dealer_node.register_handler(f"{TOPIC}/#", __process)

    with dealer_node, resp_node1, resp_node2:
        d1 = data.copy()
        d2 = data2.copy()
        while d1 and d2:
            dealer_node.send(f"{TOPIC}/A", d1.pop(0))
            dealer_node.send(f"{TOPIC}/A", d2.pop(0))
        time.sleep(1)

    assert len(res) == 4
    assert len(data) == 0
    assert len(data2) == 0


def test_dealer_reps_on_same_node(dealer_node, data):
    res = []

    def __process(req):
        res.append(req.payload)

    dealer_node.register_handler(f"{TOPIC}/#", __process, dealer_node.tubes[0])

    def __process_resp(req):
        data.remove(req.payload)
        return req.create_response(f'RESP-{req.payload[-2:]}')

    tube = Tube(
        name='RESP',
        addr=ADDR,
        server=False,
        tube_type=zmq.REP
    )
    dealer_node.register_tube(tube, f"{TOPIC}/#")
    dealer_node.register_handler(f"{TOPIC}/#", __process_resp, tube)

    with dealer_node:
        for it in data.copy():
            dealer_node.send(f"{TOPIC}/A", it)
        time.sleep(.1)

    assert len(res) == 2
    assert len(data) == 0
