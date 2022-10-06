import sys

import asyncio

import zmq
import pytest

from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

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
        name='DEALER1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.DEALER
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


@pytest.fixture(params=[{'server': False}])
def resp_node1(data, request):
    async def __process(req):
        if req.payload in data:
            data.remove(req.payload)
            return req.create_response(f'RESP1-{req.payload[-2:]}')

    tube = Tube(
        name='RESP1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process, tube)
    return node


@pytest.fixture(params=[{'server': False}])
def resp_node2(data2, request):
    async def __process(req):
        if req.payload in data2:
            data2.remove(req.payload)
            return req.create_response(f'RESP2-{req.payload[-2:]}')

    tube = Tube(
        name='RESP2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process, tube)
    return node


@pytest.mark.asyncio
async def test_dealer_reps(dealer_node, resp_node1, resp_node2, data, data2):
    res = []

    async def __process(req):
        res.append(req.payload)

    dealer_node.register_handler(f"{TOPIC}/#", __process)

    with dealer_node, resp_node1, resp_node2:
        for _ in range(len(data)):
            dealer_node.send(f"{TOPIC}/A", data[0])
            dealer_node.send(f"{TOPIC}/A", data2[0])
            await asyncio.sleep(.2)
        await asyncio.sleep(1.2)

    assert len(res) == 4
    assert len(data) == 0
    assert len(data2) == 0


@pytest.mark.asyncio
async def test_dealer_reps_on_same_node(dealer_node, data):
    res = []

    async def __process(req):
        res.append(req.payload)

    dealer_node.register_handler(f"{TOPIC}/#", __process, dealer_node.tubes[0])

    async def __process_resp(req):
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
        for _ in range(len(data)):
            dealer_node.send(f"{TOPIC}/A", data[0])
            await asyncio.sleep(.1)
        await asyncio.sleep(1)

    assert len(res) == 2
    assert len(data) == 0
