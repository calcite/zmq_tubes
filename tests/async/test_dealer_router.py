import sys

import asyncio

import zmq
import pytest

from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/dealer_router.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21']


@pytest.fixture(params=[{'server': True}])
def router_node(data, request):
    async def __process(req):
        data.remove(req.payload)
        if 'REQ10' in req.payload:
            await asyncio.sleep(.3)
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
def dealer_node1(request):

    tube = Tube(
        name='DEALER1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.DEALER
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


@pytest.mark.asyncio
async def test_router_dealer(router_node, dealer_node1, data):

    res = []

    async def __process(req):
        res.append(req.payload)

    dealer_node1.register_handler(f"{TOPIC}/#", __process)

    async with router_node, dealer_node1:
        for _ in range(len(data)):
            await dealer_node1.send(f"{TOPIC}/A", data[0])
            await asyncio.sleep(.1)

    assert len(res) == 4
    assert len(data) == 0


@pytest.mark.asyncio
async def test_router_dealer_on_same_node(router_node, data):
    res = []

    async def __process(req):
        res.append(req.payload)

    tube = Tube(
        name='DEALER',
        addr=ADDR,
        server=False,
        tube_type=zmq.DEALER
    )
    router_node.register_tube(tube, f"{TOPIC}/#")
    router_node.register_handler(f"{TOPIC}/#", __process, tube)

    async with router_node:
        for _ in range(len(data)):
            await router_node.send(f"{TOPIC}/A", data[0], tube)
            await asyncio.sleep(.1)

    assert len(res) == 4
    assert len(data) == 0
