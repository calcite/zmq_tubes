import sys

import asyncio

import zmq
import pytest

from tests.helpers import wait_for_result
from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/dealer_router.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21'].copy()


@pytest.fixture
def result():
    return []


@pytest.fixture(params=[{'server': True, 'utf8_decoding': True}])
def router_node(result, request):
    async def __process(req):
        result.append(req.payload)
        if isinstance(req.payload, str) and 'REQ10' in req.payload:
            await asyncio.sleep(.3)
        return req.create_response(
            f'RESP1{req.payload[-2:]}' if request.param['utf8_decoding']
            else b'RESP1' + req.payload[-2:])

    tube = Tube(
        name='ROUTER',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.ROUTER,
        utf8_decoding=request.param['utf8_decoding']
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
        utf8_decoding=request.param['utf8_decoding']
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


################################################################################
#   Tests
################################################################################


@pytest.mark.asyncio
async def test_router_dealer(router_node, dealer_node, data, result):
    res = []

    async def __process(req):
        res.append(req.payload)
    dealer_node.register_handler(f"{TOPIC}/#", __process)
    result.clear()
    async with router_node, dealer_node:
        while data:
            await dealer_node.send(f"{TOPIC}/A", data.pop())
        assert await wait_for_result(
            lambda: len(res) == 4 and len(result) == 4,
            timeout=1
        )


@pytest.mark.asyncio
async def test_router_dealer_on_same_node(router_node, data, result):
    res = []

    async def __process(req):
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

    async with router_node:
        while data:
            await router_node.send(f"{TOPIC}/A", data.pop(), tube)
        assert await wait_for_result(
            lambda: len(res) == 4 and len(result) == 4,
            timeout=1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("router_node,dealer_node",
                         [({'server': True, 'utf8_decoding': False},
                           {'server': False, 'utf8_decoding': False})],
                         indirect=["router_node", "dealer_node"])
async def test_router_dealer_bytes(router_node, dealer_node, result):
    res = []

    async def __process(req):
        res.append(req.payload)
    dealer_node.register_handler(f"{TOPIC}/#", __process)
    result.clear()
    async with router_node, dealer_node:
        await dealer_node.send(f"{TOPIC}/A", 'XXX')
        assert await wait_for_result(
            lambda: len(res) == 1 and isinstance(res[0], bytes) and
                    len(result) == 1 and isinstance(result[0], bytes),
            timeout=1
        )
