import sys

import zmq
import pytest

from tests.helpers import wait_for_result
from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/dealer_dealer.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21'].copy()


@pytest.fixture
def result():
    return []


@pytest.fixture(params=[{'server': True, 'utf8_decoding': True}])
def dealer_node1(request, result):
    async def __process(req):
        result.append(req.payload)
        await req.tube.send(req.create_response(
            f'DEALER1{req.payload[-2:]}' if request.param['utf8_decoding']
            else b'DEALER1' + req.payload[-2:]))

    tube = Tube(
        name='DEALER1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.DEALER,
        utf8_decoding=request.param['utf8_decoding']
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process, tube)
    return node


@pytest.fixture(params=[{'server': False, 'utf8_decoding': True}])
def dealer_node2(request):
    tube = Tube(
        name='DEALER2',
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
async def test_dealer_dealer(dealer_node1, dealer_node2, data, result):
    res = []

    async def __process(req):
        res.append(req.payload)
    dealer_node2.register_handler(f"{TOPIC}/#", __process)
    result.clear()
    async with dealer_node1, dealer_node2:
        while data:
            await dealer_node2.send(f"{TOPIC}/A", data.pop())
        assert await wait_for_result(
            lambda: len(res) == 4 and len(result) == 4,
            timeout=1
        )


@pytest.mark.asyncio
async def test_dealer_dealer_on_same_node(dealer_node1, data, result):
    res = []

    async def __process(req):
        res.append(req.payload)
    result.clear()
    tube = Tube(
        name='DEALER2',
        addr=ADDR,
        server=False,
        tube_type=zmq.DEALER
    )

    dealer_node1.register_tube(tube, f"{TOPIC}/#")
    dealer_node1.register_handler(f"{TOPIC}/#", __process, tube)

    async with dealer_node1:
        while data:
            await dealer_node1.send(f"{TOPIC}/A", data.pop())
        assert await wait_for_result(
            lambda: len(res) == 4 and len(result) == 4,
            timeout=1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("dealer_node1,dealer_node2",
                         [({'server': True, 'utf8_decoding': False},
                           {'server': False, 'utf8_decoding': False})],
                         indirect=["dealer_node1", "dealer_node2"])
async def test_dealer_reps_bytes(dealer_node1, dealer_node2, result):
    res = []

    async def __process(req):
        res.append(req.payload)
    dealer_node2.register_handler(f"{TOPIC}/#", __process)

    result.clear()
    async with dealer_node1, dealer_node2:
        await dealer_node2.send(f"{TOPIC}/A", 'XXX')
        assert await wait_for_result(
            lambda: len(res) == 1 and isinstance(res[0], bytes) and
                    len(result) == 1 and isinstance(result[0], bytes),
            timeout=1
        )
