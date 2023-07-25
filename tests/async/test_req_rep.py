import sys

import asyncio
import zmq
import pytest

from tests.helpers import wait_for_result
from zmq_tubes.manager import TubeMessageTimeout
from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/req_resp.pipe'
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


@pytest.fixture(params=[{'server': True, 'sleep': None, 'utf8_decoding': True}])
def resp_node(result, request):
    async def __process(req):
        result.append(req.payload)
        if request.param['sleep']:
            await asyncio.sleep(request.param['sleep'])
        return req.create_response(
            f'RESP{req.payload[-2:]}' if request.param['utf8_decoding'] else
            b'RESP' + req.payload[-2:]
        )

    tube = Tube(
        name='REP',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP,
        utf8_decoding=request.param['utf8_decoding']
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': False, 'utf8_decoding': True}])
def req_node1(request):
    tube = Tube(
        name='REQ1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REQ,
        utf8_decoding=request.param['utf8_decoding']
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


@pytest.fixture(params=[{'server': False, 'utf8_decoding': True}])
def req_node2(request):
    tube = Tube(
        name='REQ2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REQ,
        utf8_decoding=request.param['utf8_decoding']
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


################################################################################
#   Tests
################################################################################

@pytest.mark.asyncio
async def test_resp_reqs(resp_node, req_node1, req_node2, data, data2, result):
    res = []
    result.clear()

    async def step(node, d, p, delay=None):
        if delay:
            await asyncio.sleep(delay)  # distance between tasks
        while d:
            resp = await node.request(f"{TOPIC}/{p}", d.pop(), timeout=1)
            res.append('RESP' in resp.payload)

    async with resp_node:
        await asyncio.gather(
            asyncio.create_task(step(req_node1, data, 'A')),
            asyncio.create_task(step(req_node2, data2, 'B', delay=.05))
        )
        assert await wait_for_result(
            lambda: len(res) == 4 and all(res) and len(result) == 4,
            timeout=1
        )


@pytest.mark.asyncio
async def test_req_resp_on_same_node(resp_node, data, result):
    res = []
    result.clear()
    tube = Tube(
        name='REQ',
        addr=ADDR,
        server=False,
        tube_type=zmq.REQ
    )
    resp_node.register_tube(tube, f"{TOPIC}/#")
    async with resp_node:
        while data:
            resp = await resp_node.request(f"{TOPIC}/A", data.pop(), timeout=1)
            res.append('RESP' in resp.payload)
        assert await wait_for_result(
            lambda: len(res) == 2 and all(res) and len(result) == 2,
            timeout=1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("resp_node",
                         [({'server': True, 'sleep': 1,
                            'utf8_decoding': True})],
                         indirect=["resp_node"])
async def test_req_resp_timeout(resp_node, req_node1, data):
    async with resp_node:
        try:
            await req_node1.request(f"{TOPIC}/A", data.pop(), timeout=.1)
            assert False
        except TubeMessageTimeout:
            assert True


@pytest.mark.asyncio
@pytest.mark.parametrize("resp_node,req_node1",
                         [({'server': True, 'sleep': None,
                            'utf8_decoding': False},
                           {'server': False, 'utf8_decoding': False})],
                         indirect=["resp_node", "req_node1"])
async def test_req_resp_bytes(resp_node, req_node1, data, result):
    result.clear()
    async with resp_node:
        rr = await req_node1.request(f"{TOPIC}/A", data.pop())
        assert isinstance(rr.payload, bytes)
        rr = await req_node1.request(f"{TOPIC}/A", data.pop(),
                                     utf8_decoding=True)
        assert not isinstance(rr.payload, bytes)
        assert await wait_for_result(
            lambda: len(result) == 2 and isinstance(result[0], bytes),
            timeout=1
        )
