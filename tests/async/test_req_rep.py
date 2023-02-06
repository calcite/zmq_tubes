import sys

import asyncio
import zmq
import pytest

from zmq_tubes.manager import TubeMessageTimeout
from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/req_resp.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11', 'REQ20', 'REQ21']


@pytest.fixture(params=[{'server': True, 'sleep': None}])
def resp_node(data, request):
    async def __process(req):
        data.remove(req.payload)
        if request.param['sleep']:
            await asyncio.sleep(request.param['sleep'])
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


@pytest.mark.asyncio
async def test_resp_reqs(resp_node, req_node1, req_node2, data):
    res = []

    async def step(node, p, delay=None):
        if delay:
            await asyncio.sleep(delay)  # distance between tasks
        while data:
            resp = await node.request(f"{TOPIC}/{p}", data[0], timeout=1)
            res.append('RESP' in resp.payload)

    async with resp_node:
        await asyncio.gather(
            asyncio.create_task(step(req_node1, 'A')),
            asyncio.create_task(step(req_node2, 'B', delay=.1))
        )
    assert all(res)
    assert len(data) == 0


@pytest.mark.asyncio
async def test_req_resp_on_same_node(resp_node, data):
    res = []

    async def step(node, p, d):
        for it in d:
            resp = await node.request(f"{TOPIC}/{p}", it, timeout=1)
            res.append('RESP' in resp.payload)

    tube = Tube(
        name='REQ',
        addr=ADDR,
        server=False,
        tube_type=zmq.REQ
    )
    resp_node.register_tube(tube, f"{TOPIC}/#")

    async with resp_node:
        await step(resp_node, 'A', data.copy())
    assert all(res)
    assert len(data) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("resp_node",
                         [({'server': True, 'sleep': 3})],
                         indirect=["resp_node"])
async def test_req_resp_timeout(resp_node, req_node1, data):
    res = []

    async def step(node, p):
        try:
            await node.request(f"{TOPIC}/{p}", data[0], timeout=.5)
            res.append(False)
        except TubeMessageTimeout:
            res.append(True)

    async with resp_node:
        await step(req_node1, 'A')
    assert all(res)
