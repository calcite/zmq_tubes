import asyncio
import zmq
import sys
import pytest

from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/req_router.pipe'
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
async def test_router_reqs(router_node, req_node1, req_node2, data):

    res = []

    async def step(node, p, delay=None):
        if delay:
            await asyncio.sleep(delay)  # distance between tasks
        while data:
            resp = await node.request(f"{TOPIC}/{p}", data[0], timeout=1)
            res.append('RESP' in resp.payload)

    async with router_node:
        await asyncio.gather(
            asyncio.create_task(step(req_node1, 'A')),
            asyncio.create_task(step(req_node2, 'B', delay=.1))
        )
    assert all(res)
    assert len(data) == 0


@pytest.mark.asyncio
async def test_req_router_on_same_node(router_node, data):
    """
        The REQ/ROUTER and client on the same node.
    """

    res = []

    async def step(node, p):
        for _ in range(len(data)):
            resp = await node.request(f"{TOPIC}/{p}", data[0], timeout=1)
            res.append('RESP' in resp.payload)

    tube = Tube(
        name='REQ',
        addr=ADDR,
        server=False,
        tube_type=zmq.REQ
    )
    router_node.register_tube(tube, f"{TOPIC}/#")

    async with router_node:
        await step(router_node, 'A')
    assert all(res)
    assert len(data) == 0
