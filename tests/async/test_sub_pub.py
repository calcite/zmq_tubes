import sys

import asyncio

import zmq
import pytest

from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/sub_pub.pipe'
TOPIC = 'sub'


@pytest.fixture
def data():
    return ['PUB10', 'PUB11', 'PUB20', 'PUB21']


@pytest.fixture
def data2():
    return ['PUB10', 'PUB11', 'PUB20', 'PUB21']


@pytest.fixture(params=[{'server': True}])
def sub_node(data, request):
    async def __process(req):
        data.remove(req.payload)

    tube = Tube(
        name='SUB1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.SUB
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': True}])
def sub_node2(data2, request):
    async def __process(req):
        data2.remove(req.payload)

    tube = Tube(
        name='SUB2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.SUB
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': False}])
def pub_node1(request):
    tube = Tube(
        name='PUB1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.PUB
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


@pytest.fixture(params=[{'server': False}])
def pub_node2(request):
    tube = Tube(
        name='PUB2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.PUB
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


@pytest.mark.asyncio
async def test_sub_pubs(sub_node, pub_node1, pub_node2, data):
    """
        The subscriber is server and two clients publish messages.
    """

    async def step(node1, node2):
        await asyncio.sleep(.2)  # we have to wait for server is ready
        while data:
            node1.publish(f"{TOPIC}/A", data[0])
            node2.publish(f"{TOPIC}/B", data[1])
            await asyncio.sleep(.1)  # we have to give server time for work

    with sub_node, pub_node1, pub_node2:
        await asyncio.gather(asyncio.create_task(step(pub_node1, pub_node2)))

    assert len(data) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("sub_node,sub_node2,pub_node1",
                         [({'server': False}, {'server': False},
                           {'server': True})],
                         indirect=["sub_node", "sub_node2", "pub_node1"])
async def test_pub_subs(sub_node, sub_node2, pub_node1, data, data2):
    """
        The publisher is server and two clients subscribe messages.
    """
    async def step(node):
        await asyncio.sleep(.2)  # we have to wait for server is ready
        for _ in range(len(data)):
            node.publish(f"{TOPIC}/A", data[0])
            await asyncio.sleep(.1)  # we have to give server time for work

    with sub_node, sub_node2, pub_node1:
        await asyncio.gather(asyncio.create_task(step(pub_node1)))

    assert len(data) == 0
    assert len(data2) == 0


@pytest.mark.asyncio
async def test_pub_sub_on_same_node(sub_node, data):
    """
        The publisher and client on the same node.
    """

    async def step(node):
        await asyncio.sleep(.2)      # we have to wait for server is ready
        for _ in range(len(data)):
            node.publish(f"{TOPIC}/A", data[0])
            await asyncio.sleep(.1)  # we have to give server time for work

    tube = Tube(
        name='PUB',
        addr=ADDR,
        server=False,
        tube_type=zmq.PUB
    )
    sub_node.register_tube(tube, f"{TOPIC}/#")

    with sub_node:
        await asyncio.gather(asyncio.create_task(step(sub_node)))

    assert len(data) == 0
