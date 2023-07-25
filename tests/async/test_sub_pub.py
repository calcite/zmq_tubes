import sys

import asyncio
import pytest
import zmq

from tests.helpers import wait_for_result
from zmq_tubes import Tube, TubeNode

pytestmark = pytest.mark.skipif(sys.version_info < (3, 7),
                                reason='requires python3.7')

ADDR = 'ipc:///tmp/sub_pub.pipe'
TOPIC = 'sub'


@pytest.fixture
def data():
    return ['PUB10', 'PUB11', 'PUB20', 'PUB21'].copy()


@pytest.fixture
def data2():
    return ['PUB10', 'PUB11', 'PUB20', 'PUB21'].copy()


@pytest.fixture
def result():
    return []


@pytest.fixture
def result2():
    return []


@pytest.fixture(params=[{'server': True, 'utf8_decoding': True}])
def sub_node(result, request):
    async def __process(req):
        result.append(req.payload)

    tube = Tube(
        name='SUB1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.SUB,
        utf8_decoding=request.param['utf8_decoding']
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    return node


@pytest.fixture(params=[{'server': True, 'utf8_decoding': True}])
def sub_node2(result2, request):
    async def __process(req):
        result2.append(req.payload)

    tube = Tube(
        name='SUB2',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.SUB,
        utf8_decoding=request.param['utf8_decoding']
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


################################################################################
#   Tests
################################################################################


@pytest.mark.asyncio
async def test_sub_pubs(sub_node, pub_node1, pub_node2, data, data2,
                        result):
    """
        The subscriber is server and two clients publish messages.
    """
    result.clear()
    async with sub_node, pub_node1, pub_node2:
        await asyncio.sleep(.2)  # we have to wait for server is ready
        while data:
            await pub_node1.publish(f"{TOPIC}/A", data.pop())
            await pub_node2.publish(f"{TOPIC}/B", data2.pop())
        assert await wait_for_result(lambda: len(result) == 8, timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("sub_node,sub_node2,pub_node1",
                         [({'server': False, 'utf8_decoding': True},
                           {'server': False, 'utf8_decoding': True},
                           {'server': True})],
                         indirect=["sub_node", "sub_node2", "pub_node1"])
async def test_pub_subs(sub_node, sub_node2, pub_node1, data, result, result2):
    """
        The publisher is server and two clients subscribe messages.
    """
    result.clear()
    result2.clear()
    async with sub_node, sub_node2, pub_node1:
        await asyncio.sleep(.2)  # we have to wait for server is ready
        while data:
            await pub_node1.publish(f"{TOPIC}/A", data.pop())
            await asyncio.sleep(.1)  # we have to give server time for work
        assert await wait_for_result(
            lambda: len(result) == 4 and len(result2) == 4,
            timeout=1
        )


@pytest.mark.asyncio
async def test_pub_sub_on_same_node(sub_node, data, result):
    """
        The publisher and client on the same node.
    """
    result.clear()
    tube = Tube(
        name='PUB',
        addr=ADDR,
        server=False,
        tube_type=zmq.PUB
    )
    sub_node.register_tube(tube, f"{TOPIC}/#")

    async with sub_node:
        await asyncio.sleep(.2)  # we have to wait for server is ready
        while data:
            await sub_node.publish(f"{TOPIC}/A", data.pop())
        assert await wait_for_result(lambda: len(result) == 4, timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("sub_node",
                         [({'server': True, 'utf8_decoding': False})],
                         indirect=["sub_node"])
async def test_pub_sub_bytes(sub_node, pub_node1, result):
    result.clear()
    async with sub_node, pub_node1:
        await asyncio.sleep(.2)  # we have to wait for server is ready
        await pub_node1.publish(f"{TOPIC}/A", 'XXX')
        assert await wait_for_result(
            lambda: len(result) > 0 and isinstance(result.pop(), bytes),
            timeout=1
        )
