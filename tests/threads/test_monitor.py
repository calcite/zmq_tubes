import io

# from threading import Thread

import pytest
import zmq

from tests.helpers import wait_for_result2 as wait_for_result
from zmq_tubes.threads import Tube, TubeNode, TubeMonitor
from zmq_tubes.monitoring import get_schema, simulate  # logs,

ADDR = 'ipc:///tmp/req_resp.pipe'
MONITOR = 'ipc:///tmp/monitor.pipe'
TOPIC = 'req'


@pytest.fixture
def data():
    return ['REQ10', 'REQ11'].copy()


@pytest.fixture
def result():
    return []


@pytest.fixture(params=[{'server': True}])
def resp_node(result, request):
    def __process(req):
        result.append(req.payload)
        return req.create_response(f'RESP{req.payload[-2:]}')

    context = zmq.Context.instance()
    tube = Tube(
        name='REP',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REP,
        context=context
    )

    monitor = TubeMonitor(addr=MONITOR, context=context)

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    node.register_handler(f"{TOPIC}/#", __process)
    node.register_monitor(tube, monitor)
    return node


@pytest.fixture(params=[{'server': False}])
def req_node1(request):
    tube = Tube(
        name='REQ1',
        addr=ADDR,
        server=request.param['server'],
        tube_type=zmq.REQ,
        context=zmq.Context.instance()
    )

    node = TubeNode()
    node.register_tube(tube, f"{TOPIC}/#")
    return node


################################################################################
#   Tests
################################################################################


def test_schema(resp_node):
    with resp_node:
        schema = get_schema(MONITOR)
    assert 'tubes' in schema
    assert len(schema['tubes']) == 1
    tube = schema['tubes'].pop()
    assert tube.get('name') == 'REP'
    assert tube.get('tube_type') == 'REP'
    assert tube.get('server') == 'yes'
    assert tube.get('addr') == ADDR
    assert tube.get('monitor') == MONITOR


# def test_logging(resp_node, req_node1, data, result):
#     result.clear()
#     buffer = io.BytesIO()
#     th = Thread(target=lambda: logs(MONITOR, buffer, True, False), daemon=True)
#     th.start()

#     with resp_node:
#         while data:
#             res = req_node1.request(f"{TOPIC}/A", data.pop(0))
#             assert 'RESP' in res.payload
#         assert wait_for_result(
#             lambda: len(result) == 2,
#             timeout=1
#         )
#     th.join(timeout=2)
#     lines = buffer.getvalue().decode().split('\n')
#     print(lines)
#     assert len(lines) >= 4
#     assert lines[0].endswith('REP < req/A REQ10')
#     assert lines[1].endswith('REP > req/A RESP10')
#     assert lines[2].endswith('REP < req/A REQ11')
#     assert lines[3].endswith('REP > req/A RESP11')


def test_simulation(resp_node, result):
    dump = io.BytesIO(
        b'0.017551183700561523 REP < req/A REQ10\n'
        b'0.00019621849060058594 REP > req/A RESP10\n'
        b'0.0006017684936523438 REP < req/A REQ11\n'
        b'0.0001468658447265625 REP > req/A RESP11\n'
    )

    schema = io.StringIO(f"""
        tubes:
        - name: REP
          addr: {ADDR}
          tube_type: REQ
    """)
    result.clear()
    with resp_node:
        simulate(schema, dump, 1)
        assert wait_for_result(
            lambda: len(result) == 2,
            timeout=1
        )
