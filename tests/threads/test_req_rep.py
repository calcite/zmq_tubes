import time

import zmq

from zmq_tubes.manager import TubeMessageTimeout
from ..helpers import run_test_threads, wrapp, cleanup_threads
from zmq_tubes.threads import Tube, TubeNode

ADDR = 'ipc:///tmp/req_resp.pipe'
TOPIC = 'req'


@cleanup_threads
def test_req_resp():

    @wrapp
    def request_task(node, topic, name, number=2, timeout=30):
        for it in range(0, number):
            resp = node.request(topic, f"request-{name}-{it}", timeout=timeout)
            assert resp.payload == f"response-{name}-{it}"

    @wrapp
    def response_task(node, topic):
        def __process(message):
            assert message.payload[0:8] == 'request-'
            return f'response-{message.payload[8:]}'
        node.register_handler(topic, __process)
        node.start()

    tube_req1 = Tube(
        name='REQ1',
        addr=ADDR,
        tube_type=zmq.REQ
    )
    tube_req2 = Tube(
        name='REQ2',
        addr=ADDR,
        tube_type=zmq.REQ
    )
    tube_resp = Tube(
        name='RESP',
        addr=ADDR,
        server=True,
        tube_type=zmq.REP
    )
    node_req1 = TubeNode()
    node_req1.register_tube(tube_req1, f"{TOPIC}/#")

    node_req2 = TubeNode()
    node_req2.register_tube(tube_req2, f"{TOPIC}/#")

    node_resp = TubeNode()
    node_resp.register_tube(tube_resp, f"{TOPIC}/#")

    with node_req1, node_req2:
        run_test_threads(
            [request_task(node_req1, f'{TOPIC}/aaa', 'REQ1'),
             request_task(node_req2, f'{TOPIC}', 'REQ2')],
            [response_task(node_resp, f'{TOPIC}/#')]
        )


@cleanup_threads
def test_req_resp_on_same_node():
    """
        The req/resp and client on the same node.
    """

    @wrapp
    def request_task(node, topic, name, number=2, timeout=30):
        for it in range(0, number):
            resp = node.request(topic, f"request-{name}-{it}", timeout=timeout)
            assert resp.payload == f"response-{name}-{it}"

    @wrapp
    def response_task(node, topic):
        def __process(message):
            assert message.payload[0:8] == 'request-'
            return f'response-{message.payload[8:]}'
        node.register_handler(topic, __process)
        node.start()

    tube1 = Tube(
        name='REQ',
        addr=ADDR,
        tube_type=zmq.REQ
    )

    tube2 = Tube(
        name='REP',
        addr=ADDR,
        server=True,
        tube_type=zmq.REP
    )

    node = TubeNode()
    node.register_tube(tube1, f"{TOPIC}/#")
    node.register_tube(tube2, f"{TOPIC}/#")

    run_test_threads(
        [request_task(node, TOPIC, 'REQ1')],
        [response_task(node, f'{TOPIC}/#')]
    )


@cleanup_threads
def test_req_resp_timeout():

    @wrapp
    def request_task(node, topic):
        try:
            node.request(topic, "request", timeout=1)
            assert False, "The TubeMessageTimeout exception was not fired."
        except TubeMessageTimeout:
            pass

    @wrapp
    def response_task(node, topic):
        def __process(message):
            time.sleep(3)
            return 'response'
        node.register_handler(topic, __process)
        node.start()

    tube_req1 = Tube(
        name='REQ1',
        addr=ADDR,
        tube_type=zmq.REQ
    )
    tube_resp = Tube(
        name='RESP',
        addr=ADDR,
        server=True,
        tube_type=zmq.REP
    )
    node_req1 = TubeNode()
    node_req1.register_tube(tube_req1, f"{TOPIC}/#")

    node_resp = TubeNode()
    node_resp.register_tube(tube_resp, f"{TOPIC}/#")

    with node_req1:
        run_test_threads(
            [request_task(node_req1, f'{TOPIC}/aaa')],
            [response_task(node_resp, f'{TOPIC}/#')]
        )
