import logging
import sys
import time

import zmq

from ..helpers import run_test_threads, cleanup_threads, wrapp
from zmq_tubes.threads import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/dealer_rep.pipe'
TOPIC = 'req'

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


@cleanup_threads
def test_dealer_reps():
    data = ['response-DEALER_REQ-0', 'response-DEALER_REQ-1']

    @wrapp
    def request_task(node, topic, name):
        time.sleep(2)
        for it in range(0, 2):
            node.send(topic, f"request-{name}-{it}")
        time.sleep(3)

    @wrapp
    def response_dealer_task(node, topic, name):
        def __process(response: TubeMessage):
            assert response.payload in data
            data.remove(response.payload)
        node.register_handler(topic, __process)
        node.start()

    @wrapp
    def response_task(node, topic, name):
        def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            if 'REQ-0' in request.payload:
                time.sleep(1)
            return request.create_response(f'response-{request.payload[8:]}')
        node.register_handler(topic, __process)
        node.start()

    tube_dealer = Tube(
        name='DEALER',
        addr=ADDR,
        server=True,
        tube_type=zmq.DEALER
    )

    tube_rep1 = Tube(
        name='REP1',
        addr=ADDR,
        tube_type=zmq.REP
    )

    tube_rep2 = Tube(
        name='REP2',
        addr=ADDR,
        tube_type=zmq.REP
    )
    node_rep1 = TubeNode()
    node_rep1.register_tube(tube_rep1, f"{TOPIC}/#")

    node_rep2 = TubeNode()
    node_rep2.register_tube(tube_rep2, f"{TOPIC}/#")

    node_dealer = TubeNode()
    node_dealer.register_tube(tube_dealer, f"{TOPIC}/#")
    # node_dealer.connect()

    run_test_threads(
        [request_task(node_dealer, TOPIC, 'DEALER_REQ')],
        [response_dealer_task(node_dealer, f'{TOPIC}/#', 'DEALER_RESP'),
         response_task(node_rep1, f'{TOPIC}/#', 'REP1'),
         response_task(node_rep2, f'{TOPIC}/#', 'REP2')]
    )

    assert len(data) == 0


@cleanup_threads
def test_dealer_reps_on_same_node():
    data = ['response-DEALER_REQ-0', 'response-DEALER_REQ-1']

    @wrapp
    def request_task(node, topic, name, tube):
        time.sleep(2)
        for it in range(0, 2):
            node.send(topic, f"request-{name}-{it}", tube)
        time.sleep(3)

    @wrapp
    def response_dealer_task(node, topic, name, tube):
        def __process(response: TubeMessage):
            assert response.payload in data
            data.remove(response.payload)
        node.register_handler(topic, __process, tube)

    @wrapp
    def response_task(node, topic, name, tube):
        def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            if 'REQ-0' in request.payload:
                time.sleep(1)
            return request.create_response(f'response-{request.payload[8:]}')
        node.register_handler(topic, __process, tube)
        time.sleep(0.5)    # Wait for response_dealer_task registered
        node.start()

    tube_dealer = Tube(
        name='DEALER',
        addr=ADDR,
        server=True,
        tube_type=zmq.DEALER
    )

    tube_rep = Tube(
        name='REP1',
        addr=ADDR,
        tube_type=zmq.REP
    )

    node = TubeNode()
    node.register_tube(tube_dealer, f"{TOPIC}/#")
    node.register_tube(tube_rep, f"{TOPIC}/#")
    # node.connect()

    run_test_threads(
        [request_task(node, TOPIC, 'DEALER_REQ', tube_dealer)],
        [response_dealer_task(node, f'{TOPIC}/#', 'DEALER_RESP',
                              tube_dealer),
         response_task(node, f'{TOPIC}/#', 'REP1', tube_rep)]
    )
    assert len(data) == 0
