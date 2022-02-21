import time

import zmq

from ..helpers import run_test_threads, cleanup_threads, wrapp
from zmq_tubes.threads import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/dealer_dealer.pipe'
TOPIC = 'req'


@cleanup_threads
def test_dealer_dealer():
    data = ['request-DEALER1_REQ-0', 'request-DEALER1_REQ-1',
            'request-DEALER2_REQ-0', 'request-DEALER2_REQ-1']

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

    tube_dealer1 = Tube(
        name='DEALER1',
        addr=ADDR,
        server=True,
        tube_type=zmq.DEALER
    )

    tube_dealer2 = Tube(
        name='DEALER2',
        addr=ADDR,
        tube_type=zmq.DEALER
    )

    node_dealer1 = TubeNode()
    node_dealer1.register_tube(tube_dealer1, f"{TOPIC}/#")
    node_dealer1.connect()

    node_dealer2 = TubeNode()
    node_dealer2.register_tube(tube_dealer2, f"{TOPIC}/#")
    node_dealer2.connect()

    run_test_threads(
        [request_task(node_dealer1, TOPIC, 'DEALER1_REQ'),
         request_task(node_dealer2, TOPIC, 'DEALER2_REQ')],
        [response_dealer_task(node_dealer1, f'{TOPIC}/#', 'DEALER1_RESP'),
         response_dealer_task(node_dealer2, f'{TOPIC}/#', 'DEALER2_RESP')]
    )

    assert len(data) == 0


@cleanup_threads
def test_dealer_dealer_on_same_node():
    data = ['request-DEALER1_REQ-0', 'request-DEALER1_REQ-1',
            'request-DEALER2_REQ-0', 'request-DEALER2_REQ-1']

    @wrapp
    def request_task(node, topic, name, tube):
        time.sleep(2)
        for it in range(0, 2):
            node.send(topic, f"request-{name}-{it}", tube=tube)
        time.sleep(3)

    @wrapp
    def response_dealer_task(node, topic, name, tube):
        def __process(response: TubeMessage):
            assert response.payload in data
            data.remove(response.payload)
        node.register_handler(topic, __process, tube)
        node.start()

    tube_dealer1 = Tube(
        name='DEALER1',
        addr=ADDR,
        server=True,
        tube_type=zmq.DEALER
    )

    tube_dealer2 = Tube(
        name='DEALER2',
        addr=ADDR,
        tube_type=zmq.DEALER
    )

    node_dealer1 = TubeNode()
    node_dealer1.register_tube(tube_dealer1, f"{TOPIC}/#")
    node_dealer1.register_tube(tube_dealer2, f"{TOPIC}/#")
    node_dealer1.connect()

    run_test_threads(
        [request_task(node_dealer1, TOPIC, 'DEALER1_REQ', tube_dealer1),
         request_task(node_dealer1, TOPIC, 'DEALER2_REQ', tube_dealer2)],
        [response_dealer_task(node_dealer1, f'{TOPIC}/#',
                              'DEALER1_RESP', tube_dealer1),
         response_dealer_task(node_dealer1, f'{TOPIC}/#',
                              'DEALER2_RESP', tube_dealer2)]
    )

    assert len(data) == 0
