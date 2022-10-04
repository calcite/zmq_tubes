import time

import zmq

from ..helpers import run_test_threads, wrapp, cleanup_threads
from zmq_tubes.threads import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/dealer_router.pipe'
TOPIC = 'req'


@cleanup_threads
def test_dealer_router():
    data = ['response-DEALER_REQ-0', 'response-DEALER_REQ-1']

    @wrapp
    def request_task(node, topic, name):
        time.sleep(2)
        for it in range(0, 2):
            node.send(topic, f"request-{name}-{it}")
            time.sleep(1)
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
            return request.create_response(f'response-{request.payload[8:]}')
        node.register_handler(topic, __process)
        node.start()

    tube_dealer = Tube(
        name='DEALER',
        addr=ADDR,
        server=True,
        tube_type=zmq.DEALER
    )

    tube_router = Tube(
        name='ROUTER',
        addr=ADDR,
        tube_type=zmq.ROUTER
    )

    node_router = TubeNode()
    node_router.register_tube(tube_router, f"{TOPIC}/#")

    node_dealer = TubeNode()
    node_dealer.register_tube(tube_dealer, f"{TOPIC}/#")

    with node_dealer:
        run_test_threads(
            [request_task(node_dealer, TOPIC, 'DEALER_REQ')],
            [response_dealer_task(node_dealer, f'{TOPIC}/#', 'DEALER_RESP'),
             response_task(node_router, f'{TOPIC}/#', 'ROUTER')]
        )

    assert len(data) == 0


@cleanup_threads
def test_dealer_router_on_same_node():
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
            return request.create_response(f'response-{request.payload[8:]}')

        node.register_handler(topic, __process, tube)
        node.start()

    tube_dealer = Tube(
        name='DEALER',
        addr=ADDR,
        tube_type=zmq.DEALER
    )

    tube_router = Tube(
        name='ROUTER',
        addr=ADDR,
        server=True,
        tube_type=zmq.ROUTER
    )

    node = TubeNode()
    node.register_tube(tube_router, f"{TOPIC}/#")
    node.register_tube(tube_dealer, f"{TOPIC}/#")

    with node:
        run_test_threads(
            [request_task(node, TOPIC, 'DEALER_REQ', tube_dealer)],
            [response_dealer_task(node, f'{TOPIC}/#', 'DEALER_RESP',
                                  tube_dealer),
             response_task(node, f'{TOPIC}/#', 'ROUTER', tube_router)]
        )

    assert len(data) == 0
