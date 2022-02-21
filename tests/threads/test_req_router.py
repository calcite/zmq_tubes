import time

import zmq

from ..helpers import run_test_threads, wrapp, cleanup_threads
from zmq_tubes.threads import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/req_router.pipe'
TOPIC = 'req'


@cleanup_threads
def test_req_route():

    @wrapp
    def request_task(node, topic, name, number=4, timeout=30):
        for it in range(0, number):
            response = node.request(topic, f"request-{name}-{it}",
                                    timeout=timeout)
            assert response.payload == f"response-{name}-{it}"

    @wrapp
    def response_task(node, topic, name):
        def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            if 'REQ2' in request.payload:
                time.sleep(2)
            return request.create_response(f'response-{request.payload[8:]}')

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
    tube_router = Tube(
        name='ROUTER',
        addr=ADDR,
        server=True,
        tube_type=zmq.ROUTER
    )
    node_req1 = TubeNode()
    node_req1.register_tube(tube_req1, f"{TOPIC}/#")

    node_req2 = TubeNode()
    node_req2.register_tube(tube_req2, f"{TOPIC}/#")

    node_router = TubeNode()
    node_router.register_tube(tube_router, f"{TOPIC}/#")

    run_test_threads(
        [request_task(node_req1, TOPIC, 'REQ1', timeout=5),
         request_task(node_req2, TOPIC, 'REQ2', number=1)
         ],
        [response_task(node_router, f'{TOPIC}/#', 'ROUTER')]
    )


@cleanup_threads
def test_req_router_on_same_node():
    """
        The REQ/ROUTER and client on the same node.
    """

    @wrapp
    def request_task(node, topic, name, number=4, timeout=30):
        for it in range(0, number):
            response = node.request(topic, f"request-{name}-{it}",
                                    timeout=timeout)
            assert response.payload == f"response-{name}-{it}"

    @wrapp
    def response_task(node, topic, name):
        def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            return request.create_response(f'response-{request.payload[8:]}')

        node.register_handler(topic, __process)
        node.start()

    tube1 = Tube(
        name='REQ',
        addr=ADDR,
        tube_type=zmq.REQ
    )

    tube2 = Tube(
        name='ROUTER',
        addr=ADDR,
        server=True,
        tube_type=zmq.ROUTER
    )

    node = TubeNode()
    node.register_tube(tube1, f"{TOPIC}/#")
    node.register_tube(tube2, f"{TOPIC}/#")

    run_test_threads(
        [request_task(node, TOPIC, 'REQ1', timeout=6)],
        [response_task(node, f'{TOPIC}/#', 'ROUTER')]
    )
