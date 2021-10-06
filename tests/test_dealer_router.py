import asyncio
import zmq

from helpers import run_test_tasks
from tubes import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/dealer_router.pipe'
TOPIC = 'req'


def test_dealer_router():
    data = ['response-DEALER_REQ-0', 'response-DEALER_REQ-1']

    async def request_task(tube, topic, name):
        asyncio.current_task().set_name(name)
        for it in range(0, 2):
            tube.send(topic, f"request-{name}-{it}")
        await asyncio.sleep(3)

    async def response_dealer_task(tube, topic, name):
        async def __process(response: TubeMessage):
            assert response.payload in data
            data.remove(response.payload)
        asyncio.current_task().set_name(name)
        tube.register_handler(topic, __process)
        await tube.start()

    async def response_task(tube, topic, name):
        async def __process(request: TubeMessage):
            assert request.payload[0:8] == 'request-'
            return request.create_response(f'response-{request.payload[8:]}')

        asyncio.current_task().set_name(name)
        tube.register_handler(topic, __process)
        await tube.start()

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
    node_router.connect()

    node_dealer = TubeNode()
    node_dealer.register_tube(tube_dealer, f"{TOPIC}/#")
    node_dealer.connect()

    asyncio.run(
        run_test_tasks(
            [request_task(node_dealer, TOPIC, 'DEALER_REQ')],
            [response_dealer_task(node_dealer, f'{TOPIC}/#', 'DEALER_RESP'),
             response_task(node_router, f'{TOPIC}/#', 'ROUTER')]
        )
    )

    assert len(data) == 0
