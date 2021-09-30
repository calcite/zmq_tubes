import asyncio
import zmq

from helpers import run_test_tasks
from tube import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/dealer_dealer.pipe'
TOPIC = 'req'


def test_dealer_router():
    data = ['request-DEALER1_REQ-0', 'request-DEALER1_REQ-1',
            'request-DEALER2_REQ-0', 'request-DEALER2_REQ-1']

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

    asyncio.run(
        run_test_tasks(
            [request_task(node_dealer1, TOPIC, 'DEALER1_REQ'),
             request_task(node_dealer2, TOPIC, 'DEALER2_REQ')],
            [response_dealer_task(node_dealer1, f'{TOPIC}/#', 'DEALER1_RESP'),
             response_dealer_task(node_dealer2, f'{TOPIC}/#', 'DEALER2_RESP')]
        )
    )

    assert len(data) == 0
