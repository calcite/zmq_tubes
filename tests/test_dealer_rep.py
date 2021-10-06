import asyncio
import zmq

from helpers import run_test_tasks
from tubes import Tube, TubeNode, TubeMessage

ADDR = 'ipc:///tmp/dealer_rep.pipe'
TOPIC = 'req'


def test_dealer_reps():
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
            if 'REQ-0' in request.payload:
                await asyncio.sleep(1)
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
    node_rep1.connect()

    node_rep2 = TubeNode()
    node_rep2.register_tube(tube_rep2, f"{TOPIC}/#")
    node_rep2.connect()

    node_dealer = TubeNode()
    node_dealer.register_tube(tube_dealer, f"{TOPIC}/#")
    node_dealer.connect()

    asyncio.run(
        run_test_tasks(
            [request_task(node_dealer, TOPIC, 'DEALER_REQ')],
            [response_dealer_task(node_dealer, f'{TOPIC}/#', 'DEALER_RESP'),
             response_task(node_rep1, f'{TOPIC}/#', 'REP1'),
             response_task(node_rep2, f'{TOPIC}/#', 'REP2')]
        )
    )

    assert len(data) == 0
