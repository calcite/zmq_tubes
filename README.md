[![PyPI](https://img.shields.io/pypi/v/zmq_tubes?color=green&style=plastic)](https://pypi.org/project/zmq-tubes/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/zmq_tubes?style=plastic)
![License](https://img.shields.io/github/license/calcite/zmq_tubes?style=plastic)
# ZMQ Tubes

ZMQ Tubes is a managing system for ZMQ communication. 
It can manage many ZMQ sockets by one interface. 
The whole system is hierarchical, based on topics 
(look at [MQTT topics](https://www.hivemq.com/blog/mqtt-essentials-part-5-mqtt-topics-best-practices/)).

## Classes
- **TubeMessage** - This class represents a request/response message. 
  Some types of tubes require a response in this format.
- **Tube** - This class wraps a ZMQ socket. 
  It represents a connection between client and server.
- **TubeNode** - This represents an application interface for communication via tubes.


## Asyncio / Threading
The library support bot method. Asyncio from Python 3.7.

```python
from zmq_tubes import TubeNode, Tube            # Asyncio classes
from zmq_tubes.threads import TubeNode, Tube    # Threads classes
```


## Usage:

### Node definitions in yml file 
We can define all tubes for one TubeNode by yml file. 

```yaml
# test.yml
tubes:
  - name: Client REQ
    addr:  ipc:///tmp/req.pipe      
    tube_type: REQ
    topics:
      - foo/#
      - +/bar
  
  - name: Client PUB
    addr:  ipc:///tmp/pub.pipe      
    tube_type: PUB
    topics:
      - foo/pub/#

  - name: Server ROUTER
    addr:  ipc:///tmp/router.pipe      
    tube_type: ROUTER
    server: yes
    sockopts:
      LINGER: 0
    topics:
      - server/#
```

```python
import asyncio
import yaml
from zmq_tubes import TubeNode, TubeMessage


async def handler(request: TubeMessage):
  print(request.payload)
  return request.create_response('response')


async def run():
  with open('test.yml', 'r+') as fd:
    schema = yaml.safe_load(fd)
  node = TubeNode(schema=schema)
  node.register_handler('server/#', handler)
  with node:
      node.publish('foo/pub/test', 'message 1')
      print(await node.request('foo/xxx', 'message 2'))

asyncio.run(run())
```




### Request / Response
This is a simple scenario, the server processes the requests serially.
#### Server:

```python
from zmq_tubes import Tube, TubeNode, TubeMessage


async def handler(request: TubeMessage):
  print(request.payload)
  return 'answer'
  # or return request.create_response('response')


tube = Tube(
  name='Server',
  addr='ipc:///tmp/req_resp.pipe',
  server=True,
  tube_type='REP'
)

node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)
await node.start()

# output: 'question'
```

#### Client:

```python
from zmq_tubes import Tube, TubeNode

tube = Tube(
  name='Client',
  addr='ipc:///tmp/req_resp.pipe',
  tube_type='REQ'
)

node = TubeNode()
node.register_tube(tube, 'test/#')
response = await node.request('test/xxx', 'question')
print(response.payload)
# output: 'answer'
```




### Subscribe / Publisher
#### Server:

```python
from zmq_tubes import Tube, TubeNode, TubeMessage


async def handler(request: TubeMessage):
  print(request.payload)


tube = Tube(
  name='Server',
  addr='ipc:///tmp/sub_pub.pipe',
  server=True,
  tube_type='SUB'
)

node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)
await node.start()
# output: 'message'
```

#### Client:

```python
from zmq_tubes import Tube, TubeNode

tube = Tube(
  name='Client',
  addr='ipc:///tmp/sub_pub.pipe',
  tube_type='PUB'
)
# In the case of publishing, the first message is very often
# lost. The workaround is to connect the tube manually as soon as possible.
tube.connect()

node = TubeNode()
node.register_tube(tube, 'test/#')
node.publish('test/xxx', 'message')        
```




### Request / Router
The server is asynchronous. It means it is able to process 
more requests at the same time.

#### Server:

```python
import asyncio
from zmq_tubes import Tube, TubeNode, TubeMessage


async def handler(request: TubeMessage):
  print(request.payload)
  if request.payload == 'wait':
    await asyncio.sleep(10)
  return request.create_response(request.payload)


tube = Tube(
  name='Server',
  addr='ipc:///tmp/req_router.pipe',
  server=True,
  tube_type='ROUTER'
)

node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)
await node.start()
# output: 'wait'
# output: 'message'
```

#### Client:

```python
import asyncio
from zmq_tubes import Tube, TubeNode

tube = Tube(
  name='Client',
  addr='ipc:///tmp/req_router.pipe',
  tube_type='REQ'
)


async def task(node, text):
  print(await node.request('test/xxx', text))


node = TubeNode()
node.register_tube(tube, 'test/#')
asyncio.create_task(task(node, 'wait'))
asyncio.create_task(task(node, 'message'))
# output: 'message'
# output: 'wait'
```




### Dealer / Response
The client is asynchronous. It means it is able to send 
more requests at the same time.

#### Server:

```python
from zmq_tubes import Tube, TubeNode, TubeMessage


async def handler(request: TubeMessage):
  print(request.payload)
  return 'response'
  # or return requset.create_response('response')


tube = Tube(
  name='Server',
  addr='ipc:///tmp/dealer_resp.pipe',
  server=True,
  tube_type='REP'
)

node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)
await node.start()
# output: 'message'
```

#### Client:

```python
from zmq_tubes import Tube, TubeNode, TubeMessage

tube = Tube(
  name='Client',
  addr='ipc:///tmp/dealer_resp.pipe',
  tube_type='DEALER'
)


async def handler(response: TubeMessage):
  print(response.payload)


node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)

node.send('test/xxx', 'message')

# output: 'response'
```



### Dealer / Router
The client and server are asynchronous. It means it is able to send and process 
more requests/responses at the same time.

#### Server:

```python
import asyncio
from zmq_tubes import Tube, TubeNode, TubeMessage


async def handler(request: TubeMessage):
  print(request.payload)
  if request.payload == 'wait':
    await asyncio.sleep(10)
  return request.create_response(request.payload)


tube = Tube(
  name='Server',
  addr='ipc:///tmp/dealer_router.pipe',
  server=True,
  tube_type='ROUTER'
)

node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)
await node.start()
# output: 'wait'
# output: 'message'
```

#### Client:

```python
from zmq_tubes import Tube, TubeNode, TubeMessage

tube = Tube(
  name='Client',
  addr='ipc:///tmp/dealer_router.pipe',
  tube_type='DEALER'
)


async def handler(response: TubeMessage):
  print(response.payload)


node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)

node.send('test/xxx', 'wait')
node.send('test/xxx', 'message')

# output: 'message'
# output: 'wait'
```



### Dealer / Dealer
The client and server are asynchronous. It means it is able to send and process 
more requests/responses at the same time.

#### Server:

```python
from zmq_tubes import Tube, TubeNode, TubeMessage

tube = Tube(
  name='Server',
  addr='ipc:///tmp/dealer_dealer.pipe',
  server=True,
  tube_type='DEALER'
)


async def handler(response: TubeMessage):
  print(response.payload)


node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)

node.send('test/xxx', 'message from server')
# output: 'message from client'
```

#### Client:

```python
from zmq_tubes import Tube, TubeNode, TubeMessage

tube = Tube(
  name='Client',
  addr='ipc:///tmp/dealer_dealer.pipe',
  tube_type='DEALER'
)


async def handler(response: TubeMessage):
  print(response.payload)


node = TubeNode()
node.register_tube(tube, 'test/#')
node.register_handler('test/#', handler)

node.send('test/xxx', 'message from client')
# output: 'message from server'
```
