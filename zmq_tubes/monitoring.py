import json

import time
import yaml

import zmq

context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect("ipc:///storage//display.monitor")

socket.send(b'get_schema')
data = socket.recv().decode()
print(yaml.dump(json.loads(data)))

socket.send(b'enabled')
while True:
    if socket.poll(zmq.POLLIN):
        data = socket.recv_multipart()
        print(' '.join([m.decode('utf-8', 'backslashreplace') for m in data]))
    time.sleep(.01)
