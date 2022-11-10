import json

import time
import yaml

import zmq

context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect("ipc:///storage//display.monitor")

# socket.send(b'__get_schema__')

socket.send(b'__enabled__')
while True:
    if socket.poll(zmq.POLLIN):
        data = socket.recv_multipart()
        print(data)
        if data:
            if data[0] == b'__connect__':
                socket.send(b'__enabled__')
                continue
            elif data[0] == b'__disconnect__':
                break
            elif data[0] == b'__schema__':
                print(yaml.dump(json.loads(data[1].decode())))
                break
            print(' '.join([m.decode('utf-8', 'backslashreplace') for m in data]))
    time.sleep(.01)
