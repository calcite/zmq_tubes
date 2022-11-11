import argparse
import json
import time
import yaml
import zmq


def logs(socket, args):
    socket.send(b'__enabled__')
    while True:
        if socket.poll(zmq.POLLIN):
            data = socket.recv_multipart()
            if data:
                if data[0] == b'__connect__':
                    socket.send(b'__enabled__')
                    continue
                elif data[0] == b'__disconnect__':
                    break
                if args.dump:
                    args.dump.write(b' '.join(data) + b'\n')
                if args.notime:
                    data.pop(0)
                print(' '.join([m.decode('utf-8', 'backslashreplace')
                                for m in data]))
        time.sleep(.01)


def get_socket(addr: str):
    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    socket.connect(addr)
    return socket


def get_schema(socket):
    socket.send(b'__get_schema__')
    for _ in range(10):
        data = socket.recv_multipart()
        if data and data[0] == b'__schema__':
            return json.loads(data[1].decode())


def main():
    parser = argparse.ArgumentParser(
        prog='ZMQ TubeNode monitor',
        description='This tool can monitor zmq TubeNode')
    parser.add_argument('-s', '--socket', help='Path to monitor socket.',
                        required=True)
    subparsers = parser.add_subparsers(help='sub-command help')
    # Schema
    parser_schema = subparsers.add_parser('get_schema',
                                          help='Get tubeNode schema')
    parser_schema.set_defaults(
        func=lambda socket, args: print(yaml.dump(get_schema(socket)))
    )
    # Logs
    parser_logs = subparsers.add_parser('logs',
                                        help='Logs tubeNode communication.')
    parser_logs.add_argument('--notime', action='store_true',
                             help='Does not show relative time')
    parser_logs.add_argument('-d', '--dump', type=argparse.FileType('wb'),
                             help='Output dump file')
    parser_logs.set_defaults(func=lambda socket, args: logs(socket, args))


    args = parser.parse_args()
    socket = get_socket(args.socket)
    args.func(socket, args)


if __name__ == "__main__":
    main()
