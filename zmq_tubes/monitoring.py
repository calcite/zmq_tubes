import argparse
import json
import logging

import sys
import time
import zmq

from zmq_tubes.threads import TubeNode, Tube

last_result = None
last_time = None

try:
    import yaml
except ImportError:
    sys.stderr.write("For the simulation is required install "
                     "pyaml package.")
    sys.exit(1)


def get_socket(addr: str):
    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    socket.connect(addr)
    return socket


def logs(addr, dump_file, notime, print_stdout=True):
    """
    Log ZMQ communication
    :param addr:  address of monitor socket
    :param dump_file: file descriptor of file for same ZMQMessages
    :param notime: bool: disable printing of relative time (default is False)
    :param print_stdout bool: print ZMQMessages to stdout
    """
    socket = get_socket(addr)
    socket.send(b'__enabled__', flags=zmq.NOBLOCK)
    try:
        while True:
            if socket.poll(zmq.POLLIN):
                data = socket.recv_multipart()
                if data:
                    if data[0] == b'__connect__':
                        socket.send(b'__enabled__', flags=zmq.NOBLOCK)
                        continue
                    elif data[0] == b'__disconnect__':
                        break
                    if dump_file:
                        dump_file.write(b' '.join(data) + b'\n')
                    if print_stdout:
                        data = [m.decode('utf-8', 'backslashreplace')
                                for m in data]
                        if notime:
                            data.pop(0)
                        print(' '.join(data))
    except KeyboardInterrupt:
        pass


def get_schema(addr):
    """
    Get ZMQTube schema from connected application
    :param addr: address of monitor socket
    """
    socket = get_socket(addr)
    socket.send(b'__get_schema__')
    for _ in range(10):
        data = socket.recv_multipart()
        if data and data[0] == b'__schema__':
            return json.loads(data[1].decode())


def simulate_speed(rtime, speed):
    global last_time
    if last_time:
        dt = rtime * speed - (time.time() - last_time)
        if dt > 0:
            time.sleep(dt)
    last_time = time.time()


def simulate_send(node: TubeNode, line, speed):
    global last_result
    rtime, tube_name, direction, msg = \
        line.decode().rstrip().split(' ', 3)
    if direction != '<':
        if direction == '>' and last_result and last_result != msg:
            logging.warning("The request result is different: "
                            f"'{msg}' != '{last_result}'")
            last_result = None
        return
    msg = msg.split(' ', 1)
    topic = msg.pop(0)
    data = msg.pop(0) if msg else ''
    tube = node.get_tube_by_name(tube_name)
    if not tube:
        sys.stderr.write(f'The tube {tube_name} does not exist.\n')
        return
    if speed:
        simulate_speed(float(rtime), speed)
    if tube.tube_type == zmq.REQ:
        res = tube.request(topic, data, timeout=-1)
        last_result = b' '.join(res.format_message()[-2:]).decode()
    else:
        tube.send(topic, data)


def simulate(schema_yaml, dump_file, speed):
    """
    Simulate ZMQ communication
    :param schema_yaml: the file descriptor of simulator definition
    :param dump_file: the file descriptor of ZMQTube dump communication
    :param speed: float - speed of playback 0 - no blocking, 1 - real time
    """
    schema = yaml.safe_load(schema_yaml)
    node = TubeNode()
    for tube_info in schema['tubes']:
        if 'monitor' in tube_info:
            del tube_info['monitor']
        tube = Tube(**tube_info)
        node.register_tube(tube, ['#'])

    with node:
        while True:
            line = dump_file.readline()
            if not line:
                break
            simulate_send(node, line, speed)


def main():
    parser = argparse.ArgumentParser(
        prog='ZMQ TubeNode monitor',
        description='This tool can monitor zmq TubeNode')
    parser.add_argument('-v', '--verbose', help='Verbose.', action='store_true')
    subparsers = parser.add_subparsers(help='sub-command help')
    # Schema
    parser_schema = subparsers.add_parser('get_schema',
                                          help='Get tubeNode schema')
    parser_schema.add_argument('socket', help='Path to monitor socket.')
    parser_schema.set_defaults(
        func=lambda args: print(yaml.dump(get_schema(args.socket)))
    )
    # Logs
    parser_logs = subparsers.add_parser('logs',
                                        help='Logs tubeNode communication.')
    parser_logs.add_argument('socket', help='Path to monitor socket.')
    parser_logs.add_argument('--notime', action='store_true',
                             help='Does not show relative time')
    parser_logs.add_argument('-d', '--dump', type=argparse.FileType('wb'),
                             help='Output dump file')
    parser_logs.set_defaults(func=lambda args: logs(args.socket, args.dump,
                                                    args.notime))

    # Simulate
    parser_sim = subparsers.add_parser('simulate',
                                       help='Simulate tubeNode communication.')
    parser_sim.add_argument('schema', type=argparse.FileType('r'),
                            help='The tubeNode schema file')
    parser_sim.add_argument('dump', type=argparse.FileType('rb'),
                            help='Dump file')
    parser_sim.add_argument('-s', '--speed', type=float, default=1,
                            help='Speed of simulation. 0 - no wait, '
                                 '1 - real speed (default)')
    parser_sim.set_defaults(func=lambda args: simulate(args.schema, args.dump,
                                                       args.speed))

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    args.func(args)


if __name__ == "__main__":
    main()
