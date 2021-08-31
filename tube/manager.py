import datetime
import logging
from collections import Callable
from functools import partial

import yaml
import zmq
from zmq.asyncio import Poller, Context, Socket
from cachetools import TTLCache

from tube.matcher import MQTTMatcher


class TubeException(Exception): pass
class TubeTopicNotConfigured(Exception): pass
class TubeMessageError(Exception): pass
class TubeMessageTimeout(Exception): pass

ZMQ_SOCKET_TYPE_MAPPING = {
    'SUB': zmq.SUB,
    'PUB': zmq.PUB,
    'REQ': zmq.REQ,
    'REP': zmq.REP,
    'ROUTER': zmq.ROUTER,
    'DEALER': zmq.DEALER,
    'PAIR': zmq.PAIR
}

class SocketTemplate:

    TYPE_SERVER = 'server'
    TYPE_CLIENT = 'client'

    def __init__(self, **kwargs):
        self.__socket: Socket = None
        self.context = Context().instance()
        self.socket_info = kwargs
        self.addr = kwargs.get('addr')
        self.name = kwargs.get('name')
        self.socket_type = kwargs.get('socket_type')
        self.__server = kwargs.get('type') == self.TYPE_SERVER

    @property
    def addr(self) -> str:
        return self.__addr

    @addr.setter
    def addr(self, val: str):
        if not val:
            raise TubeException(f"The parameter 'addr' is required.")
        self.__addr = val

    @property
    def name(self) -> str:
        return self.__name if self.__name else self.__addr

    @name.setter
    def name(self, val: str):
        self.__name = val

    @property
    def socket_type(self) -> str:
        return self.__socket_type

    @socket_type.setter
    def socket_type(self, val: str):
        self.__socket_type = ZMQ_SOCKET_TYPE_MAPPING.get(val)
        if not self.__socket_type:
            raise TubeException(f"The socket '{self.name}' has got "
                                f"an unsupported socket_type.")

    @property
    def is_server(self) -> bool:
        return self.__server

    @property
    def socket(self) -> Socket:
        if self.socket_type in [zmq.SUB, zmq.REP]:
            if not self.__socket:
                self.__socket = self.context.socket(self.__socket_type)
                if self.is_server:
                    self.__socket.bind(self.addr)
                else:
                    self.__socket.connect(self.addr)
            return self.__socket

    def __create_socket(self, socket_info) -> Socket:

        socket = self.context.socket(socket_type)
        if socket_info.get('type') == 'server':
            # self.logger.info(
            #     f"The socket '{socket_name}' "
            #     f"(ZMQ.{socket_info.get('socket_type')}) "
            #     f"binds to the port {socket_info.get('addr')}"
            # )
            socket.bind(socket_info.get('addr'))
        else:
            # self.logger.info(
            #     f"The socket '{socket_name}' "
            #     f"(ZMQ.{socket_info.get('socket_type')}) "
            #     f"connects to the server {socket_info.get('addr')}"
            # )
            socket.connect(socket_info.get('addr'))
        socket.__dict__['name'] = socket_name
        if socket_type == zmq.SUB:
            socket.setsockopt(zmq.SUBSCRIBE, b'')
        return socket



class Manager:

    def __init__(self, schema_file=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__sockets_tree = MQTTMatcher()
        self.__callbacks = MQTTMatcher()
        self.__response_cache = TTLCache(maxsize=128, ttl=60)   # ttl in seconds
        if schema_file:
            self.load_schema(schema_file)

    def close(self):
        for socket in self.__sockets_tree.values():
            if isinstance(socket, Socket):
                socket.close()



    def load_schema(self, schema_filename):
        data = {}
        with open(schema_filename, 'r+') as fd:
            data = yaml.load(fd, Loader=yaml.FullLoader)
            self.logger.debug(f"The file '{schema_filename}' was loaded.")
        if 'sockets' in data:
            for socket_info in data['sockets']:
                if socket_info.get('socket_type') not in ['REQ']:
                    socket = self.__create_socket(socket_info)
                    self.register_socket(socket, socket_info.get('topics', []))
                else:
                    socket_template = partial(self.__create_socket, socket_info)
                    self.register_socket(socket_template,
                                         socket_info.get('topics', []))

    def get_socket_by_topic(self, topic: str) -> Socket:
        try:
            return next(self.__sockets_tree.iter_match(topic))
        except StopIteration:
            return None

    def get_socket_by_name(self, name: str) -> Socket:
        sockets = self.__sockets_tree.values()
        for socket in sockets:
            if socket.__dict__.get('name', '') == name:
                return socket
        return None

    def get_callback_by_topic(self, topic: str) -> Callable:
        try:
            return next(self.__callbacks.iter_match(topic))
        except StopIteration:
            return None

    @staticmethod
    def __format_payload(payload):
        if isinstance(payload, str):
            return payload.encode('utf8')
        elif isinstance(payload, (bytes, bytearray)):
            return payload
        elif isinstance(payload, (int, float)):
            return str(payload).encode('ascii')
        elif payload is None:
            return b''
        else:
            raise TypeError(
                'payload must be a string, bytearray, int, float or None.')

    def register_socket(self, socket: Socket, topics: [str]):
        socket_name = socket.__dict__.get('name', '')
        for topic in topics:
            self.logger.debug(f"The socket '{socket_name}' registers "
                              f"a topic: {topic}")
            self.__sockets_tree[topic] = socket

    def send(self, topic: str, payload=None, enabled_socket_types=None,
             socket=None, message_id=None):
        if not socket:
            socket = self.get_socket_by_topic(topic)
        if not socket:
            raise TubeTopicNotConfigured(f'The topic "{topic}" is not assign '
                                         f'to any ZMQ socket.')
        socket_type = socket.getsockopt(zmq.TYPE)
        if enabled_socket_types and socket_type not in enabled_socket_types:
            raise TubeException(
                f"This topic '{topic}' does not support this kind of send "
                f"method (type: {socket_type})")

        b_payload = self.__format_payload(payload)
        if message_id:
            message = [topic.encode('utf-8'), message_id.encode('utf-8'),
                       b_payload]
            self.logger.debug("Send to %s (message id: %s): %s",
                              topic, message_id, b_payload)
        else:
            message = [topic.encode('utf-8'), b_payload]
            self.logger.debug("Send to %s: %s", topic, b_payload)
        try:
            socket.send_multipart(message)
        except (TypeError, zmq.ZMQError) as ex:
            raise TubeMessageError(f"The message {message} does not be sent.") \
                from ex

    async def __recieve_data(self, socket, size_of_message):
        socket_name = socket.__dict__.get('name', '')
        raw_request = await socket.recv_multipart()
        self.logger.debug(
            f"Income (socket {socket_name}): {raw_request}")
        if len(raw_request) != size_of_message:
            raise TubeMessageError(
                f"The income message (from '{socket_name}') "
                f"is in unknown format.")
        return [it.decode('utf-8') for it in raw_request]

    def publish(self, topic: str, payload=None):
        self.send(topic, payload, [zmq.PUB])

    def subscribe(self, topic: str, fce: Callable):
        self.__callbacks[topic] = fce

    def register_handler(self, topic: str, fce: Callable):
        self.__callbacks[topic] = fce

    async def request(self, topic: str, payload=None, timeout=30):
        socket = self.get_socket_by_topic(topic)
        if isinstance(socket, partial):
            socket = socket()
        try:
            # This is a compromise between the quality of random ID and their
            # complexity.
            message_id = datetime.datetime.now().strftime('%s%f')
            self.send(topic, payload, [zmq.REQ], socket=socket,
                      message_id=message_id)
            for ix in range(0, 10*timeout):
                # _key = f'{topic}_{message_id}'
                # if _key in self.__response_cache:
                #     self.logger.debug(f"Reload from cache '{_key}'")
                #     # The response was received by another request.
                #     return self.__response_cache.pop(_key)
                while await socket.poll(100) != 0:
                    in_topic, in_message_id, in_payload = \
                        await self.__recieve_data(socket, 3)
                    # if in_topic != topic or in_message_id != message_id:
                    #     # This is situation, when the response is not for
                    #     # this request.
                    #     self.logger.debug(
                    #         f"Save to cache '{in_topic}_{in_message_id}'")
                    #     self.__response_cache[f'{in_topic}_{in_message_id}'] = \
                    #         payload
                    # else:
                    return payload
        finally:
            socket.close()
        raise TubeMessageTimeout(
            f"No answer for the request in {timeout}s. Topic: {topic}")

    async def loop(self):
        poller = Poller()
        sockets = self.__sockets_tree.values()
        run_this_thread = False
        for socket in sockets:
            if isinstance(socket, Socket) and \
                    socket.socket_type in [zmq.SUB, zmq.REP]:
                poller.register(socket, zmq.POLLIN)
                run_this_thread = True
        if not run_this_thread:
            return
        self.logger.info("Main loop was started.")
        while True:
            events = await poller.poll()
            for event in events:
                socket = event[0]
                if socket.socket_type == zmq.SUB:
                    topic, payload = self.__recieve_data(socket, 2)
                    callback = self.get_callback_by_topic(topic)
                    await callback(payload)
                elif socket.socket_type == zmq.REP:
                    topic, message_id, payload = \
                        await self.__recieve_data(socket, 3)
                    callback = self.get_callback_by_topic(topic)
                    res = await callback(payload)
                    self.send(topic, res, socket=socket, message_id=message_id)

