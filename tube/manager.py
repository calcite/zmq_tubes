import asyncio
import datetime
import logging
from collections.abc import Callable

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

class TubeMessage:

    def __init__(self, socket, **kwargs):
        self.socket: TubeSocket = socket
        self.topic = kwargs.get('topic')
        self.message = kwargs.get('message')


class TubeSocket:

    TYPE_SERVER = 'server'
    TYPE_CLIENT = 'client'

    def __init__(self, **kwargs):
        """
        Constructor TubeSocket
        :param addr:str     address of server
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__socket: Socket = None
        self.context = Context().instance()
        self.socket_info = kwargs
        self.addr = kwargs.get('addr')
        self.name = kwargs.get('name')
        self.socket_type = kwargs.get('socket_type')
        self.identity = kwargs.get('identity')
        self.__server = kwargs.get('type') == self.TYPE_SERVER

    @staticmethod
    def get_socket_type_name(socket_type):
        if isinstance(socket_type, int):
            for key, val in ZMQ_SOCKET_TYPE_MAPPING.items():
                if socket_type == val:
                    return key
        return socket_type

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

    @property
    def addr(self) -> str:
        """
        returns the address
        """
        return self.__addr

    @addr.setter
    def addr(self, val: str):
        """
        set the address (format: 'protocol://interface:port')
        """
        if not val:
            raise TubeException(f"The parameter 'addr' is required.")
        self.__addr = val

    @property
    def name(self) -> str:
        """
        returns name of this socket or a socket address
        """
        return self.__name if self.__name else self.__addr

    @name.setter
    def name(self, val: str):
        """
        set the name of this socket
        """
        self.__name = val

    @property
    def socket_type(self) -> str:
        """
        returns the ZMQ socket type
        """
        return self.__socket_type

    @property
    def socket_type_name(self):
        return self.get_socket_type_name(self.__socket_type)

    @socket_type.setter
    def socket_type(self, val):
        """
        set the ZMQ socket type
        """
        if not isinstance(val, int):
            self.__socket_type = ZMQ_SOCKET_TYPE_MAPPING.get(val)
            if not self.__socket_type:
                raise TubeException(f"The socket '{self.name}' has got "
                                    f"an unsupported socket_type.")
        else:
            if val not in ZMQ_SOCKET_TYPE_MAPPING.values():
                raise TubeException(f"The socket '{self.name}' has got "
                                    f"an unsupported socket_type.")
            self.__socket_type = val

    @property
    def is_server(self) -> bool:
        """
        Is the socket a server side?
        """
        return self.__server

    @property
    def is_persistent(self) -> bool:
        """
        Is the socket persistent?
        """
        return self.socket_type in [zmq.PUB, zmq.SUB, zmq.REP, zmq.ROUTER]

    @property
    def raw_socket(self) -> Socket:
        """
        returns a native ZMQ Socket. For persistent sockets this returns still
        the same ZMQ socket.
        """
        if self.is_persistent:
            if not self.__socket:
                self.__socket = self.__create_socket()
            return self.__socket
        else:
            return self.__create_socket()

    def __create_socket(self) -> Socket:
        raw_socket = self.context.socket(self.__socket_type)
        if self.is_server:
            self.logger.debug(
                f"The socket '{self.name}' (ZMQ.{self.socket_type_name}) "
                f"binds to the port {self.addr}.")
            raw_socket.bind(self.addr)
        else:
            self.logger.debug(
                f"The socket '{self.name}' (ZMQ.{self.socket_type_name}) "
                f"connects to the server {self.addr}")
            raw_socket.connect(self.addr)
        raw_socket.__dict__['tube_socket'] = self
        if self.socket_type == zmq.SUB:
            raw_socket.setsockopt(zmq.SUBSCRIBE, b'')
        if self.identity:
            self.logger.debug(
                f"Set identity '{self.identity}' for socket '{self.name}'."
            )
            raw_socket.setsockopt(zmq.IDENTITY, self.identity.encode('utf-8'))
        return raw_socket

    def connect(self):
        """
        For persistent sockets, this open connection (connect/bind) to address.
        """
        if self.is_persistent and self.__socket is None:
            return self.raw_socket

    def close(self):
        """
        For persistent sockets, this close connection.
        """
        if self.is_persistent and self.__socket:
            self.raw_socket.close()

    def send(self, topic: str, payload=None, raw_socket=None):
        """
        Send payload to topic.
        """
        b_payload = self.__format_payload(payload)
        topic = topic.encode('utf-8')
        message = [topic, b_payload]
        if not raw_socket:
            raw_socket = self.raw_socket
        self.logger.debug("Send to %s: %s", topic, b_payload)
        try:
            raw_socket.send_multipart(message)
        except (TypeError, zmq.ZMQError) as ex:
            raise TubeMessageError(f"The message {message} does not be sent.") \
                from ex

    async def request(self, topic: str, payload=None, timeout=30):
        raw_socket = self.raw_socket
        try:
            self.send(topic, payload, raw_socket=raw_socket)
            for ix in range(0, timeout*10):
                while await raw_socket.poll(100) != 0:
                    in_topic, in_payload = await self.receive_data(raw_socket)
                    if in_topic != topic:
                        raise TubeMessageError(
                            f"The response comes to different topic "
                            f"({topic} != {in_topic}).")
                    return in_payload
        finally:
            if not self.is_persistent:
                self.logger.debug(f"Close socket {self.name}")
                raw_socket.close()
        raise TubeMessageTimeout(
            f"No answer for the request in {timeout}s. Topic: {topic}")

    async def receive_data(self, raw_socket=None):
        if not raw_socket:
            raw_socket = self.raw_socket
        raw_request = await raw_socket.recv_multipart()
        self.logger.debug(
            f"Income (socket {self.name}): {raw_request}")
        if len(raw_request) != 2:
            raise TubeMessageError(
                f"The income message (from '{self.name}') "
                f"is in unknown format. '{raw_request}'")
        return [it.decode('utf-8') for it in raw_request]


class TubeManager:

    def __init__(self, schema_file=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__sockets_tree = MQTTMatcher()
        self.__callbacks = MQTTMatcher()
        self.__response_cache = TTLCache(maxsize=128, ttl=60)   # ttl in seconds
        if schema_file:
            self.load_schema(schema_file)
        self.__stop_main_loop = False

    @property
    def sockets(self) -> [TubeSocket]:
        """
        returns a list of all registered sockets
        """
        return self.__sockets_tree.values()

    @property
    def random_message_id(self):
        # This is a compromise between the quality of random ID and their
        # complexity.
        return datetime.datetime.now().strftime('%s%f')

    def connect(self):
        """
        opens all persistent connections
        """
        for socket in self.sockets:
            socket.connect()

    def close(self):
        """
        close all persistent connections
        """
        for socket in self.sockets:
            socket.close()

    def load_schema(self, schema_filename: str):
        """
        loads tube sockets from yml file and register their topics.
        """
        data = {}
        with open(schema_filename, 'r+') as fd:
            data = yaml.load(fd, Loader=yaml.FullLoader)
            self.logger.debug(f"The file '{schema_filename}' was loaded.")
        if 'sockets' in data:
            for socket_info in data['sockets']:
                socket = TubeSocket(**socket_info)
                self.register_socket(socket, socket_info.get('topics', []))

    def get_socket_by_topic(self, topic: str) -> TubeSocket:
        """
        returns the TubeSocket which is assigned to topic.
        """
        try:
            return next(self.__sockets_tree.iter_match(topic))
        except StopIteration:
            return None

    def get_socket_by_name(self, name: str) -> TubeSocket:
        """
        returns the TubeSocket with the name
        """
        sockets = self.__sockets_tree.values()
        for socket in sockets:
            if socket.name == name:
                return socket
        return None

    def register_socket(self, socket: TubeSocket, topics: [str]):
        """
        registers list of topics to the TubeSocket
        """
        if isinstance(topics, str):
            topics = [topics]
        for topic in topics:
            self.logger.debug(f"The socket '{socket.name}' registers "
                              f"a topic: {topic}")
            self.__sockets_tree[topic] = socket

    def get_callback_by_topic(self, topic: str) -> Callable:
        try:
            return next(self.__callbacks.iter_match(topic))
        except StopIteration:
            return None

    def send(self, topic: str, payload=None, socket=None, raw_socket=None):
        if not socket:
            socket = self.get_socket_by_topic(topic)
        if not socket:
            raise TubeTopicNotConfigured(f'The topic "{topic}" is not assign '
                                         f'to any TubeSocket.')

        socket.send(topic, payload, raw_socket=raw_socket)

    async def request(self, topic: str, payload=None, timeout=30):
        socket = self.get_socket_by_topic(topic)
        return await socket.request(topic, payload, timeout)

    def publish(self, topic: str, payload=None):
        self.send(topic, payload)

    def subscribe(self, topic: str, fce: Callable):
        self.register_handler(topic, fce)

    def register_handler(self, topic: str, fce: Callable):
        try:
            self.__callbacks[topic]
        except KeyError:
            self.__callbacks[topic] = []
        self.__callbacks[topic].append(fce)

    def stop(self):
        self.self.__stop_main_loop = True

    async def start(self):
        async def __callback_wrapper(callback, topic, payload, raw_socket):
            res = await callback(payload)
            __socket = raw_socket.__dict__['tube_socket']
            __socket.send(topic, res, raw_socket=raw_socket)

        poller = Poller()
        loop = asyncio.get_event_loop()
        run_this_thread = False
        for socket in self.sockets:
            if socket.socket_type in [zmq.SUB, zmq.REP, zmq.ROUTER]:
                poller.register(socket.raw_socket, zmq.POLLIN)
                run_this_thread = True
        if not run_this_thread:
            self.logger.debug("The main loop is disabled, "
                              "There is not registered any supported socket.")
            return
        self.logger.info(f"Main loop was started.")
        while not self.__stop_main_loop:
            events = await poller.poll(timeout=100)
            for event in events:
                raw_socket = event[0]
                socket: TubeSocket = raw_socket.__dict__['tube_socket']
                topic, payload = \
                    await socket.receive_data(raw_socket=raw_socket)
                callbacks = self.get_callback_by_topic(topic)
                if not callbacks:
                    self.logger.warning(
                        f"Incoming message does not match any topic, "
                        f"it is ignored (topic: {topic})"
                    )
                    continue
                self.logger.debug(
                    f"Incoming message for socket '{socket.name}'")
                if socket.socket_type == zmq.SUB:
                    for callback in callbacks:
                        loop.create_task(callback(payload), name='zmq/sub')
                elif socket.socket_type == zmq.REP:
                    loop.create_task(
                        __callback_wrapper(callbacks[-1], topic, payload,
                                           raw_socket),
                        name='zmq/rep'
                    )
                elif socket.socket_type == zmq.ROUTER:
                    loop.create_task(
                        __callback_wrapper(callbacks[-1], topic, payload,
                                           raw_socket),
                        name='zmq/router'
                    )
        self.logger.info(f"Main loop was ended.")
