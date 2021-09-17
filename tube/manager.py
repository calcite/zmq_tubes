import asyncio
import datetime
import json
import logging
from collections.abc import Callable
from functools import singledispatchmethod

import yaml
import zmq
from zmq.asyncio import Poller, Context, Socket

from tube.matcher import MQTTMatcher


class TubeException(Exception): pass            # flake8: E701
class TubeTopicNotConfigured(Exception): pass   # flake8: E701
class TubeMessageError(Exception): pass         # flake8: E701
class TubeMessageTimeout(Exception): pass       # flake8: E701


TUBE_TYPE_MAPPING = {
    'SUB': zmq.SUB,
    'PUB': zmq.PUB,
    'REQ': zmq.REQ,
    'REP': zmq.REP,
    'ROUTER': zmq.ROUTER,
    'DEALER': zmq.DEALER,
    'PAIR': zmq.PAIR
}


class TubeMessage:

    @staticmethod
    def __format_string(data):
        if isinstance(data, str):
            return data.encode('utf8')
        elif isinstance(data, (bytes, bytearray)):
            return data
        elif isinstance(data, (int, float)):
            return str(data).encode('ascii')
        elif data is None:
            return b''
        else:
            raise TypeError(
                'data must be a string, bytearray, int, float or None.')

    def __init__(self, tube, **kwargs):
        self.tube: Tube = tube
        self.topic = kwargs.get('topic')
        self.raw_socket = kwargs.get('raw_socket')
        self.identity = kwargs.get('identity')
        self.request: TubeMessage = kwargs.get('request')
        self.payload = kwargs.get('payload', '')

    def __repr__(self):
        res = ''
        if self.identity:
            res = f"indentity: {self.identity}, "
        return f"{res}topic: {self.topic},  payload: {self.payload}"

    @property
    def payload(self) -> str:
        return self.__payload

    @payload.setter
    def payload(self, value):
        if isinstance(value, list) or isinstance(value, dict):
            value = json.dumps(value)
        self.__payload = value

    def to_json(self):
        return json.loads(self.payload)

    def get_response(self, payload=None) -> 'TubeMessage':
        return TubeMessage(
            self.tube,
            topic=self.topic,
            raw_socket=self.raw_socket,
            identity=self.identity,
            request=self,
            payload=payload
        )

    def parse(self, data):
        if self.tube.tube_type == zmq.ROUTER:
            if len(data) != 4:
                raise TubeMessageError(
                    f"The received message (tube '{self.tube.name}') "
                    f"is in unknown format. '{data}'")
            self.identity = data.pop(0)
            data.pop(0)
        elif self.tube.tube_type == zmq.DEALER:
            if len(data) != 3:
                raise TubeMessageError(
                    f"The received message (tube '{self.tube.name}') "
                    f"is in unknown format. '{data}'")
            data.pop(0)

        data = [it.decode('utf-8') for it in data]
        if len(data) != 2:
            raise TubeMessageError(
                f"The received message (tube '{self.tube.name}') "
                f"is in unknown format. '{data}'")
        self.topic, self.payload = data

    def format_message(self):
        response = []
        if self.tube.tube_type == zmq.ROUTER:
            response += [self.identity, b'']
        if self.tube.tube_type == zmq.DEALER:
            response.append(b'')
        response += [self.topic, self.payload]
        return [self.__format_string(it) for it in response]


class Tube:

    TYPE_SERVER = 'server'
    TYPE_CLIENT = 'client'

    def __init__(self, **kwargs):
        """
        Constructor Tube
        :param addr:str         address of tube
        :param name:str         name of tube
        :param type_type:str    'server' or 'client' (default)
        :param type:str or int  type of tube
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__socket: Socket = None
        self.context = Context().instance()
        self.tube_info = kwargs
        self.addr = kwargs.get('addr')
        self.name = kwargs.get('name')
        self.tube_type = kwargs.get('tube_type')
        self.identity = kwargs.get('identity')
        self.__server = kwargs.get('type') == self.TYPE_SERVER

    @staticmethod
    def get_tube_type_name(tube_type):
        if isinstance(tube_type, int):
            for key, val in TUBE_TYPE_MAPPING.items():
                if tube_type == val:
                    return key
        return tube_type

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
            raise TubeException("The parameter 'addr' is required.")
        self.__addr = val

    @property
    def name(self) -> str:
        """
        returns name of this tube or a tube address
        """
        return self.__name if self.__name else self.__addr

    @name.setter
    def name(self, val: str):
        """
        set the name of this tube
        """
        self.__name = val

    @property
    def tube_type(self) -> str:
        """
        returns the tube type
        """
        return self.__tube_type

    @property
    def tube_type_name(self):
        return self.get_tube_type_name(self.__tube_type)

    @tube_type.setter
    def tube_type(self, val):
        """
        set the tube type
        """
        if not isinstance(val, int):
            self.__tube_type = TUBE_TYPE_MAPPING.get(val)
            if not self.__tube_type:
                raise TubeException(f"The tube '{self.name}' has got "
                                    f"an unsupported tube_type.")
        else:
            if val not in TUBE_TYPE_MAPPING.values():
                raise TubeException(f"The tube '{self.name}' has got "
                                    f"an unsupported tube_type.")
            self.__tube_type = val

    @property
    def is_server(self) -> bool:
        """
        Is the tube a server side?
        """
        return self.__server

    @property
    def is_persistent(self) -> bool:
        """
        Is the tube persistent?
        """
        return self.tube_type in [zmq.PUB, zmq.SUB, zmq.REP, zmq.ROUTER,
                                  zmq.DEALER]

    @property
    def raw_socket(self) -> Socket:
        """
        returns a native ZMQ Socket. For persistent tubes this returns still
        the same ZMQ socket.
        """
        if self.is_persistent:
            if not self.__socket:
                self.__socket = self.__create_socket()
            return self.__socket
        else:
            return self.__create_socket()

    def __create_socket(self) -> Socket:
        raw_socket = self.context.socket(self.__tube_type)
        if self.is_server:
            self.logger.debug(
                f"The tube '{self.name}' (ZMQ.{self.tube_type_name}) "
                f"binds to the port {self.addr}.")
            raw_socket.bind(self.addr)
        else:
            self.logger.debug(
                f"The tube '{self.name}' (ZMQ.{self.tube_type_name}) "
                f"connects to the server {self.addr}")
            raw_socket.connect(self.addr)
        raw_socket.__dict__['tube'] = self
        if self.tube_type == zmq.SUB:
            raw_socket.setsockopt(zmq.SUBSCRIBE, b'')
        if self.identity:
            self.logger.debug(
                f"Set identity '{self.identity}' for tube '{self.name}'."
            )
            raw_socket.setsockopt(zmq.IDENTITY, self.identity.encode('utf-8'))
        return raw_socket

    def connect(self):
        """
        For persistent tubes, this open connection (connect/bind) to address.
        """
        if self.is_persistent and self.__socket is None:
            return self.raw_socket

    def close(self):
        """
        For persistent tubes, this close connection.
        """
        if self.is_persistent and self.__socket:
            self.raw_socket.close()

    @singledispatchmethod
    def send(self, arg):
        raise NotImplementedError("Unknown type of topic")

    @send.register
    def _(self, topic: str, payload=None, raw_socket=None):
        """
        Send payload to topic.
        :param topic - topic
        :param payload - payload
        :param raw_socket - zmqSocket, it used for non permanent connection
        """
        message = TubeMessage(
            self,
            payload=payload,
            topic=topic,
            raw_socket=raw_socket if raw_socket else self.raw_socket
        )
        self.send(message)

    @send.register
    def _(self, message: TubeMessage):
        """
        Send message.
        :param message - TubeMessage
        """
        raw_msg = message.format_message()
        self.logger.debug("Send (tube: %s) to %s", self.name, raw_msg)
        try:
            message.raw_socket.send_multipart(raw_msg)
        except (TypeError, zmq.ZMQError) as ex:
            raise TubeMessageError(
                f"The message '{message}' does not be sent.") from ex

    @singledispatchmethod
    async def request(self, arg) -> TubeMessage:
        raise NotImplementedError("Unknown type of topic")

    @request.register
    async def _(self, topic: str, payload=None, timeout=None):
        request = TubeMessage(
            self,
            payload=payload,
            topic=topic,
            raw_socket=self.raw_socket
        )
        return await self.request(request, timeout)

    @request.register
    async def _(self, request: TubeMessage, timeout: int = 30):
        try:
            self.send(request)
            for ix in range(0, timeout * 10):
                while await request.raw_socket.poll(100) != 0:
                    response = await self.receive_data(
                        raw_socket=request.raw_socket)
                    if response.topic != request.topic:
                        raise TubeMessageError(
                            f"The response comes to different topic "
                            f"({request.topic} != {response.topic}).")
                    # self.logger.debug(f"Response from {response}")
                    return response
        finally:
            if not self.is_persistent:
                self.logger.debug(f"Close tube {self.name}")
                request.raw_socket.close()
        raise TubeMessageTimeout(
            f"No answer for the request in {timeout}s. Topic: {request.topic}")

    async def receive_data(self, raw_socket=None):
        if not raw_socket:
            raw_socket = self.raw_socket
        raw_data = await raw_socket.recv_multipart()
        self.logger.debug(
            f"Received (tube {self.name}): {raw_data}")
        message = TubeMessage(tube=self, raw_socket=raw_socket)
        message.parse(raw_data)
        return message


class TubeNode:

    def __init__(self, *, schema_file=None, schema=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__tubes_tree = MQTTMatcher()
        self.__callbacks = MQTTMatcher()
        if schema_file:
            self.load_schema(schema_file)
        if schema:
            self.parse_schema(schema)
        self.__stop_main_loop = False

    @property
    def tubes(self) -> [Tube]:
        """
        returns a list of all registered tubes
        """
        return self.__tubes_tree.values()

    @property
    def random_message_id(self):
        # This is a compromise between the quality of random ID and their
        # complexity.
        return datetime.datetime.now().strftime('%s%f')

    def connect(self):
        """
        opens all persistent connections
        """
        for tube in self.tubes:
            tube.connect()

    def close(self):
        """
        close all persistent connections
        """
        for tube in self.tubes:
            tube.close()

    def parse_schema(self, schema):
        """
        parses tubes from string
        """
        if isinstance(schema, str):
            schema = yaml.safe_load(schema)
        if 'tubes' in schema:
            for tube_info in schema['tubes']:
                tube = Tube(**tube_info)
                self.register_tube(tube, tube_info.get('topics', []))

    def load_schema(self, schema_filename: str):
        """
        loads tubes from yml file and register their topics.
        """
        schema = {}
        with open(schema_filename, 'r+') as fd:
            schema = fd.read()
            self.logger.debug(f"The file '{schema_filename}' was loaded.")
        self.parse_schema(schema)

    def get_tube_by_topic(self, topic: str) -> Tube:
        """
        returns the Tube which is assigned to topic.
        """
        try:
            return next(self.__tubes_tree.iter_match(topic))
        except StopIteration:
            return None

    def get_tube_by_name(self, name: str) -> Tube:
        """
        returns the Tube with the name
        """
        tubes = self.__tubes_tree.values()
        for tube in tubes:
            if tube.name == name:
                return tube
        return None

    def register_tube(self, tube: Tube, topics: [str]):
        """
        registers list of topics to the Tube
        """
        if isinstance(topics, str):
            topics = [topics]
        for topic in topics:
            self.logger.debug(f"The tube '{tube.name}' registers "
                              f"a topic: {topic}")
            self.__tubes_tree[topic] = tube

    def get_callback_by_topic(self, topic: str) -> Callable:
        try:
            return next(self.__callbacks.iter_match(topic))
        except StopIteration:
            return None

    def send(self, topic: str, payload=None, tube=None, raw_socket=None):
        if not tube:
            tube = self.get_tube_by_topic(topic)
        if not tube:
            raise TubeTopicNotConfigured(f'The topic "{topic}" is not assign '
                                         f'to any Tube.')
        tube.send(topic, payload, raw_socket=raw_socket)

    async def request(self, topic: str, payload=None, timeout=30) \
            -> TubeMessage:
        tube = self.get_tube_by_topic(topic)
        res = await tube.request(topic, payload, timeout)
        return res

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
        async def __callback_wrapper(_callback, _request: TubeMessage):
            response = await _callback(_request)
            if not isinstance(response, TubeMessage):
                if _request.tube.tube_type in [zmq.ROUTER]:
                    raise TubeMessageError(
                        f"The response of the {_request.tube.tube_type_name} "
                        f"callback has to be a instamce of TubeMessage class.")
                payload = response
                response = request.get_response()
                response.payload = payload
            else:
                if _request.tube.tube_type in [zmq.ROUTER] and\
                        response.request.identity != response.identity:
                    raise TubeMessageError(
                        "The TubeMessage response object doesn't be created "
                        "from request object.")
            request.tube.send(response)

        poller = Poller()
        loop = asyncio.get_event_loop()
        run_this_thread = False
        for tube in self.tubes:
            if tube.tube_type in [zmq.SUB, zmq.REP, zmq.ROUTER, zmq.DEALER]:
                poller.register(tube.raw_socket, zmq.POLLIN)
                run_this_thread = True
        if not run_this_thread:
            self.logger.debug("The main loop is disabled, "
                              "There is not registered any supported tube.")
            return
        self.logger.info("The main loop was started.")
        while not self.__stop_main_loop:
            events = await poller.poll(timeout=100)
            for event in events:
                raw_socket = event[0]
                tube: Tube = raw_socket.__dict__['tube']
                request = await tube.receive_data(raw_socket=raw_socket)
                callbacks = self.get_callback_by_topic(request.topic)
                if not callbacks:
                    self.logger.warning(
                        f"Incoming message does not match any topic, "
                        f"it is ignored (topic: {request.topic})"
                    )
                    continue
                # self.logger.debug(
                #     f"Incoming message for tube '{tube.name}'")
                if tube.tube_type == zmq.SUB:
                    for callback in callbacks:
                        loop.create_task(callback(request), name='zmq/sub')
                elif tube.tube_type == zmq.REP:
                    loop.create_task(
                        __callback_wrapper(callbacks[-1], request),
                        name='zmq/rep')
                elif tube.tube_type == zmq.ROUTER:
                    loop.create_task(
                        __callback_wrapper(callbacks[-1], request),
                        name='zmq/router'
                    )
                elif tube.tube_type == zmq.DEALER:
                    loop.create_task(callbacks[-1](request), name='zmq/dealer')
        self.logger.info("The main loop was ended.")
