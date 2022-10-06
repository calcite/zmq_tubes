import sys

import asyncio
import json
import logging
from collections.abc import Callable

import zmq
from zmq.asyncio import Poller, Context, Socket

from zmq_tubes.matcher import TopicMatcher


class TubeException(Exception): pass            # flake8: E701
class TubeTopicNotConfigured(TubeException): pass   # flake8: E701
class TubeMessageError(TubeException): pass         # flake8: E701
class TubeMessageTimeout(TubeException): pass       # flake8: E701
class TubeMethodNotSupported(TubeException): pass   # flake8: E701
class TubeConnectionError(TubeException): pass   # flake8: E701


LESS38 = sys.version_info < (3, 8)

TUBE_TYPE_MAPPING = {
    'SUB': zmq.SUB,
    'PUB': zmq.PUB,
    'REQ': zmq.REQ,
    'REP': zmq.REP,
    'ROUTER': zmq.ROUTER,
    'DEALER': zmq.DEALER,
    'PAIR': zmq.PAIR
}


def flatten(llist):
    if isinstance(llist, list):
        return sum(llist, [])
    return llist


class TubeMessage:

    @staticmethod
    def _format_string(data):
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
        return self._payload

    @payload.setter
    def payload(self, value):
        if isinstance(value, list) or isinstance(value, dict):
            value = json.dumps(value)
        self._payload = value

    def from_json(self):
        return json.loads(self.payload)

    def create_response(self, payload=None) -> 'TubeMessage':
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
        return [self._format_string(it) for it in response]


class Tube:

    def __init__(self, **kwargs):
        """
        Constructor Tube
        :param addr:str         address of tube
        :param name:str         name of tube
        :param server:bool   is this tube endpoint a server side (default False)
        :param type:str or int  type of tube
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self._socket: Socket = None
        self.context = Context().instance()
        self.tube_info = kwargs
        self.is_closed = False
        self._sockopts = {}
        self.sockopts = kwargs.get('sockopts', {})
        self.addr = kwargs.get('addr')
        self.name = kwargs.get('name')
        self._server = \
            str(kwargs.get('server', '')).lower() in ('yes', 'true', '1')
        self.tube_type = kwargs.get('tube_type')
        self.identity = kwargs.get('identity')

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
        return self._addr

    @addr.setter
    def addr(self, val: str):
        """
        set the address (format: 'protocol://interface:port')
        """
        if not val:
            raise TubeException("The parameter 'addr' is required.")
        self._addr = val

    @property
    def name(self) -> str:
        """
        returns name of this tube or a tube address
        """
        return self._name if self._name else self._addr

    @name.setter
    def name(self, val: str):
        """
        set the name of this tube
        """
        self._name = val

    @property
    def tube_type(self) -> str:
        """
        returns the tube type
        """
        return self._tube_type

    @property
    def tube_type_name(self):
        return self.get_tube_type_name(self._tube_type)

    @tube_type.setter
    def tube_type(self, val):
        """
        set the tube type
        """
        if not isinstance(val, int):
            self._tube_type = TUBE_TYPE_MAPPING.get(val)
            if not self._tube_type:
                raise TubeException(f"The tube '{self.name}' has got "
                                    f"an unsupported tube_type.")
        else:
            if val not in TUBE_TYPE_MAPPING.values():
                raise TubeException(f"The tube '{self.name}' has got "
                                    f"an unsupported tube_type.")
            self._tube_type = val
        if self._tube_type == zmq.SUB:
            self.add_sock_opt(zmq.SUBSCRIBE, '')

    @property
    def is_server(self) -> bool:
        """
        Is the tube a server side?
        """
        return self._server

    @property
    def is_persistent(self) -> bool:
        """
        Is the tube persistent?
        """
        return self.tube_type in [zmq.PUB, zmq.SUB, zmq.REP, zmq.ROUTER,
                                  zmq.DEALER]

    @property
    def is_connected(self):
        return self._socket is not None

    @property
    def raw_socket(self) -> Socket:
        """
        returns a native ZMQ Socket. For persistent tubes this returns still
        the same ZMQ socket.
        """
        if self.is_persistent:
            if not self._socket:
                self._socket = self._create_socket()
            return self._socket
        else:
            return self._create_socket()

    def add_sock_opt(self, key, val):
        if isinstance(key, str):
            key = zmq.__dict__[key]
        if isinstance(val, str):
            val = val.encode('utf8')
        self._sockopts[key] = val

    @property
    def sockopts(self):
        return self._sockopts.copy()

    @sockopts.setter
    def sockopts(self, opts: dict):
        self._sockopts = {}
        for key, val in opts.items():
            self.add_sock_opt(key, val)

    @property
    def identity(self):
        return self._sockopts.get(zmq.IDENTITY, b'').decode('utf8')

    @identity.setter
    def identity(self, val):
        if val:
            self.logger.debug(
                f"Set identity '{val}' for tube '{self.name}'."
            )
            self.add_sock_opt(zmq.IDENTITY, val)

    def _create_socket(self) -> Socket:
        raw_socket = self.context.socket(self._tube_type)
        if self.is_server:
            self.logger.debug(
                f"The tube '{self.name}' (ZMQ.{self.tube_type_name}) "
                f"binds to the port {self.addr}")
            raw_socket.bind(self.addr)
        else:
            self.logger.debug(
                f"The tube '{self.name}' (ZMQ.{self.tube_type_name}) "
                f"connects to the server {self.addr}")
            raw_socket.connect(self.addr)
        raw_socket.__dict__['tube'] = self
        for opt, val in self._sockopts.items():
            raw_socket.setsockopt(opt, val)
        return raw_socket

    def connect(self):
        """
        For persistent tubes, this open connection (connect/bind) to address.
        """
        if self.is_persistent and self._socket is None:
            self.raw_socket
        self.is_closed = False
        return self

    def close(self):
        """
        For persistent tubes, this close connection.
        """
        if self.is_persistent and self._socket:
            self.raw_socket.close()
        self.is_closed = True

    def send(self, *args, **kwargs):
        if args:
            if isinstance(args[0], TubeMessage):
                return self.__send_message(*args, **kwargs)
            elif isinstance(args[0], str):
                return self.__send_payload(*args, **kwargs)
        elif kwargs:
            if 'message' in kwargs:
                return self.__send_message(**kwargs)
            elif 'topic' in kwargs:
                return self.__send_payload(**kwargs)
        raise NotImplementedError("Unknown type of topic")

    def __send_payload(self, topic: str, payload=None, raw_socket=None):
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
        self.__send_message(message)

    def __send_message(self, message: TubeMessage):
        """
        Send message.
        :param message - TubeMessage
        """
        raw_msg = message.format_message()
        self.logger.debug("Send (tube: %s) to %s", self.name, raw_msg)
        if not message.raw_socket or message.raw_socket.closed:
            raise TubeConnectionError(
                f'The tube {message.tube.name} is already closed.')
        try:
            message.raw_socket.send_multipart(raw_msg)
        except (TypeError, zmq.ZMQError) as ex:
            raise TubeMessageError(
                f"The message '{message}' does not be sent.") from ex

    async def request(self, *args, **kwargs) -> TubeMessage:
        """
        Send request
        :param request: Optional[TubeMessage]
        :param topic: Optional[str]
        :param payload: Optional[dict]
        :param timeout: int
        :return:
        """
        if args:
            if isinstance(args[0], TubeMessage):
                return await self.__request_message(*args, **kwargs)
            elif isinstance(args[0], str):
                return await self.__request_payload(*args, **kwargs)
        elif kwargs:
            if 'message' in kwargs:
                return await self.__request_message(**kwargs)
            elif 'topic' in kwargs:
                return await self.__request_payload(**kwargs)
        raise NotImplementedError("Unknown type of topic")

    async def __request_payload(self, topic: str, payload=None, timeout=None):
        request = TubeMessage(
            self,
            payload=payload,
            topic=topic,
            raw_socket=self.raw_socket
        )
        return await self.__request_message(request, timeout)

    async def __request_message(self, request: TubeMessage, timeout: int = 30):
        if self.tube_type != zmq.REQ:
            raise TubeMethodNotSupported(
                f"The tube '{self.name}' (type: '{self.tube_type_name}') "
                f"can request topic."
            )
        try:
            self.send(request)
            if await request.raw_socket.poll(timeout * 1000) != 0:
                response = await self.receive_data(
                    raw_socket=request.raw_socket)
                if response.topic != request.topic:
                    raise TubeMessageError(
                        f"The response comes to different topic "
                        f"({request.topic} != {response.topic}).")
                return response
            else:
                self.logger.error("The request timout")
        finally:
            if not self.is_persistent:
                self.logger.debug(f"Close tube {self.name}")
                if request.raw_socket and not request.raw_socket.closed:
                    request.raw_socket.close()
        if self.is_closed:
            raise TubeConnectionError(f'The tube {self.name} was closed.')
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

    __TUBE_CLASS = Tube

    def __init__(self, *, schema=None, warning_not_mach_topic=True):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._tubes = TopicMatcher()
        self._callbacks = TopicMatcher()
        if schema:
            self.parse_schema(schema)
        self._stop_main_loop = False
        self.warning_not_mach_topic = warning_not_mach_topic

    def __enter__(self):
        self.connect()
        args = {} if LESS38 else {'name': 'zmq/main'}
        asyncio.create_task(self.start(), **args)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.close()

    @property
    def tubes(self) -> [Tube]:
        """
        returns a list of all registered tubes
        """
        return flatten(self._tubes.values())

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
        parses tubes from configuration
        """
        if 'tubes' in schema:
            for tube_info in schema['tubes']:
                tube = self.__TUBE_CLASS(**tube_info)
                self.register_tube(tube, tube_info.get('topics', []))

    def get_tube_by_topic(self, topic: str, types=None) -> Tube:
        """
        returns the Tube which is assigned to topic.
        Optional: we can specify a type of tube.
        """
        res = self._tubes.match(topic)
        if res and types:
            res = [t for t in res if t.tube_type in types]
        if not res:
            return None
        if isinstance(res, list):
            res = res.pop()
        return res

    def filter_tube_by_topic(self, topic: str, types=None) -> [(str, Tube)]:
        tubes = self._tubes.filter(topic)
        res = {}
        for top, tts in tubes:
            for tt in tts:
                if not types or tt.tube_type in types:
                    res[top] = tt
        return res

    def get_tube_by_name(self, name: str) -> Tube:
        """
        returns the Tube with the name
        """
        tubes = flatten(self._tubes.values())
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
            tubes = self._tubes.get_topic(topic) or []
            tubes.append(tube)
            self.logger.debug(f"The tube '{tube.name}' was registered to "
                              f"the topic: {topic}")
            self._tubes.set_topic(topic, tubes)

    def get_callback_by_topic(self, topic: str, tube=None) -> Callable:
        """
        This return callbacks for the topic.
        If any of the callbacks is assigned to the tube,
        it is returned only these. Otherwise, this returns all unassigned.
        """
        callbacks = []
        callbacks_for_tube = []
        for clb in self._callbacks.match(topic) or []:
            if 'tube' not in clb.__dict__:
                callbacks.append(clb)
            elif clb.__dict__['tube'] == tube:
                callbacks_for_tube.append(clb)
        return callbacks_for_tube if callbacks_for_tube else callbacks

    def send(self, topic: str, payload=None, tube=None):
        if not tube:
            tube = self.get_tube_by_topic(topic, [zmq.DEALER])
            if not tube:
                raise TubeTopicNotConfigured(f'The topic "{topic}" is not '
                                             f'assigned to any Tube for '
                                             f'dealer.')
        tube.send(topic, payload)

    async def request(self, topic: str, payload=None, timeout=30) \
            -> TubeMessage:
        tube = self.get_tube_by_topic(topic, [zmq.REQ])
        if not tube:
            raise TubeTopicNotConfigured(f'The topic "{topic}" is not assigned '
                                         f'to any Tube for request.')
        res = await tube.request(topic, payload, timeout)
        return res

    def publish(self, topic: str, payload=None):
        """
        In the case with asyncio, the first message is very often lost.
        The workaround is to connect the tube manually as soon as possible.
        """
        tube = self.get_tube_by_topic(topic, [zmq.PUB])
        if not tube:
            raise TubeTopicNotConfigured(f'The topic "{topic}" is not assigned '
                                         f'to any Tube for publishing.')
        self.send(topic, payload, tube)

    def subscribe(self, topic: str, fce: Callable):
        topic_tubes = self.filter_tube_by_topic(topic, [zmq.SUB])
        if not topic_tubes:
            raise TubeTopicNotConfigured(f'The topic "{topic}" is not assigned '
                                         f'to any Tube for subscribe.')
        for tube_topic, tube in topic_tubes.items():
            self.register_handler(tube_topic, fce, tube=tube)

    def register_handler(self, topic: str, fce: Callable, tube: Tube = None):
        """
        We can register more handlers for SUB and all will be executed.
        For REP, ROUTER and DEALER, there will be executed only
        the last registered.
        If we want to use DEALER as server and client on the same node,
        we have to specify which tube will be used for this handler.
        :param topic: str
        :param fce: Callable
        :param tube: Tube - only for the case DEALER x DEALER on the same node.
        """
        if tube:
            fce.__dict__['tube'] = tube
        self._callbacks.get_topic(topic, set_default=[]).append(fce)

    def stop(self):
        self._stop_main_loop = True

    async def start(self):
        async def _callback_wrapper(_callback, _request: TubeMessage):
            response = await _callback(_request)
            if not isinstance(response, TubeMessage):
                if _request.tube.tube_type in [zmq.ROUTER]:
                    raise TubeMessageError(
                        f"The response of the {_request.tube.tube_type_name} "
                        f"callback has to be a instance of TubeMessage class.")
                _payload = response
                response = _request.create_response()
                response.payload = _payload
            else:
                if _request.tube.tube_type in [zmq.ROUTER] and\
                        response.request.identity != response.identity:
                    raise TubeMessageError(
                        "The TubeMessage response object doesn't be created "
                        "from request object.")
            _request.tube.send(response)

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
        while not self._stop_main_loop:
            events = await poller.poll(timeout=100)
            # print(events)
            for event in events:
                raw_socket = event[0]
                tube: Tube = raw_socket.__dict__['tube']
                request = await tube.receive_data(raw_socket=raw_socket)
                callbacks = self.get_callback_by_topic(request.topic, tube)
                if not callbacks:
                    if self.warning_not_mach_topic:
                        self.logger.warning(
                            f"Incoming message does not match any topic, "
                            f"it is ignored (topic: {request.topic})"
                        )
                    continue
                if tube.tube_type == zmq.SUB:
                    args = {} if LESS38 else {'name': 'zmq/sub'}
                    for callback in callbacks:
                        loop.create_task(callback(request), **args)
                elif tube.tube_type == zmq.REP:
                    args = {} if LESS38 else {'name': 'zmq/rep'}
                    loop.create_task(
                        _callback_wrapper(callbacks[-1], request),
                        **args)
                elif tube.tube_type == zmq.ROUTER:
                    args = {} if LESS38 else {'name': 'zmq/router'}
                    loop.create_task(
                        _callback_wrapper(callbacks[-1], request),
                        **args
                    )
                elif tube.tube_type == zmq.DEALER:
                    args = {} if LESS38 else {'name': 'zmq/dealer'}
                    loop.create_task(callbacks[-1](request), **args)
        self.logger.info("The main loop was ended.")
