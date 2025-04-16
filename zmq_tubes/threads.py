import time

import concurrent
from threading import Thread, Lock, Event, current_thread
import zmq
from zmq import Poller, Context

from .manager import TubeMessage, Tube as AsyncTube, TubeNode as AsyncTubeNode, \
    TubeMethodNotSupported, TubeMessageError, TubeMessageTimeout, \
    TubeMonitor as AsyncTubeMonitor, TubeTopicNotConfigured, TubeConnectionError


class TubeThreadDeadLock(Exception): pass


class StoppableThread(Thread):
    def __init__(self, *args, **kwargs):
        self.stop_event = Event()
        # kwargs['daemon'] = True
        super().__init__(*args, **kwargs)

    def is_stopped(self):
        return self.stop_event.is_set()

    def stop(self):
        self.stop_event.set()
        self.join(timeout=1)


class TubeMonitor(AsyncTubeMonitor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Because singleton execute __init__ for each try.
        if hasattr(self, 'lock') and self.lock:
            return
        self.context = Context.instance()
        self.lock = Lock()

    def connect(self):
        self.raw_socket = self.context.socket(zmq.PAIR)
        self.raw_socket.bind(self.addr)
        self.raw_socket.__dict__['monitor'] = self
        try:
            with self.lock:
                self.raw_socket.send(b'__connect__', flags=zmq.NOBLOCK)
        except zmq.ZMQError:
            # The monitor is not connected
            pass

    def close(self):
        if self.raw_socket:
            try:
                with self.lock:
                    self.raw_socket.send(b'__disconnect__', flags=zmq.NOBLOCK)
                time.sleep(.1)
            except zmq.ZMQError:
                # The monitor is not connected
                pass
            self.raw_socket.close(1)
            self.raw_socket = None
        self.enabled = False

    def process(self):
        if self.raw_socket:
            self.__process_cmd(self.raw_socket.recv())

    def send_message(self, msg: TubeMessage):
        if self.raw_socket and self.enabled:
            row_msg = self.__format_message(msg, '>')
            with self.lock:
                self.raw_socket.send_multipart(row_msg)

    def receive_message(self, msg: TubeMessage):
        if self.raw_socket and self.enabled:
            row_msg = self.__format_message(msg, '<')
            with self.lock:
                self.raw_socket.send_multipart(row_msg)


class Tube(AsyncTube):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lock = Lock()
        self.context = Context().instance()

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
        if not self.lock.acquire(timeout=10):
            raise TubeThreadDeadLock(f"The tube '{self.name}' waits more then "
                                     f"10s for access to socket.")
        try:
            message.raw_socket.send_multipart(raw_msg)
            try:
                if self.monitor:
                    self.monitor.send_message(message)
            except Exception as ex:
                self.logger.error(
                    "The error with sending of an outgoing message "
                    "to the monitor tube.",
                    exc_info=ex)
        except (TypeError, zmq.ZMQError) as ex:
            raise TubeMessageError(
                f"The message '{message}' does not be sent.") from ex
        finally:
            self.lock.release()

    def request(self, *args, post_send_callback=None, **kwargs) -> TubeMessage:
        """
                Send request
                :param request: Optional[TubeMessage]
                :param topic: Optional[str]
                :param payload: Optional[dict]
                :param timeout: int
                :param post_send_callback: Optional[Callable]
                :param utf8_decoding: bool (default True)
                :return: TubeMessage
                """
        if args:
            if isinstance(args[0], TubeMessage):
                return self.__request_message(
                    *args, post_send_callback=post_send_callback, **kwargs
                )
            elif isinstance(args[0], str):
                return self.__request_payload(
                    *args, post_send_callback=post_send_callback, **kwargs
                )
        elif kwargs:
            if 'message' in kwargs:
                return self.__request_message(
                    post_send_callback=post_send_callback, **kwargs
                )
            elif 'topic' in kwargs:
                return self.__request_payload(
                    post_send_callback=post_send_callback, **kwargs
                )
        raise NotImplementedError("Unknown type of topic")

    def __request_payload(self, topic: str, payload=None, timeout=None,
                          post_send_callback=None, utf8_decoding=None):
        request = TubeMessage(
            self,
            payload=payload,
            topic=topic,
            raw_socket=self.raw_socket,
        )
        return self.__request_message(request, timeout=timeout,
                                      post_send_callback=post_send_callback,
                                      utf8_decoding=utf8_decoding)

    def __request_message(self, request: TubeMessage, timeout: int = 30,
                          post_send_callback=None, utf8_decoding=None):
        if self.tube_type != zmq.REQ:
            raise TubeMethodNotSupported(
                f"The tube '{self.name}' (type: '{self.tube_type_name}') "
                f"can request topic."
            )
        try:
            self.send(request)
            if post_send_callback:
                post_send_callback(request)
            if request.raw_socket.poll(timeout * 1000) != 0:
                response = self.receive_data(
                    raw_socket=request.raw_socket,
                    utf8_decoding=utf8_decoding
                )
                if response.topic != request.topic:
                    raise TubeMessageError(
                        f"The response comes to different topic "
                        f"({request.topic} != {response.topic}).")
                return response
        finally:
            if not self.is_persistent:
                # self.logger.debug(f"Close tube {self.name}")
                if request.raw_socket and not request.raw_socket.closed:
                    request.raw_socket.close(1000)
        if self.is_closed:
            raise TubeConnectionError(f'The tube {self.name} was closed.')
        raise TubeMessageTimeout(
            f"No answer for the request in {timeout}s. Topic: {request.topic}")

    def receive_data(self, raw_socket=None, timeout=3, utf8_decoding=None):
        if not raw_socket:
            raw_socket = self.raw_socket
        if not self.lock.acquire(timeout=timeout):
            raise TubeThreadDeadLock(f"The tube '{self.name}' waits more then "
                                     f"{timeout}s for access to socket.")
        try:
            raw_data = raw_socket.recv_multipart()
        finally:
            self.lock.release()
        self.logger.debug(
            f"Received (tube {self.name}): {raw_data}")
        message = TubeMessage(tube=self, raw_socket=raw_socket)
        message.parse(
            raw_data,
            self.utf8_decoding if utf8_decoding is None else utf8_decoding
        )
        try:
            if self.monitor:
                self.monitor.receive_message(message)
        except Exception as ex:
            self.logger.error("The error with sending of an incoming message "
                              "to the monitor tube.",
                              exc_info=ex)
        return message


class TubeNode(AsyncTubeNode):
    __TUBE_CLASS = Tube
    __MONITOR_CLASS = TubeMonitor

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.main_thread = None
        self.max_workers = None

    def __enter__(self):
        self.connect()
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.close()

    def connect(self):
        """
        opens all persistent connections
        """
        for tube in self.tubes:
            tube.connect()
        for monitor in self.__monitors:
            monitor.connect()

    def close(self):
        """
        close all persistent connections
        """
        for tube in self.tubes:
            tube.close()
        for monitor in self.__monitors:
            monitor.close()

    def send(self, topic: str, payload=None, tube=None):
        if not tube:
            tube = self.get_tube_by_topic(topic, [zmq.DEALER])
            if not tube:
                raise TubeTopicNotConfigured(f'The topic "{topic}" is not '
                                             f'assigned to any Tube for '
                                             f'dealer.')
        tube.send(topic, payload)

    def request(self, topic: str, payload=None, timeout=30,
                post_send_callback=None, utf8_decoding=None) -> TubeMessage:
        tube = self.get_tube_by_topic(topic, [zmq.REQ])
        if not tube:
            raise TubeTopicNotConfigured(f'The topic "{topic}" is not assigned '
                                         f'to any Tube for request.')
        res = tube.request(topic, payload, timeout=timeout,
                           post_send_callback=post_send_callback,
                           utf8_decoding=utf8_decoding)
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

    def stop(self):
        if self.main_thread:
            self.main_thread.stop()

    def start(self):
        def _callback_wrapper(_callback, _request: TubeMessage):
            tube = _request.tube
            response = _callback(_request)
            if not isinstance(response, TubeMessage):
                if tube.tube_type in [zmq.ROUTER]:
                    raise TubeMessageError(
                        f"The response of the {tube.tube_type_name} "
                        f"callback has to be a instance of TubeMessage class.")
                _payload = response
                response = _request.create_response()
                response.payload = _payload
            else:
                if tube.tube_type in [zmq.ROUTER] and\
                        response.request.identity != response.identity:
                    raise TubeMessageError(
                        "The TubeMessage response object doesn't be created "
                        "from request object.")
            try:
                tube.send(response)
            except TubeConnectionError:
                self.logger.warning(
                    f"The client (tube '{tube.name}') closes the socket for "
                    f"sending answer. Probably timeout.")

        def _one_event(request):
            callbacks = self.get_callback_by_topic(request.topic, request.tube)
            if not callbacks:
                if self.warning_not_mach_topic:
                    self.logger.warning(
                        f"Incoming message does not match any topic, "
                        f"it is ignored (topic: {request.topic})"
                    )
                return
            c_process = current_thread()
            if request.tube.tube_type == zmq.SUB:
                for callback in callbacks:
                    c_process.name = 'zmq/worker/sub'
                    callback(request)
            elif request.tube.tube_type == zmq.REP:
                c_process.name = 'zmq/worker/rep'
                _callback_wrapper(callbacks[-1], request)
            elif request.tube.tube_type == zmq.ROUTER:
                c_process.name = 'zmq/worker/router'
                _callback_wrapper(callbacks[-1], request)
            elif request.tube.tube_type == zmq.DEALER:
                c_process.name = 'zmq/worker/router'
                callbacks[-1](request)

        def _main_loop():
            poller = Poller()
            run_this_thread = False
            for tube in self.tubes:
                if tube.tube_type in [zmq.SUB, zmq.REP, zmq.ROUTER, zmq.DEALER]:
                    poller.register(tube.raw_socket, zmq.POLLIN)
                    run_this_thread = True
            for monitor in self.__monitors:
                poller.register(monitor.raw_socket, zmq.POLLIN)
                run_this_thread = True
            if not run_this_thread:
                self.logger.debug("The main process is disabled, "
                                  "There is not registered any supported tube.")
                return
            self.logger.info("The main process was started.")
            cur_thread = current_thread()
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_workers,
                    thread_name_prefix='zmq/worker/') as executor:
                while not cur_thread.is_stopped():
                    try:
                        events = poller.poll(timeout=100)
                    except zmq.error.ZMQError:
                        # This happens during shutdown
                        continue
                    for event in events:
                        # self.logger.debug(f"New event {event}")
                        raw_socket = event[0]
                        if isinstance(raw_socket, object) and \
                                'monitor' in raw_socket.__dict__:
                            try:
                                monitor = raw_socket.__dict__['monitor']
                                executor.submit(monitor.process)
                            except Exception as ex:
                                self.logger.error(
                                    "The monitor event process failed.",
                                    exc_info=ex)
                            continue
                        tube: Tube = raw_socket.__dict__['tube']
                        request = tube.receive_data(
                            raw_socket=raw_socket
                        )
                        req_tubes = self._tubes.match(request.topic)
                        if req_tubes and tube not in req_tubes:
                            # This message is not for this node.
                            # The topic is not registered for this node.
                            continue
                        executor.submit(_one_event, request)
            self.logger.info("The main process was ended.")

        if not self.main_thread:
            self.main_thread = StoppableThread(target=_main_loop,
                                               name='zmq/main')
            self.main_thread.start()
            time.sleep(.2)  # wait for main thread is ready
        return self.main_thread
