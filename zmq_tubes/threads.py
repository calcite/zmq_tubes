from functools import singledispatchmethod
from threading import Thread, Lock, Event, current_thread
import zmq
from zmq import Poller, Context

from .manager import TubeMessage, Tube as AsyncTube, TubeNode as AsyncTubeNode,\
    TubeMethodNotSupported, TubeMessageError, TubeMessageTimeout, \
    TubeTopicNotConfigured


class TubeThreadDeadLock(Exception): pass


class DaemonThread(Thread):
    def __init__(self, *args, **kwargs):
        self.stop_event = Event()
        kwargs['daemon'] = True
        super().__init__(*args, **kwargs)

    def is_stopped(self):
        return self.stop_event.is_set()

    def stop(self):
        self.stop_event.set()
        self.join(timeout=10)


class Tube(AsyncTube):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lock = Lock()
        self.context = Context().instance()

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
        if not self.lock.acquire(timeout=10):
            raise TubeThreadDeadLock()
        try:
            message.raw_socket.send_multipart(raw_msg)
        except (TypeError, zmq.ZMQError) as ex:
            raise TubeMessageError(
                f"The message '{message}' does not be sent.") from ex
        finally:
            self.lock.release()

    @singledispatchmethod
    def request(self, arg) -> TubeMessage:
        raise NotImplementedError("Unknown type of topic")

    @request.register
    def _(self, topic: str, payload=None, timeout=None):
        request = TubeMessage(
            self,
            payload=payload,
            topic=topic,
            raw_socket=self.raw_socket
        )
        return self.request(request, timeout)

    @request.register
    def _(self, request: TubeMessage, timeout: int = 30):
        if self.tube_type != zmq.REQ:
            raise TubeMethodNotSupported(
                f"The tube '{self.name}' (type: '{self.tube_type_name}') "
                f"can request topic."
            )
        try:
            self.send(request)
            # if request.raw_socket.poll(timeout * 1000) == zmq.POLLIN:
            counter = timeout
            while (res := request.raw_socket.poll(1000)) != 0 or \
                    (counter != 0 and not self.is_closed):
                if res != 0:
                    response = self.receive_data(raw_socket=request.raw_socket)
                    if response.topic != request.topic:
                        raise TubeMessageError(
                            f"The response comes to different topic "
                            f"({request.topic} != {response.topic}).")
                    return response
                elif counter == 0:
                    self.logger.error("The request timout")
                    break
                counter -= 1
        finally:
            if not self.is_persistent:
                self.logger.debug(f"Close tube {self.name}")
                request.raw_socket.close(3000)
        raise TubeMessageTimeout(
            f"No answer for the request in {timeout}s. Topic: {request.topic}")

    def receive_data(self, raw_socket=None, timeout=3):
        if not raw_socket:
            raw_socket = self.raw_socket
        if not self.lock.acquire(timeout=timeout):
            raise TubeThreadDeadLock()
        try:
            raw_data = raw_socket.recv_multipart()
        finally:
            self.lock.release()
        self.logger.debug(
            f"Received (tube {self.name}): {raw_data}")
        message = TubeMessage(tube=self, raw_socket=raw_socket)
        message.parse(raw_data)
        return message


class TubeNode(AsyncTubeNode):
    __TUBE_CLASS = Tube

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__main_thread = None

    def request(self, topic: str, payload=None, timeout=30) \
            -> TubeMessage:
        tube = self.get_tube_by_topic(topic, [zmq.REQ])
        if not tube:
            raise TubeTopicNotConfigured(f'The topic "{topic}" is not assigned '
                                         f'to any Tube for request.')
        res = tube.request(topic, payload, timeout)
        return res

    def stop(self):
        if self.__main_thread:
            self.__main_thread.stop()

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
            except TubeThreadDeadLock:
                self.logger.error(
                    f"The tube '{tube.name}' waits more then "
                    f"10s for access to socket.")

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
            if not run_this_thread:
                self.logger.debug("The main process is disabled, "
                                  "There is not registered any supported tube.")
                return
            self.logger.info("The main process was started.")
            cur_thread = current_thread()
            while not cur_thread.is_stopped():
                events = poller.poll(timeout=100)
                for event in events:
                    # self.logger.debug(f"New event {event}")
                    raw_socket = event[0]
                    tube: Tube = raw_socket.__dict__['tube']
                    try:
                        request = tube.receive_data(
                            raw_socket=raw_socket
                        )
                        Thread(
                            target=_one_event,
                            args=(request, ),
                            name=f"zmq/worker/{tube.name}"
                        ).start()
                    except TubeThreadDeadLock:
                        self.logger.error(
                            f"The tube '{tube.name}' waits more then "
                            f"3s for access to socket.")
            self.logger.info("The main process was ended.")

        self.__main_thread = DaemonThread(target=_main_loop, name='zmq/main')
        self.__main_thread.start()
        return self.__main_thread
