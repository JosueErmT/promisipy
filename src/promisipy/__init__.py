from typing import List, Any, Literal, Union
import threading
import multiprocessing


class EventLoop:
    promises: List["Promise"]

    def __init__(self):
        self.promises = []

    def register(self, promise: "Promise"):
        self.promises.append(promise)

    def unregister(self, promise: "Promise"):
        self.promises = [_promise for _promise in self.promises if _promise != promise]

    def wait(self):
        for promise in self.promises:
            if promise.status != Promise.Status.CREATED:
                continue
            promise.wait()


main_event_loop = EventLoop()


class Promise:
    class Status:
        CREATED = "created"
        STARTED = "started"
        FINISHED = "finished"

    class Resolution:
        result: Any = None
        error: Any = None

    task: Union[threading.Thread, multiprocessing.Process]
    resolution: "Promise.Resolution"
    event_loop: EventLoop
    status: "Promise.Status"
    _metadata: dict

    def __init__(
        self,
        execution,
        mode: Literal["threading", "multiprocessing"] = "threading",
        event_loop=main_event_loop,
    ) -> None:
        self.status = Promise.Status.CREATED
        self.event_loop = event_loop
        self.resolution = Promise.Resolution()
        self._metadata = {}
        self.event_loop.register(self)

        if mode == "threading":

            def wrapped():
                try:
                    result = execution()
                    if type(result) is Promise:
                        result = result.wait()
                    self.resolution.result = result
                except Exception as error:
                    self.resolution.error = error
                finally:
                    self.status = Promise.Status.FINISHED

            self.task = threading.Thread(target=wrapped)

        if mode == "multiprocessing":
            self._metadata["queue"] = multiprocessing.Queue()

            def wrapped(queue: multiprocessing.Queue):
                try:
                    result = execution()
                    if type(result) is Promise:
                        result = result.wait()
                    queue.put(result)
                    queue.put(None)
                except Exception as error:
                    queue.put(None)
                    queue.put(error)
                finally:
                    queue.put(Promise.Status.FINISHED)

            self.task = multiprocessing.Process(
                target=wrapped, args=[self._metadata["queue"]]
            )

    def start(self):
        if self.status != Promise.Status.CREATED:
            return self

        self.status = Promise.Status.STARTED
        self.task.start()

        return self

    def wait(self):
        if self.status == Promise.Status.FINISHED:
            return self.resolution

        if self.status == Promise.Status.CREATED:
            raise Exception("Promise has not been started yet")

        self.task.join()

        if self._metadata.get("queue"):
            self.resolution.result = self._metadata["queue"].get()
            self.resolution.error = self._metadata["queue"].get()
            self.status = self._metadata["queue"].get()

        self.event_loop.unregister(self)

        return self.resolution

    @staticmethod
    def all(promises):
        resolutions = []
        for promise in promises:
            resolutions.append(promise.wait())

        return resolutions
