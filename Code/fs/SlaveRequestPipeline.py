import threading
import typing


class SlaveRequestPipeline:
    _slave_lock = threading.Lock()
    _master_lock = threading.Lock()
    result: 'typing.Any' = None

    def get(self) -> 'typing.Any':
        self._master_lock.acquire()
        result = self.result
        if self._slave_lock.locked():
            self._slave_lock.release()
        return result

    def provide(self, result: 'typing.Any'):
        self._slave_lock.acquire()
        self.result = result
        if self._master_lock.locked():
            self._master_lock.release()
