from queue import Queue
from typing import List
import copy
import os
import time
import threading

from fs.config import ReplicaFSConfig
from .Base import BaseOperations
from . import SlaveOperationCommands
from .SlaveRequestPipeline import SlaveRequestPipeline


SLAVE_TIMEOUT = 3600


class ReplicaFSMaster(BaseOperations):
    _read_repl = 0

    def __init__(self,
                 config: ReplicaFSConfig,
                 queues: List[Queue],
                 nbr_slaves: int,
                 ):
        backing_store = os.path.realpath(config.master_backing)
        mount_point = os.path.realpath(
            config.master_mount_point,
        )
        super().__init__(mount_point, backing_store)
        self.queues = queues
        self.nbr_slaves = nbr_slaves

    def mkdir(self, path, mode):
        super().mkdir(path, mode)

        command = SlaveOperationCommands.Mkdir(path, mode)
        self._notify_slaves(command)

    def create(self, path, mode, fi=None) -> int:
        ret = super().create(path, mode)
        if ret == -1:
            return ret

        command = SlaveOperationCommands.Create(path, mode, fi, ret_fd=ret)
        self._notify_slaves(command)
        return ret

    def open(self, path, flags):
        ret = super().open(path, flags)
        if ret == -1:
            return ret

        command = SlaveOperationCommands.Open(path, flags, ret_fd=ret)
        self._notify_slaves(command)
        return ret

    def write(self, path, buf, offset, fh):
        ret = super().write(path, buf, offset, fh)
        if ret == -1:
            return ret

        command = SlaveOperationCommands.Write(path, buf, offset, fh)
        self._notify_slaves(command)
        return ret

    def truncate(self, path, length, fh=None):
        super().truncate(path, length, fh)
        command = SlaveOperationCommands.Truncate(path, length, fh)
        self._notify_slaves(command)

    def release(self, path, fh):
        ret = super().release(path, fh)
        if ret != 0:
            return ret

        command = SlaveOperationCommands.Release(path, fh)
        self._notify_slaves(command)
        return ret

    def rename(self, old, new):
        super().rename(old, new)
        command = SlaveOperationCommands.Rename(old, new)
        self._notify_slaves(command)

    def rmdir(self, path):
        ret = super().rmdir(path)
        if ret:
            return ret
        command = SlaveOperationCommands.Rmdir(path)
        self._notify_slaves(command)
        return ret

    def unlink(self, path):
        ret = super().unlink(path)
        if ret:
            return ret
        command = SlaveOperationCommands.Unlink(path)
        self._notify_slaves(command)
        return ret

    def chmod(self, path, mode):
        ret = super().chmod(path, mode)
        if ret:
            return ret
        command = SlaveOperationCommands.Chmod(path, mode)
        self._notify_slaves(command)
        return ret

    def read(self, path, length, offset, fh):
        command = SlaveOperationCommands.Read(path, length, offset, fh)
        return self._request_from_next_slave(command)

    def _request_from_next_slave(self, command: SlaveOperationCommands.Command):
        n = self._read_repl

        self._read_repl += 1
        if self._read_repl >= self.nbr_slaves:
            self._read_repl = 0

        pipeline = SlaveRequestPipeline()
        command.pipeline = pipeline
        self.queues[n].put(command)

        result = None
        while result is None:
            result = pipeline.get()
            time.sleep(0.1)

        return result

    def _notify_slaves(self, command: SlaveOperationCommands.Command):
        condition = threading.Condition()

        with condition:
            events = []
            for n in range(self.nbr_slaves):
                command = copy.copy(command)
                command.condition = condition
                event = threading.Event()
                command.event = event
                events.append(event)
                command.slave_i = n
                self.queues[n].put(command)

            while any(map(lambda e: not e.is_set(), events)):
                condition.wait(0.1)

