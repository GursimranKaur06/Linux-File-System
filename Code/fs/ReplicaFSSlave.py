from fuse import FuseOSError
from queue import Queue
import errno
import logging
import os
import threading
import time

from fs.config import ReplicaFSConfig
from .Base import BaseOperations
from . import SlaveOperationCommands


class ReplicaFSSlave(BaseOperations):
    _WRITE_FLAGS = [
        os.O_RDWR, os.O_WRONLY,
    ]

    fd_map = {}

    def __init__(self,
                 config: ReplicaFSConfig,
                 queue: Queue,
                 slave_n: int,
                 logger: logging.Logger,
                 ):
        backing_store = os.path.realpath(
            config.slave_backings[slave_n],
        )
        mount_point = os.path.realpath(
            config.slave_mount_points[slave_n],
        )

        super().__init__(mount_point, backing_store)

        self.slave_n = slave_n
        self.queue = queue
        self.logger = logger
        self._make_run_loop()

    def mkdir(self, path, mode):
        raise FuseOSError(errno.EPERM)

    def create(self, path, mode, fi=None):
        raise FuseOSError(errno.EPERM)

    def open(self, path, flags):
        for flag in self._WRITE_FLAGS:
            if flags & flag > 0:
                raise FuseOSError(errno.EPERM)

        return super().open(path, flags)

    def write(self, path, buf, offset, fh):
        raise FuseOSError(errno.EPERM)

    def truncate(self, path, length, fh=None):
        raise FuseOSError(errno.EPERM)

    def rename(self, old, new):
        raise FuseOSError(errno.EPERM)

    def rmdir(self, path):
        raise FuseOSError(errno.EPERM)

    def unlink(self, path):
        raise FuseOSError(errno.EPERM)

    def chmod(self, path, mode):
        raise FuseOSError(errno.EPERM)

    def _repl_mkdir(self, path, mode):
        super().mkdir(path, mode)

    def _repl_open(self, path, flags, fd):
        ret = super().open(path, flags)
        if ret < 0:
            return ret

        self.fd_map[fd] = ret
        return ret

    def _repl_create(self, path, mode, fd, fi=None):
        ret = super().create(path, mode)
        if ret < 0:
            return ret

        self.fd_map[fd] = ret
        return super().create(path, mode, fi)

    def _repl_write(self, path, buf, offset, fd):
        return super().write(path, buf, offset, None if fd is None else self.fd_map[fd])

    def _repl_truncate(self, path, length, fh=None):
        super().truncate(path, length, None if fh is None else self.fd_map[fh])

    def _repl_release(self, path, fh):
        ret = super().release(path, None if fh is None else self.fd_map[fh])
        if not ret:
            return ret
        del self.fd_map[fh]
        return ret

    def _repl_rename(self, old, new):
        super().rename(old, new)

    def _repl_rmdir(self, path):
        return super().rmdir(path)

    def _repl_unlink(self, path):
        return super().unlink(path)

    def _repl_chmod(self, path, mode):
        return super().chmod(path, mode)

    def _distrib_read(self, path, length, offset, fh, pipeline):
        self.logger.debug(f'[Slave {self.slave_n}] Reading from {path}')
        data = super().read(path, length, offset, None if fh is None else self.fd_map[fh])
        pipeline.provide(data)

    def _execute_command(
            self,
            command: SlaveOperationCommands.Command,
    ):
        if type(command) == SlaveOperationCommands.Mkdir:
            command: SlaveOperationCommands.Mkdir
            self._repl_mkdir(command.path, command.mode)
        elif type(command) == SlaveOperationCommands.Create:
            command: SlaveOperationCommands.Create
            self._repl_create(command.path, command.mode, command.ret_fd, command.fi)
        elif type(command) == SlaveOperationCommands.Open:
            command: SlaveOperationCommands.Open
            self._repl_open(command.path, command.flags, command.ret_fd)
        elif type(command) == SlaveOperationCommands.Write:
            command: SlaveOperationCommands.Write
            self._repl_write(command.path, command.buf, command.offset, command.fh)
        elif type(command) == SlaveOperationCommands.Truncate:
            command: SlaveOperationCommands.Truncate
            self._repl_truncate(command.path, command.length, command.fh)
        elif type(command) == SlaveOperationCommands.Release:
            command: SlaveOperationCommands.Release
            self._repl_release(command.path, command.fh)
        elif type(command) == SlaveOperationCommands.Rename:
            command: SlaveOperationCommands.Rename
            self._repl_rename(command.old, command.new)
        elif type(command) == SlaveOperationCommands.Rmdir:
            command: SlaveOperationCommands.Rmdir
            self._repl_rmdir(command.path)
        elif type(command) == SlaveOperationCommands.Unlink:
            command: SlaveOperationCommands.Unlink
            self._repl_unlink(command.path)
        elif type(command) == SlaveOperationCommands.Chmod:
            command: SlaveOperationCommands.Chmod
            self._repl_chmod(command.path, command.mode)
        elif type(command) == SlaveOperationCommands.Read:
            command: SlaveOperationCommands.Read
            self._distrib_read(command.path, command.length, command.offset, command.fh, command.pipeline)
            return
        else:
            raise TypeError(f'Invalid command type {type(command)}')
        command.event.set()

        with command.condition:
            command.condition.notify()

    def _make_run_loop(self):
        self.run_loop_thread = threading.Thread(target=self._run_loop)
        self.run_loop_thread.start()

    def _run_loop(self):
        while True:
            if not self.queue.empty():
                command: SlaveOperationCommands = self.queue.get()
                self._execute_command(command)
                time.sleep(0.1)
