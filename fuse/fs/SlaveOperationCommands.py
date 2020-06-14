from abc import ABC
from dataclasses import dataclass
import threading
import typing

from .SlaveRequestPipeline import SlaveRequestPipeline


class Command(ABC):
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Mkdir(Command):
    path: str
    mode: 'typing.Any'
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Create(Command):
    path: str
    mode: 'typing.Any'
    fi: 'typing.Any'
    ret_fd: 'typing.Any'
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Open(Command):
    path: str
    flags: 'typing.Any'
    ret_fd: 'typing.Any'
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Write(Command):
    path: str
    buf: 'typing.Any'
    offset: int
    fh: 'typing.Any'
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Truncate(Command):
    path: str
    length: int
    fh: 'typing.Any'
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None


@dataclass
class Rename(Command):
    old: str
    new: str
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Release(Command):
    path: str
    fh: 'typing.Any'
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Rmdir(Command):
    path: str
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Unlink(Command):
    path: str
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Chmod(Command):
    path: str
    mode: 'typing.Any'
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None


@dataclass
class Read(Command):
    path: str
    length: int
    offset: int
    fh: 'typing.Any'
    slave_i: int = None
    event: threading.Event = None
    condition: threading.Condition = None
    pipeline: SlaveRequestPipeline = None
