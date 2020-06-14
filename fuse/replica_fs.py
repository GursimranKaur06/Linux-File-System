from fuse import FUSE
from queue import Queue
from typing import List
import click
import logging
import os
import pathlib
import threading

from fs.ReplicaFSMaster import ReplicaFSMaster
from fs.ReplicaFSSlave import ReplicaFSSlave
from fs.config import ReplicaFSConfig
import constants


def get_slave_mount_points(mount_point_path: str, n: int):
    return [f'{mount_point_path}{i}' for i in range(n)]


def get_slave_backing_stores(backing_store_path: str, n: int):
    return [f'{backing_store_path}{i}' for i in range(n)]


def create_dirs(config: ReplicaFSConfig):
    pathlib.Path(config.master_mount_point).mkdir(parents=True, exist_ok=True)
    pathlib.Path(config.master_backing).mkdir(parents=True, exist_ok=True)

    for mp in config.slave_mount_points:
        pathlib.Path(mp).mkdir(parents=True, exist_ok=True)

    for mp in config.slave_backings:
        pathlib.Path(mp).mkdir(parents=True, exist_ok=True)


def create_master_fuse(
        config: ReplicaFSConfig,
        foreground: bool,
        queues: List[Queue],
):
    fs = ReplicaFSMaster(config, queues=queues, nbr_slaves=config.nbr_slaves)
    print(f'Master FUSE initializing foreground={foreground}', flush=True)
    FUSE(fs, config.master_mount_point, nothreads=True, foreground=foreground)
    print(f'Master FUSE initialized foreground={foreground}', flush=True)


def create_slave_fuse(
        config: ReplicaFSConfig,
        n: int,
        foreground: bool,
        queues: List[Queue],
        logger: logging.Logger,
):
    fs = ReplicaFSSlave(config, queue=queues[n], slave_n=n, logger=logger)
    print(f'Slave FUSE initializing foreground={foreground}', flush=True)
    FUSE(
        fs,
        config.slave_mount_points[n],
        nothreads=True,
        foreground=foreground,
    )
    print(f'Slave FUSE initialized foreground={foreground}', flush=True)


def create_logger() -> logging.Logger:
    logger = logging.getLogger('replica_fs')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('replicafs.log')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


@click.command()
@click.option(
    '--foreground',
    '-f',
    default=False,
    is_flag=True,
    help='Run in foreground'
)
@click.option(
    '--nbr-slaves',
    '-n',
    default=1,
    help='Number of slaves'
)
@click.argument('backing_store')
def init_replica_fs(foreground: bool, backing_store: str, nbr_slaves: int):
    config = ReplicaFSConfig(
        master_mount_point=constants.MASTER_MOUNT_PATH,
        master_backing=os.path.join(
            backing_store,
            constants.MASTER_BACKING_STORAGE_NAME,
        ),
        slave_mount_points=get_slave_mount_points(
            constants.SLAVE_MOUNT_PATH_PREFIX, n=nbr_slaves,
        ),
        slave_backings=get_slave_backing_stores(
            os.path.join(
                backing_store,
                constants.SLAVE_BACKING_STORAGE_NAME_PREFIX
            ),
            n=nbr_slaves,
        ),
        nbr_slaves=nbr_slaves,
    )
    create_dirs(config)

    queues: List[Queue] = []

    for i in range(nbr_slaves):
        queue = Queue()
        queues.append(queue)

    logger = create_logger()

    master_thread = threading.Thread(
        target=create_master_fuse,
        args=(config, foreground, queues),
        daemon=True,
    )

    slave_threads = [
        threading.Thread(
            target=create_slave_fuse,
            daemon=True,
            args=(config, i, foreground, queues, logger),
        ) for i in range(nbr_slaves)
    ]

    master_thread.start()
    for slave_thread in slave_threads:
        slave_thread.start()

    master_thread.join()
    for slave_thread in slave_threads:
        slave_thread.join()


if __name__ == '__main__':
    init_replica_fs()
