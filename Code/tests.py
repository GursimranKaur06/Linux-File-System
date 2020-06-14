from typing import List
import os
import pytest
import random
import stat
import string
import time


NBR_SLAVES = 1
MASTER_MOUNT_POINT = '../master'
SLAVE_MOUNT_POINT_PREFIX = '../slave_'
BACKING_STORE_DIR = './backing_test'
MASTER_BACKING_STORE = os.path.join(BACKING_STORE_DIR, 'master')
SLAVE_BACKING_STORE_PREFIX =os.path.join(BACKING_STORE_DIR, 'slave_')


def random_string(length: int) -> str:
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


def get_all_slaves() -> List[str]:
    return [f'{SLAVE_MOUNT_POINT_PREFIX}{i}' for i in range(NBR_SLAVES)]


def get_all_slave_backing_stores() -> List[str]:
    return [f'{SLAVE_BACKING_STORE_PREFIX}{i}' for i in range(NBR_SLAVES)]


def empty_dir(path: str):
    assert os.path.isdir(path)

    for f in os.scandir(path):
        if os.path.isdir(f):
            os.rmdir(f)
        else:
            os.unlink(f)


@pytest.fixture(scope='session', autouse=True)
def test_setup():
    """
    Empty the directories before and after tests.
    """
    empty_dir(MASTER_BACKING_STORE)
    for slave_backing_store in get_all_slave_backing_stores():
        empty_dir(slave_backing_store)

    yield


def test_success_1():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_success_1')
    os.mkdir(master_file_path)

    time.sleep(1)

    for backing_store in get_all_slave_backing_stores():
        slave_file_path = os.path.join(backing_store, 'test_success_1')
        assert os.path.exists(slave_file_path)
        assert os.path.isdir(slave_file_path)


def test_success_2():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_success_2')
    f = open(master_file_path, 'a')
    f.close()

    time.sleep(1)

    for backing_store in get_all_slave_backing_stores():
        slave_file_path = os.path.join(backing_store, 'test_success_2')
        assert os.path.exists(slave_file_path)
        assert os.path.isfile(slave_file_path)


def test_success_3():
    file_path = os.path.join(MASTER_MOUNT_POINT, 'test_success_3')
    f = open(file_path, 'a')
    # Closing to simulate updating
    f.close()

    random_str = random_string(200)

    f = open(file_path, 'a')
    f.write(random_str)
    f.close()

    time.sleep(1)

    for backing_store in get_all_slave_backing_stores():
        slave_file_path = os.path.join(backing_store, 'test_success_3')
        assert os.path.exists(slave_file_path)
        assert os.path.isfile(slave_file_path)

        with open(slave_file_path, 'r') as f:
            assert f.read() == random_str


def test_success_4():
    master_file_path_old = os.path.join(MASTER_MOUNT_POINT, 'test_success_4')
    os.mkdir(master_file_path_old)

    master_file_path_new = os.path.join(MASTER_MOUNT_POINT, 'test_success_4_new')
    os.rename(master_file_path_old, master_file_path_new)

    time.sleep(1)

    for backing_stores in get_all_slave_backing_stores():
        slave_file_path_old = os.path.join(backing_stores, 'test_success_4')
        slave_file_path_new = os.path.join(backing_stores, 'test_success_4_new')
        assert not os.path.exists(slave_file_path_old)
        assert os.path.exists(slave_file_path_new)
        assert os.path.isdir(slave_file_path_new)


def test_success_5():
    master_file_path_old = os.path.join(MASTER_MOUNT_POINT, 'test_success_5')
    f = open(master_file_path_old, 'a')
    f.close()

    master_file_path_new = os.path.join(MASTER_MOUNT_POINT, 'test_success_5_new')
    os.rename(master_file_path_old, master_file_path_new)

    time.sleep(1)

    for backing_stores in get_all_slave_backing_stores():
        slave_file_path_old = os.path.join(backing_stores, 'test_success_5')
        slave_file_path_new = os.path.join(backing_stores, 'test_success_5_new')
        assert not os.path.exists(slave_file_path_old)
        assert os.path.exists(slave_file_path_new)
        assert os.path.isfile(slave_file_path_new)


def test_success_6():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_success_6')
    os.mkdir(master_file_path)

    time.sleep(1)

    os.rmdir(master_file_path)

    time.sleep(1)

    for backing_store in get_all_slave_backing_stores():
        slave_file_path = os.path.join(backing_store, 'test_success_6')
        assert not os.path.exists(slave_file_path)


def test_success_7():
    file_path = os.path.join(MASTER_MOUNT_POINT, 'test_success_7')
    f = open(file_path, 'a')
    # Closing to simulate updating
    f.close()

    time.sleep(1)

    os.unlink(file_path)

    time.sleep(1)

    for backing_store in get_all_slave_backing_stores():
        slave_file_path = os.path.join(backing_store, 'test_success_7')
        assert not os.path.exists(slave_file_path)


def test_success_8():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_success_8')
    os.mkdir(master_file_path)
    os.chmod(master_file_path, 0o777)

    time.sleep(1)

    for backing_store in get_all_slave_backing_stores():
        slave_file_path = os.path.join(backing_store, 'test_success_8')
        assert os.stat(slave_file_path).st_mode & stat.S_IRWXU == stat.S_IRWXU


def test_success_9():
    file_path = os.path.join(MASTER_MOUNT_POINT, 'test_success_9')
    f = open(file_path, 'a')
    # Closing to simulate updating
    f.close()
    os.chmod(file_path, 0o777)

    time.sleep(1)

    for backing_store in get_all_slave_backing_stores():
        slave_file_path = os.path.join(backing_store, 'test_success_9')
        assert os.stat(slave_file_path).st_mode & stat.S_IRWXU == stat.S_IRWXU


def test_failing_1():
    for mount_point in get_all_slaves():
        slave_file_path = os.path.join(mount_point, 'test_failing_1')
        with pytest.raises(PermissionError):
            os.mkdir(slave_file_path)
        assert not os.path.exists(slave_file_path)


def test_failing_2():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_failing_2')
    os.mkdir(master_file_path)

    time.sleep(1)

    for mount_point in get_all_slaves():
        slave_file_path = os.path.join(mount_point, 'test_failing_2')
        with pytest.raises(PermissionError):
            os.rmdir(slave_file_path)
        assert os.path.exists(slave_file_path)


def test_failing_3():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_failing_3')
    os.mkdir(master_file_path)

    time.sleep(1)

    for mount_point in get_all_slaves():
        slave_file_path_old = os.path.join(mount_point, 'test_failing_3')
        slave_file_path_new = os.path.join(mount_point, 'test_failing_3_new')
        with pytest.raises(PermissionError):
            os.rename(slave_file_path_old, slave_file_path_new)
        assert os.path.exists(slave_file_path_old)
        assert not os.path.exists(slave_file_path_new)


def test_failing_4():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_failing_4')
    os.mkdir(master_file_path)

    time.sleep(1)

    for mount_point in get_all_slaves():
        slave_file_path = os.path.join(mount_point, 'test_failing_4')
        existing_mode = os.stat(slave_file_path).st_mode
        with pytest.raises(PermissionError):
            os.chmod(slave_file_path, 0o777)
        assert os.stat(slave_file_path).st_mode == existing_mode


def test_failing_5():
    for mount_point in get_all_slaves():
        slave_file_path = os.path.join(mount_point, 'test_failing_5')
        with pytest.raises(PermissionError):
            open(slave_file_path, 'a')
        assert not os.path.exists(slave_file_path)


def test_failing_6():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_failing_6')
    open(master_file_path, 'a').close()

    time.sleep(1)

    for mount_point in get_all_slaves():
        slave_file_path = os.path.join(mount_point, 'test_failing_6')
        with pytest.raises(PermissionError):
            os.unlink(slave_file_path)
        assert os.path.exists(slave_file_path)


def test_failing_7():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_failing_7')
    open(master_file_path, 'a').close()

    time.sleep(1)

    for mount_point in get_all_slaves():
        slave_file_path_old = os.path.join(mount_point, 'test_failing_7')
        slave_file_path_new = os.path.join(mount_point, 'test_failing_7_new')
        with pytest.raises(PermissionError):
            os.rename(slave_file_path_old, slave_file_path_new)
        assert os.path.exists(slave_file_path_old)
        assert not os.path.exists(slave_file_path_new)


def test_failing_8():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_failing_8')
    open(master_file_path, 'a').close()

    time.sleep(1)

    for mount_point in get_all_slaves():
        slave_file_path = os.path.join(mount_point, 'test_failing_8')
        existing_mode = os.stat(slave_file_path).st_mode
        with pytest.raises(PermissionError):
            os.chmod(slave_file_path, 0o777)
        assert os.stat(slave_file_path).st_mode == existing_mode


def test_failing_9():
    master_file_path = os.path.join(MASTER_MOUNT_POINT, 'test_failing_9')
    open(master_file_path, 'a').close()

    time.sleep(1)

    for mount_point in get_all_slaves():
        slave_file_path = os.path.join(mount_point, 'test_failing_9')
        with pytest.raises(PermissionError):
            with open(slave_file_path, 'w') as f:
                f.write('test')
        assert os.stat(slave_file_path).st_size == 0

