from fuse import Operations, FuseOSError
import errno
import os


class BaseOperations(Operations):
    def __init__(self, mount_point: str, backing_store: str):
        self.mount_point = mount_point
        self.backing_store = backing_store

    def _get_real_path(self, path: str) -> str:
        if path.startswith('/'):
            path = path[1:]

        path = os.path.join(self.backing_store, path)
        return path

    def access(self, path: str, mode):
        full_path = self._get_real_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._get_real_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._get_real_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._get_real_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        full_path = self._get_real_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._get_real_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.mount_point)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._get_real_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._get_real_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        os.mkdir(self._get_real_path(path), mode)

    def statfs(self, path):
        full_path = self._get_real_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
                                                         'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._get_real_path(path))

    def symlink(self, name, target):
        return os.symlink(name, self._get_real_path(target))

    def rename(self, old, new):
        return os.rename(self._get_real_path(old), self._get_real_path(new))

    def link(self, target, name):
        return os.link(self._get_real_path(target), self._get_real_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._get_real_path(path), times)

    def open(self, path, flags):
        return os.open(self._get_real_path(path), flags)

    def create(self, path, mode, fi=None):
        full_path = self._get_real_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._get_real_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)
