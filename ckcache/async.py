""" The Threaded cache returns immediately on writes, then sends to the upstream
in a thread.
"""

from . import Cache, PassthroughCache
from . import copy_file_or_flo

from Queue import Queue, Empty
from threading import Thread
import signal

upload_queue = Queue(2000)
keep_alive = True  # Don't need a mutex on a boolean.

orig_handler = signal.getsignal(signal.SIGINT)


def handler(signum, frame):
    keep_alive = False
    orig_handler(signum, frame)

signal.signal(signal.SIGINT, handler)

upload_thread = None


class UploaderThread(Thread):

    """A Thread job to write a long job"""

    def run(self):
        from ckcache import new_cache
        global keep_alive
        global upload_queue

        while(keep_alive and not upload_queue.empty()):

            try:
                (rel_path, cache_string, buffer) = upload_queue.get(False)

                print "Send ", rel_path, cache_string

                cache = new_cache(cache_string)
                with cache.put_stream(rel_path) as s:
                    copy_file_or_flo(buffer, s)

                upload_queue.task_done()

            except Empty:
                break


def submit_task(rel_path, cache_string, buffer):
    """Put an upload job on the queue, and start the thread if required"""
    global upload_queue
    global upload_thread
    upload_queue.put((rel_path, cache_string, buffer))

    if upload_thread is None or not upload_thread.is_alive():
        upload_thread = UploaderThread()
        upload_thread.start()


class ThreadedWriteCache(Cache):

    """ The Threaded cache returns immediately on writes, then sends to the upstream
    in a thread.
    """

    def __init__(self, upstream, **kwargs):
        super(ThreadedWriteCache, self).__init__(upstream)

    def clone(self):
        return ThreadedWriteCache(upstream=self.upstream, **self.args)

    @property
    def priority(self):
        return self.upstream.priority - 1  # give a slightly better priority

    def set_priority(self, i):
        self.upstream.set_priority(i)

    ##
    # Put
    ##

    def put(self, source, rel_path, metadata=None):

        sink = self.put_stream(self._rename(rel_path), metadata=metadata)

        sink.close()

        return self.path(self._rename(rel_path))

    def put_stream(self, rel_path, metadata=None, cb=None):
        from io import IOBase
        from cStringIO import StringIO

        if not metadata:
            metadata = {}

        class flo(IOBase):

            ''' '''

            def __init__(self, upstream, rel_path):

                self._upstream = upstream
                self._rel_path = rel_path
                self._buffer = StringIO()

            @property
            def rel_path(self):
                return self._rel_path

            def write(self, str_):
                self._buffer.write(str_)

            def close(self):

                submit_task(
                    rel_path, str(
                        self._upstream), StringIO(
                        self._buffer.getvalue()))

                self._buffer.close()

            def __enter__(self):
                return self

            def __exit__(self, type_, value, traceback):
                if type_:
                    return False

                self.close()

        self.put_metadata(rel_path, metadata)

        return flo(self.upstream, rel_path)
    ##
    # Get
    ##

    def get_stream(self, rel_path, cb=None):

        return self.upstream.get_stream(rel_path, cb=cb)

    def get(self, rel_path, cb=None):

        return self.upstream.get(rel_path, cb=cb)

    def find(self, query):
        '''Passes the query to the upstream, if it exists'''
        if not self.upstream:
            raise Exception("DelayedWriteCache must have an upstream")

        return self.upstream.find(query)

    ##
    # Delete
    ##

    def remove(self, rel_path, propagate=False):
        '''Delete the file from the cache, and from the upstream'''

        if not self.upstream:
            raise Exception("Must have an upstream")

        # Must always propagate, since this is really just a filter.
        self.upstream.remove(self._rename(rel_path), propagate)

    ##
    # Information
    ##

    def path(self, rel_path, **kwargs):

        kwargs['missing_ok'] = True

        return self.upstream.path(self._rename(rel_path), **kwargs)

    def md5(self, rel_path):
        '''Return the MD5 for the file. Since the file is compressed,
        this will rely on the metadata'''

        md = self.metadata(rel_path)

        return md['md5']

    def list(self, path=None, with_metadata=False, include_partitions=False):
        '''get a list of all of the files in the repository'''
        return self.upstream.list(
            path,
            with_metadata=with_metadata,
            include_partitions=include_partitions)

    def store_list(self, cb=None):
        """List the cache and store it as metadata. This allows for getting the list from HTTP caches
        and other types where it is not possible to traverse the tree"""
        return self.upstream.store_list(cb=cb)

    def has(self, rel_path, md5=None, propagate=True):

        # This odd structure is because the MD5 check won't work if it is computed on a uncompressed
        # file and checked on a compressed file. But it will work if the check is done on an s3
        # file, which stores the md5 as metadada

        r = self.upstream.has(
            self._rename(rel_path),
            md5=md5,
            propagate=propagate)

        if r:
            return True

        return self.upstream.has(
            self._rename(rel_path), md5=None, propagate=propagate)

    def metadata(self, rel_path):
        return self.upstream.metadata(self._rename(rel_path))

    @staticmethod
    def _rename(rel_path):
        return rel_path

    @property
    def repo_id(self):
        return self.upstream.repo_id + '#async'

    @property
    def cache_dir(self):
        return self.upstream.cache_dir

    def __repr__(self):

        us = str(self.upstream)

        if '#' in us:
            return us + ';async'
        else:
            return us + '#async'


class LogEntry(object):

    """A class for acessing LoggingCache Log entries and deleting them"""

    def __init__(self, cache, rel_path):

        import json

        self.rel_path = rel_path
        self.__dict__.update(json.load(cache.get_stream(rel_path)))

    def remove(self):
        from ckcache import new_cache
        cache = new_cache(self.cache)
        cache.remove(self.rel_path)


class LoggingCache(PassthroughCache):

    """The Logging cache writes a record of all of the files that bave been put in a '_log'
    directory in the upstream """

    def write_record(self, rel_path):
        import os
        import hashlib
        import json
        import time

        t = time.time()

        path = os.path.join(
            "_log", str(int(t * 1000)) + '_' + hashlib.md5(rel_path).hexdigest())

        with self.upstream.put_stream(path) as s:
            s.write(
                json.dumps(
                    dict(
                        path=rel_path,
                        cache=str(
                            self.upstream),
                        time=t)))

    def put(self, source, rel_path, metadata=None):
        self.write_record(rel_path)
        return self.upstream.put(source, rel_path, metadata)

    def put_stream(self, rel_path, metadata=None):
        self.write_record(rel_path)
        return self.upstream.put_stream(rel_path, metadata)

    def list_log(self):

        for e in sorted(self.upstream.list('_log').keys()):
            yield LogEntry(self.upstream, e)

    def __repr__(self):
        return str(self.upstream) + \
            (';' if '#' in str(self.upstream) else '#') + 'record'
