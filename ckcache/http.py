from . import Cache
import requests
from . import NotFoundError


class HttpCache(Cache):

    """Cache that uses HTTP protocol. Similar to a read-only version of the S3 cache, except that the
    list() method requires that a special file, meta/_list.json be put to the source"""

    def __init__(self, url, **kwargs):
        self._url = url

    def _rename(self, rel_path):
        '''Remove the .gz suffix that may have been added by a compression cache.
        In s3, compression is indicated by the content encoding.  '''
        import re
        rel_path = re.sub('\.gz$', '', rel_path)
        return rel_path

    def url(self, u, *args, **kwargs):

        if u.startswith("http"):
            return u

        if len(u) > 0 and u[0] == '/':
            u = u[1:]

        return self._url + '/' + u.format(*args, **kwargs)

    def handle_status(self, r):

        if r.status_code >= 300:

            try:
                o = r.json()
            except:
                o = None

            if isinstance(o, dict) and 'exception' in o:
                e = self.handle_exception(o)
                raise e

            if 400 <= r.status_code < 500:
                raise NotFoundError(
                    "Failed to find resource for URL: {}".format(
                        r.url))

            r.raise_for_status()

    def handle_return(self, r):

        if r.headers.get('content-type', False) == 'application/json':
            self.last_response = r
            return r.json()
        else:
            return r

    @property
    def repo_id(self):
        return self.url('')

    def path(self, rel_path, **kwargs):

        return self.url(self._rename(rel_path))

    def get(self, rel_path, cb=None):
        raise NotImplementedError()

    def get_stream(self, rel_path, cb=None):

        url = self._rename(rel_path)

        r = requests.get(self.url(url), verify=False, stream=True)

        self.handle_status(r)

        if r.headers.get('content-encoding', '') == 'gzip':
            from . import FileLikeFromIter
            # In  the requests library, iter_content will auto-decompress
            response = FileLikeFromIter(r.iter_content(chunk_size=128 * 1024))
        else:
            response = r.raw

        response.meta = {}

        for k, v in r.headers.items():
            if k.startswith('x-amz'):
                k = k.replace('x-amz', '')

            response.meta[k] = v

        response.meta.update(self.metadata(rel_path))

        return response

    def has(self, rel_path, md5=None, propagate=True):

        try:

            r = requests.head(self.url(self._rename(rel_path)), params={})
            self.handle_status(r)

            return self.handle_return(r)

        except NotFoundError:
            return False

    def put(self, source, rel_path, metadata=None):
        raise NotImplementedError()

    def put_stream(self, rel_path, metadata=None, cb=None):
        raise NotImplementedError()

    def put_metadata(self, rel_path, metadata):
        raise NotImplementedError()

    def metadata(self, rel_path):

        rel_path = self._rename(rel_path)

        import json
        import os

        if rel_path.startswith('meta'):
            return {}

        try:
            strm = self.get_stream(os.path.join('meta', rel_path))
        except NotFoundError:
            return {}

        if strm:
            try:
                s = strm.read()
                if not s:
                    return {}
                return json.loads(s)
            except ValueError as e:
                raise ValueError(
                    "Failed to decode json for key '{}',  {}. {}".format(
                        rel_path,
                        self.path(
                            os.path.join(
                                'meta',
                                rel_path)),
                        strm))
        else:
            return {}

    def remove(self, rel_path, propagate=False):
        raise NotImplementedError()

    def find(self, query):
        raise NotImplementedError()

    def clean(self):
        raise NotImplementedError()

    def list(self, path=None, with_metadata=True, include_partitions=True):

        r = requests.get(self.url('meta/_list.json'), params={})
        self.handle_status(r)

        l = self.handle_return(r)

        if not l:

            raise NotFoundError(
                "Did not find _list.json file at {}".format(
                    self.path('_list.json')))

        if not isinstance(l, dict):
            l = l.json()

        return {k: {} for k in l}

    def attach(self, upstream):
        raise NotImplementedError()

    def detach(self):
        raise NotImplementedError()

    def set_priority(self, i):
        self._priority = self.base_priority + i

    @property
    def priority(self):
        return self._priority

    def last_upstream(self):
        raise NotImplementedError()

    def __repr__(self):
        return "HttpCache: url={}".format(self.url(''))
