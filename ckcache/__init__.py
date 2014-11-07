    
import os
import logging
import sys

class CacheError(Exception):
    ''''''

class ConfigurationError(CacheError):
    '''Error in the configuration files'''

def new_cache(config, root_dir='no_root_dir', run_config=None):
        """Return a new :class:`FsCache` built on the configured cache directory
        """

        if isinstance(config, basestring):
            config = parse_cache_string(config, root_dir)

        if 'size' in config:
            from filesystem import FsLimitedCache
            fsclass = FsLimitedCache
        elif 'url' in config:
            if 'rest' in config['options']:
                from remote import RestReadCache
                fsclass = RestReadCache
            else:
                from remote import HttpCache
                fsclass = HttpCache
        elif 'account' in config:


            if isinstance(config['account'], basestring):
                if not run_config:
                    raise ConfigurationError("Config has an account, but run_config was not specified to resolve the account")

                config['account'] = run_config.account(config['account'])


            if config['account']['service'] == 's3':
                from s3 import S3Cache
                fsclass = S3Cache
            elif config['account']['service'] == 'gcs':
                from gcs import GcsCache
                fsclass = GcsCache
            else:
                raise ConfigurationError("Unknown account service: {}".format(config['account']['service']))
                                                                 
        elif 'dir' in config:
            from filesystem import FsCache
            fsclass = FsCache
        else:
            
            raise ConfigurationError("Can't determine cache type: {} ".format(config))


        # Re-write account to get login credentials, if the run_config is available
        if 'account' in config and run_config :
            try:
                config['account'] = run_config.account(config['account'])
            except TypeError: # config['account'] is already a dict
                pass

        if 'options' in config and 'compress' in config['options'] :
            # Need to clone the config because we don't want to propagate the changes
            try:
                cc = config.to_dict()
            except AttributeError:
                cc = dict(config.items())

            cc['options'] = [ i for i in config['options'] if i !=  'compress']
            from filesystem import FsCompressionCache
            return FsCompressionCache(upstream=cc)
        else:
            return  fsclass(**dict(config))


def parse_cache_string(remote, root_dir='no_root_dir'):
    import urlparse

    remote = remote.format(root=root_dir)

    parts = urlparse.urlparse(remote)
    config = {}

    config['type'] = scheme = parts.scheme if parts.scheme else 'file'

    config['options'] = []


    if scheme == 'file' or not bool(scheme) :
        config['dir'] = parts.path

    # s3://bucket/prefix
    # Account name handle is the same as the bucket
    elif scheme == 's3':

        config['bucket'] = parts.netloc
        config['prefix'] = parts.path.strip('/')

        config['account'] = config['bucket']

    # file://path
    elif scheme == 'http':
        t = list(parts)
        t[5] = None # clear out the fragment
        config['url'] = urlparse.urlunparse(t)

    elif scheme == 'rest':
        config['url'] = "http:{}".format(parts.netloc)
        config['options'] +=['rest']

    config['options'] += parts.fragment.split(';')

    return config

class Cache(object):
    
    config = None
    upstream = None
    readonly = False
    usreadonly = False
    base_priority = 100 # Priority for this class of cache.
    _priority = 0
    _prior_upstreams = None
    
    def __init__(self,  upstream=None ,**kwargs):
        self.upstream = upstream
        self.args = kwargs
        self.readonly = False
        self.usreadonly = False   

        self._prior_upstreams = []

        if upstream:
            if isinstance(upstream, Cache):
                self.upstream = upstream
            else:
                self.upstream = new_cache(upstream)

    def clone(self):
        return Cache(upstream=self.upstream, **self.args)

    def subcache(self,path):
        """Clone this case, and extend the prefix"""

        cache = self.clone()
        cache.prefix = os.path.join(cache.prefix if cache.prefix else '', path)

        return cache

    @property
    def repo_id(self):
        raise NotImplementedError()

    def path(self, rel_path, **kwargs):
        if self.upstream:
            return self.upstream.path(rel_path, **kwargs)
        
        return None
    
    def get(self, rel_path, cb=None):
        if self.upstream:
            return self.upstream.get(rel_path, cb)
        
        return None

    def get_stream(self, rel_path, cb=None):

        if self.upstream:
            return self.upstream.get_stream(rel_path, cb)

        return None

    def has(self, rel_path, md5=None, propagate=True):
        if self.upstream:
            return self.upstream.has(rel_path, md5=md5, propagate=propagate)
        
        return None

    def put(self, source, rel_path, metadata=None):
        if self.upstream:
            return self.upstream.put(self, source, rel_path, metadata=metadata)
        
        return None

    def put_stream(self,rel_path, metadata=None, cb=None):
        if self.upstream:
            return self.upstream.put_stream(self,rel_path, metadata=metadata, cb=cb)
        
        return None

    def put_metadata(self,rel_path, metadata):
        import json
        
        if rel_path.startswith('meta'):
            return
        
        if metadata:
            strm = self.put_stream(os.path.join('meta',rel_path))
            json.dump(metadata, strm)
            strm.close()
    
    def metadata(self,rel_path):
        import json

        if rel_path.startswith('meta'):
            return None

        strm = self.get_stream(os.path.join('meta',rel_path))
        
        if strm:
            try:
                s = strm.read()
                if not s:
                    return {}
                return json.loads(s)
            except ValueError as e:
                raise ValueError("Failed to decode json for key '{}',  {}. {}".format(rel_path, self.path(os.path.join('meta',rel_path)), strm))
        else:
            return {}
        
    def remove(self,rel_path, propagate = False):
        if self.upstream:
            return self.upstream.remove(self,rel_path, propagate = propagate)
        
        return None

    def find(self,query):

        if self.upstream:
            return self.upstream.find(query)

        return None

    def clean(self):

        if self.upstream:
            return self.upstream.clean()
        
        return None

    def list(self, path=None,with_metadata=False):
        if self.upstream:
            return self.upstream.list(path, with_metadata=with_metadata)
        
        return None

    def store_list(self, cb=None):
        """List the cache and store it as metadata. This allows for getting the list from HTTP caches
        and other types where it is not possible to traverse the tree"""

        from StringIO import StringIO
        import json

        d = {}

        for k, v in self.list().items():

            if 'caches' in v:
                del v['caches']

            d[k] = v

        strio = StringIO(json.dumps(d))

        sink = self.put_stream('meta/_list.json')

        copy_file_or_flo(strio, sink, cb=cb)
        sink.close()

    def attach(self,upstream):
        """Attach an upstream to the last upstream. Can be removed with detach"""

        if upstream == self.last_upstream():
            raise Exception("Can't attach a cache to itself")

        self._prior_upstreams.append(self.last_upstream())

        self.last_upstream().upstream = upstream

    def detach(self):
        """Remove the last upstream from the upstream chain"""


        prior_last = self._prior_upstreams.pop()
        prior_last.upstream = None


    def set_priority(self, i):
        self._priority = self.base_priority + i

    @property
    def priority(self):
        return self._priority


    def get_upstream(self, type_):
        '''Return self, or an upstream, that has the given class type.
        This is typically used to find upstream s that impoement the RemoteInterface
        '''

        if isinstance(self, type_):
            return self
        elif self.upstream and isinstance(self.upstream, type_):
            return self.upstream
        elif self.upstream:
            return self.upstream.get_upstream(type_)
        else:
            return None

    def last_upstream(self):
        us = self

        while us.upstream:
            us = us.upstream

        return us

    def __repr__(self):
        return "{}".format(type(self))


class NullCache(Cache):
    """A Cache that acts as if it contains nothing"""

    def repo_id(self):
        raise NotImplementedError()

    def path(self, rel_path, propatate=True, **kwargs):
        return False

    def get(self, rel_path, cb=None):
        return None

    def get_stream(self, rel_path, cb=None):
        return None

    def has(self, rel_path, md5=None, propagate=True):
        return False

    def put(self, source, rel_path, metadata=None):
        return None

    def put_stream(self, rel_path, metadata=None):
        return None

    def find(self, query):
        return None

    def list(self, path=None, with_metadata=False, include_partitions=False):
        return None

    def remove(self, rel_path, propagate=False):
        return None

    def clean(self):
        return None

    def get_upstream(self, type_):
        return None

    def last_upstream(self):
        return None

    def attach(self, upstream):
        pass

    def detach(self):
        pass

# This probably duplicates the functionality of Cache ...
class PassthroughCache(Cache):
    """Pass through operations to the Upstream. Meant to be subclassed for useful behavior """

    upstream = None

    def __init__(self, upstream):
        self.upstream = upstream

    def repo_id(self):
        return self.upstream.repo_id()

    def path(self, rel_path, propatate = True, **kwargs):
        return self.upstream.path(rel_path, propatate, **kwargs)

    def get(self, rel_path, cb=None):
        return self.upstream.get(rel_path, cb)

    def get_stream(self, rel_path, cb=None):
        return self.upstream.get_stream(rel_path, cb)

    def has(self, rel_path, md5=None, propagate=True):
        return self.upstream.has(rel_path, md5, propagate)

    def put(self, source, rel_path, metadata=None):
        return self.upstream.put(source, rel_path, metadata)

    def put_stream(self,rel_path, metadata=None):
        return self.upstream.put_stream(rel_path, metadata)

    def find(self,query):
        return self.upstream.find(query)

    def list(self, path=None, with_metadata=False, include_partitions=False):
        return self.upstream.list(path, with_metadata, include_partitions)

    def remove(self, rel_path, propagate=False):
        return self.upstream.remove(rel_path, propagate)

    def clean(self):
        return self.upstream.clean()

    def get_upstream(self, type_):
        return self.upstream.get_upstream(type_)

    def last_upstream(self):
        return self.upstream.last_upstream()

    def attach(self, upstream):
        return self.upstream.attach(upstream)

    def detach(self):
        return self.upstream.detach()


class MetadataFlo(object):
    '''A File like object wrapper that has a slot for storing metadata'''


    def __init__(self,o, metadata=None):
        self.o = o

        if metadata:
            self.meta = metadata
        else:
            self.meta = {}

    def seek(self,offset,whence=0):
        return self.o.seek(offset,whence)

    def tell(self):
        return self.o.tell()

    def read(self,size=None):
        if size:
            return self.o.read(size)
        else:
            return self.o.read()

    def readline(self,size=None):
        if size:
            return self.o.readline(size)
        else:
            return self.o.readline()

    def readlines(self,size=None):
        if size:
            return self.o.readlines(size)
        else:
            return self.o.readlines()

    def write(self, d):
        self.o.write(d)

    def writelines(self, d):
        self.o.writelines(d)

    def flush(self):
        return self.o.flush()

    def close(self):
        return self.o.close()

    @property
    def closed(self):
        return self.o.closed

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        if type_:
            return False

        self.close()


def copy_file_or_flo(input_, output, buffer_size=64*1024, cb=None):
    """ Copy a file name or file-like-object to another
    file name or file-like object"""
    import shutil

    input_opened = False
    output_opened = False

    try:
        if isinstance(input_, basestring):

            if not os.path.isdir(os.path.dirname(input_)):
                os.makedirs(os.path.dirname(input_))

            input_ = open(input_,'r')
            input_opened = True

        if isinstance(output, basestring):

            if not os.path.isdir(os.path.dirname(output)):
                os.makedirs(os.path.dirname(output))

            output = open(output,'wb')
            output_opened = True

        #shutil.copyfileobj(input_,  output, buffer_size)

        def copyfileobj(fsrc, fdst, length=buffer_size):
            cumulative = 0
            while 1:
                buf = fsrc.read(length)
                if not buf:
                    break
                fdst.write(buf)
                if cb:
                    cumulative += len(buf)
                    cb(len(buf), cumulative)

        copyfileobj(input_, output)

    finally:
        if input_opened:
            input_.close()

        if output_opened:
            output.close()


def get_logger(name, file_name = None, stream = None, template=None, propagate = False):
    """Get a logger by name

    if file_name is specified, and the dirname() of the file_name exists, it will
    write to that file. If the dirname dies not exist, it will silently ignre it. """

    logger = logging.getLogger(name)

    if propagate is not None:
        logger.propagate = propagate


    for handler in logger.handlers:
        logger.removeHandler(handler)

    if not template:
        template = "%(name)s %(process)s %(levelname)s %(message)s"

    formatter = logging.Formatter(template)

    if not file_name and not stream:
        stream = sys.stdout

    handlers = []

    if stream is not None:

        handlers.append(logging.StreamHandler(stream=stream))

    if file_name is not None:

        if os.path.isdir(os.path.dirname(file_name)):
            handlers.append(logging.FileHandler(file_name))
        else:
            print("ERROR: Can't open log file {}".format(file_name))


    for ch in handlers:
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    logger.setLevel(logging.INFO)


    return logger

