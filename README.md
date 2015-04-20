=======
ckcache
=======

Common interface to a variety of file systems and object stores. 

The caches have an object store interface, and are constructed on a URL, with a directory assumed if there is no schema to the URL. 

```
from ckcache import parse_cache_string, new_cache

config = parse_cache_string('/tmp/ckcache/test') # Create a config pointing to a directory
cache  = new_cache(config) # Create a FsCache on a directory
cache.put(source_file_name, '/cache/key') # Store contents of source_file_name at the cache key in the cache dir
```

Caches are composable, so they can be linked together in layers to add features or create multiple levels. So, if you have fast but limited local storage, and large but slow remote storage:

```
from ckcache.filesystem import FsCache, FsLimitedCache
slow_fs = FsCache(...)
fast_fs = FsLimitedCache(..., upstream=slow_fs, size=500)
```

With this configuration,  writes to fast_fs will be propagated to slow_fs, but if fast_fs gets larger than 500MB, files will be deleted on a LRU basis. If a missing file is requested from fast_fs, it will be copied up from slow_fs. 

Caches can also be composed for compression and logging. There are caches that work against dictionaries and HTTP, one that does asynchronous writes to slow upstream. 

So, you could compose a cache that uses an in-memory dictionary, which fallsback to a local disk, which in turn fallas back to Amazon S3. Since writes to S3 are slow, you could have the writes happen to the local disk syncronously, but the write to S3 happens async in a thread. 
