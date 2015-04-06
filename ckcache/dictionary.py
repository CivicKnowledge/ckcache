"""

Adapts an upstream to have a dict interface, storing objects as JSON

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import Cache, PassthroughCache
from collections import MutableMapping
import json

class DictCache(PassthroughCache, MutableMapping):

    def __init__(self, upstream,**kwargs):
        super(DictCache, self).__init__(upstream)


    def make_key(self, key):
        # Turn tuples in to heirarchical keys
        if isinstance(key, tuple):
            return '/'.join(str(x) for x in key)
        else:
            return str(key)


    def __setitem__(self, key, value):
        from cStringIO import StringIO

        s = StringIO(json.dumps(value))

        key = self.make_key(key)

        self.upstream.put(s, key)

        s.close()

    def __getitem__(self, key):
        from cStringIO import StringIO

        key = self.make_key(key)

        if not self.upstream.has(key):
            raise KeyError(key)

        o = json.load(self.upstream.get_stream(key))

        return o

    def __delitem__(self, key):
        self.upstream.remove(self.make_key(key))

    def __len__(self):
        return len(self.upstream.list())

    def __iter__(self):
        return iter(self.upstream.list().keys())
