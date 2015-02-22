__author__ = 'eric'

import unittest
import os
from ckcache import new_cache

class BasicTests(unittest.TestCase):

    root = '/tmp/ckcache-test'

    def setUp(self):

        from shutil import rmtree

        try:
            rmtree(self.root)
        except OSError:
            pass

    def subpath(self,p):
        return os.path.join(self.root, p)

    def make_test_file(self):

        testfile = os.path.join(self.root, 'testfile')

        if not os.path.exists(self.root):
            os.makedirs(self.root)

        with open(testfile, 'w+') as f:
            for i in range(1024):
                f.write(str(i%10) * 1023)
                f.write('\n')

        return testfile

    def new_rand_file(self, path, size=1024):

        dir_ = os.path.dirname(path)

        if not os.path.isdir(dir_):
            os.makedirs(dir_)

        with open(path, 'w+') as f:
            for i in range(size):
                f.write(str(i % 10) * 1024)
                #f.write('\n')

        return path

    def test_basic(self):

        from ckcache.filesystem import FsCache, FsLimitedCache



        l1_repo_dir = os.path.join(self.root, 'repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(self.root, 'repo-l2')
        os.makedirs(l2_repo_dir)

        testfile = self.make_test_file()
        #
        # Basic operations on a cache with no upstream
        #
        l2 = FsCache(l2_repo_dir)

        p = l2.put(testfile, 'tf1')
        l2.put(testfile, 'tf2')
        g = l2.get('tf1')

        self.assertTrue(os.path.exists(p))
        self.assertTrue(os.path.exists(g))
        self.assertEqual(p, g)

        self.assertIsNone(l2.get('foobar'))

        l2.remove('tf1')

        self.assertIsNone(l2.get('tf1'))

        #
        # Now create the cache with an upstream, the first
        # cache we created

        l1 = FsLimitedCache(l1_repo_dir, upstream=l2, size=5)

        print l1
        print l2

        g = l1.get('tf2')
        self.assertTrue(g is not None)

        # Put to one and check in the other.

        l1.put(testfile, 'write-through')
        self.assertIsNotNone(l2.get('write-through'))

        l1.remove('write-through', propagate=True)
        self.assertIsNone(l2.get('write-through'))

        # Put a bunch of files in, and check that
        # l2 gets all of the files, but the size of l1 says constrained
        for i in range(0, 10):
            l1.put(testfile, 'many' + str(i))

        self.assertEquals(4194304, l1.size)


        # Check that the right files got deleted
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many1')))
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many5')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many6')))

        # Fetch a file that was displaced, to check that it gets loaded back
        # into the cache.
        p = l1.get('many1')
        p = l1.get('many2')
        self.assertTrue(p is not None)
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many1')))
        # Should have deleted many6
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many6')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many7')))

        #
        # Check that verification works
        #
        l1.verify()

        os.remove(os.path.join(l1.cache_dir, 'many8'))

        with self.assertRaises(Exception):
            l1.verify()

        l1.remove('many8')

        l1.verify()

        c = l1.database.cursor()
        c.execute("DELETE FROM  files WHERE path = ?", ('many9',))
        l1.database.commit()

        with self.assertRaises(Exception):
            l1.verify()

        l1.remove('many9')

        l1.verify()

    def test_basic_prefix(self):
        from ckcache.filesystem import FsCache, FsLimitedCache


        cache_dir = os.path.join(self.root, 'test_prefixes')

        prefix = 'prefix'

        cache1 = FsCache(cache_dir)

        cache2 = FsCache(cache_dir, prefix=prefix)

        tf = self.make_test_file()

        cache2.put(tf, 'tf')

        self.assertTrue(cache1.has('{}/{}'.format(prefix, 'tf')))

    def test_compression(self):

        from ckcache import new_cache, md5_for_file, copy_file_or_flo

        comp_cache = new_cache(os.path.join(self.root, 'compressioncache#compress'))

        print comp_cache

        test_file_name = 'test_file'

        fn = self.make_test_file()
        cf = comp_cache.put(fn, test_file_name)

        with open(cf) as stream:
            from ckcache.sgzip import GzipFile

            stream = GzipFile(stream)

            uncomp_cache = new_cache(os.path.join(self.root,'uncomp'))

            uncomp_stream = uncomp_cache.put_stream('decomp')

            copy_file_or_flo(stream, uncomp_stream)

        uncomp_stream.close()

        dcf = uncomp_cache.get('decomp')

        self.assertEquals(md5_for_file(fn), md5_for_file(dcf))

        with comp_cache.get_stream(test_file_name) as f:
            print len(f.read())

        with uncomp_cache.get_stream('decomp') as f:
            print len(f.read())


        os.remove(fn)

    def test_md5(self):

        from ckcache import new_cache, md5_for_file
        from ckcache.filesystem import make_metadata

        fn = self.make_test_file()

        md5 = md5_for_file(fn)

        cache = new_cache(os.path.join(self.root, 'fscache'))

        cache.put(fn, 'foo1')

        abs_path = cache.path('foo1')

        self.assertEquals(md5, cache.md5('foo1'))

        cache = new_cache(os.path.join(self.root, 'compressioncache'))

        cache.put(fn, 'foo2', metadata=make_metadata(fn))

        abs_path = cache.path('foo2')

        self.assertEquals(md5, cache.md5('foo2'))

        os.remove(fn)

    def test_attachment(self):
        from ckcache import new_cache


        testfile = self.new_rand_file(os.path.join(self.root, 'testfile'))

        fs1 = new_cache(dict(dir=os.path.join(self.root, 'fs1')))

        fs3 = new_cache(dict(dir=os.path.join(self.root, 'fs3')))

        fs3.put(testfile, 'tf')
        self.assertTrue(fs3.has('tf'))
        fs3.remove('tf')
        self.assertFalse(fs3.has('tf'))

        # Show that attachment works, and that deletes propagate.
        fs3.attach(fs1)
        fs3.put(testfile, 'tf')
        self.assertTrue(fs3.has('tf'))
        self.assertTrue(fs1.has('tf'))
        fs3.remove('tf', propagate=True)
        self.assertFalse(fs3.has('tf'))
        self.assertFalse(fs1.has('tf'))
        fs3.detach()

        # Show detachment works
        fs3.attach(fs1)
        fs3.put(testfile, 'tf')
        self.assertTrue(fs3.has('tf'))
        self.assertTrue(fs1.has('tf'))
        fs3.detach()
        fs3.remove('tf', propagate=True)
        self.assertFalse(fs3.has('tf'))
        self.assertTrue(fs1.has('tf'))

    def test_multi_cache(self):
        from ckcache import new_cache
        from ckcache.multi import MultiCache


        testfile = self.new_rand_file(os.path.join(self.root, 'testfile'), size=2)

        fs1 = new_cache(dict(dir=os.path.join(self.root, 'fs1')))
        fs2 = new_cache(dict(dir=os.path.join(self.root, 'fs2')))
        fs3 = new_cache(dict(dir=os.path.join(self.root, 'fs3')))

        caches = [fs1, fs2, fs3]

        for i, cache in enumerate(caches, 1):
            cache.put(testfile, 'fs' + str(i), metadata={'i': i})
            j = (i + 1) % len(caches)

            caches[j].put(testfile, 'fs' + str(i), metadata={'i': i})

        mc = MultiCache(caches)
        ls = mc.list()

        self.assertEqual(3, len(ls))
        self.assertIn('fs1', ls)
        self.assertIn('fs3', ls)

        self.assertIn('/tmp/ckcache-test/fs1', ls['fs1']['caches'])
        self.assertIn('/tmp/ckcache-test/fs3', ls['fs1']['caches'])

        mc2 = MultiCache([fs1, fs2])
        ls = mc2.list()
        self.assertEqual(3, len(ls))
        self.assertIn('fs1', ls)
        self.assertIn('fs3', ls)

        mc.put(testfile, 'mc1')
        ls = mc.list()
        self.assertEqual(4, len(ls))
        self.assertIn('mc1', ls)

        # Put should have gone to first cache
        mc2 = MultiCache([fs2, fs3])
        ls = mc2.list()
        self.assertEqual(3, len(ls))
        self.assertNotIn('mc1', ls)
        self.assertIn('fs1', ls)
        self.assertIn('fs3', ls)


    def test_alt_cache(self):

        from ckcache.multi import AltReadCache


        testfile = self.new_rand_file(os.path.join(self.root, 'testfile'), size=2)

        fs1 = new_cache(dict(dir=os.path.join(self.root, 'fs1')))
        fs2 = new_cache(dict(dir=os.path.join(self.root, 'fs2')))

        fs2.put(testfile, 'fs2', {'foo': 'bar'})

        self.assertFalse(fs1.has('fs2'))
        self.assertTrue(fs2.has('fs2'))

        arc = AltReadCache(fs1, fs2)
        self.assertTrue(arc.has('fs2'))
        self.assertEquals(['/tmp/ckcache-test/fs2'], arc.list()['fs2']['caches'])

        self.assertEquals('/tmp/ckcache-test/fs1/fs2', arc.get('fs2'))

        # Now the fs1 cache should have the file too
        self.assertTrue(fs1.has('fs2'))
        self.assertTrue(fs2.has('fs2'))

        self.assertIn('foo', fs1.metadata('fs2'))

    def test_accounts(self):

        config = dict(
            dir = self.subpath('ta_d1'),
            upstream = dict(
                dir = self.subpath('ta_d2'),
                upstream = 's3://devtest.sandiegodata.org/test'
            )
        )

        c =  new_cache(config)

        with c.put_stream('foobar',metadata=dict(foo='bar')) as f:
            f.write("bar baz")

    def test_http(self):

        http_cache = new_cache('http://devtest.sandiegodata.org/jdoc')

        self.assertTrue(bool(http_cache.has('library.json')))
        self.assertFalse(http_cache.has('missing'))

        with http_cache.get_stream('library.json') as f:
            import json
            d = json.load(f)

            self.assertIn('bundles', d.keys())

        file_cache = new_cache(os.path.join(self.root,'fc'))
        file_cache.upstream = http_cache

        self.assertTrue(bool(file_cache.has('library.json')))
        self.assertFalse(file_cache.has('missing'))

        with file_cache.get_stream('library.json') as f:
            import json

            d = json.load(f)

            self.assertIn('bundles', d.keys())


    def test_http_compressed(self):
        from ckcache import  copy_file_or_flo

        http_cache = new_cache('http://s3.sandiegodata.org/library')

        for x in  http_cache.list().keys():
            if 'example' in x:
                print x

        x  = http_cache.get_stream('example.com/random-0.0.2.db').__enter__()

        with http_cache.get_stream('example.com/random-0.0.2.db') as f:

            with open('/tmp/foo.db', 'wb') as fout:
                copy_file_or_flo(f, fout)


    def test_fallback(self):
        from ckcache import FallbackFlo
        import gzip

        o1 = new_cache(os.path.join(self.root,'o1'))
        o2 = new_cache(os.path.join(self.root, 'o2'))

        testfile = self.make_test_file()

        if not os.path.exists(o1._cache_dir):
            os.makedirs(o1._cache_dir)

        o1.put(testfile,'x')
        o2.put(testfile, 'x')

        with self.assertRaises(IOError):
            with gzip.GzipFile(fileobj=o1.get_stream('x')) as f:
                print f.read()

        ff = FallbackFlo(gzip.GzipFile(fileobj=o1.get_stream('x')), o2.get_stream('x'))

        self.assertEquals(1048576,  len(ff.read()))

        ff = FallbackFlo(gzip.GzipFile(fileobj=o1.get_stream('x')), gzip.GzipFile(fileobj=o2.get_stream('x')))

        with self.assertRaises(IOError):
            self.assertEquals(1048576, len(ff.read()))





if __name__ == '__main__':
    unittest.main()
