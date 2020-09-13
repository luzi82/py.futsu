from unittest import TestCase
import futsu.storage as storage
import tempfile
import os
import futsu.fs as fs
import time
import futsu.json
import json

class TestStorage(TestCase):

    def test_local_join(self):
        with tempfile.TemporaryDirectory() as tempdir:
            self.assertEqual(storage.join(tempdir,'GMJZWWTM'),os.path.join(tempdir,'GMJZWWTM'))

    def test_gcp_join(self):
        self.assertEqual(storage.join('gs://DHLQJSEX','PFMSXIDP'),'gs://DHLQJSEX/PFMSXIDP')

    def test_s3_join(self):
        self.assertEqual(storage.join('s3://PISXTKBK','EIQDPMWQ'),'s3://PISXTKBK/EIQDPMWQ')

    def test_http_join(self):
        self.assertEqual(storage.join('http://UHMNFEYK','XFGBYFFR'),'http://UHMNFEYK/XFGBYFFR')
        self.assertEqual(storage.join('https://UHMNFEYK','XFGBYFFR'),'https://UHMNFEYK/XFGBYFFR')


    def test_local_split(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path0 = storage.join(tempdir,'GMJZWWTM')
            self.assertEqual(storage.split(path0),(tempdir,'GMJZWWTM'))

    def test_gcp_split(self):
        self.assertEqual(storage.split('gs://DHLQJSEX/PFMSXIDP'),('gs://DHLQJSEX','PFMSXIDP'))

    def test_s3_split(self):
        self.assertEqual(storage.split('s3://PISXTKBK/EIQDPMWQ'),('s3://PISXTKBK','EIQDPMWQ'))

    def test_http_split(self):
        self.assertEqual(storage.split('http://UHMNFEYK/XFGBYFFR'),('http://UHMNFEYK','XFGBYFFR'))
        self.assertEqual(storage.split('https://UHMNFEYK/XFGBYFFR'),('https://UHMNFEYK','XFGBYFFR'))


    def test_local_basename(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path0 = storage.join(tempdir,'GMJZWWTM')
            self.assertEqual(storage.basename(path0),'GMJZWWTM')

    def test_gcp_basename(self):
        self.assertEqual(storage.basename('gs://DHLQJSEX/PFMSXIDP'),'PFMSXIDP')

    def test_s3_basename(self):
        self.assertEqual(storage.basename('s3://PISXTKBK/EIQDPMWQ'),'EIQDPMWQ')

    def test_http_basename(self):
        self.assertEqual(storage.basename('http://UHMNFEYK/XFGBYFFR'),'XFGBYFFR')
        self.assertEqual(storage.basename('https://UHMNFEYK/XFGBYFFR'),'XFGBYFFR')


    def test_local_dirname(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path0 = storage.join(tempdir,'GMJZWWTM')
            self.assertEqual(storage.dirname(path0),tempdir)

    def test_gcp_dirname(self):
        self.assertEqual(storage.dirname('gs://DHLQJSEX/PFMSXIDP'),'gs://DHLQJSEX')

    def test_s3_dirname(self):
        self.assertEqual(storage.dirname('s3://PISXTKBK/EIQDPMWQ'),'s3://PISXTKBK')

    def test_http_dirname(self):
        self.assertEqual(storage.dirname('http://UHMNFEYK/XFGBYFFR'),'http://UHMNFEYK')
        self.assertEqual(storage.dirname('https://UHMNFEYK/XFGBYFFR'),'https://UHMNFEYK')
        