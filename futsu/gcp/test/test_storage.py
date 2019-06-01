from unittest import TestCase
import futsu.gcp.storage as fstorage
import tempfile
import os

class TestStorage(TestCase):

    def test_is_gs_bucket_path(self):
        self.assertTrue(fstorage.is_gs_bucket_path('gs://bucket'))
        self.assertTrue(fstorage.is_gs_bucket_path('gs://bucket/'))

        self.assertFalse(fstorage.is_gs_bucket_path('gs://bucket//'))
        self.assertFalse(fstorage.is_gs_bucket_path('gs://bucket/asdf'))
        self.assertFalse(fstorage.is_gs_bucket_path('gs://bucket/asdf/'))
        self.assertFalse(fstorage.is_gs_bucket_path('gs://bucket/asdf/asdf'))

        self.assertFalse(fstorage.is_gs_bucket_path('s://bucket'))
        self.assertFalse(fstorage.is_gs_bucket_path('g://bucket'))
        self.assertFalse(fstorage.is_gs_bucket_path('gs//bucket'))
        self.assertFalse(fstorage.is_gs_bucket_path('gs:/bucket'))
        self.assertFalse(fstorage.is_gs_bucket_path('gs://'))
        self.assertFalse(fstorage.is_gs_bucket_path('gs:///'))
        self.assertFalse(fstorage.is_gs_bucket_path('gs:///asdf'))

    def test_is_gs_file_path(self):
        self.assertFalse(fstorage.is_gs_file_path('gs://bucket'))
        self.assertFalse(fstorage.is_gs_file_path('gs://bucket/'))

        self.assertTrue(fstorage.is_gs_file_path('gs://bucket//'))
        self.assertTrue(fstorage.is_gs_file_path('gs://bucket/asdf'))
        self.assertTrue(fstorage.is_gs_file_path('gs://bucket/asdf/'))
        self.assertTrue(fstorage.is_gs_file_path('gs://bucket/asdf/asdf'))

        self.assertFalse(fstorage.is_gs_file_path('s://bucket'))
        self.assertFalse(fstorage.is_gs_file_path('g://bucket'))
        self.assertFalse(fstorage.is_gs_file_path('gs//bucket'))
        self.assertFalse(fstorage.is_gs_file_path('gs:/bucket'))
        self.assertFalse(fstorage.is_gs_file_path('gs://'))
        self.assertFalse(fstorage.is_gs_file_path('gs:///'))
        self.assertFalse(fstorage.is_gs_file_path('gs:///asdf'))
