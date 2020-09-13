from unittest import TestCase
import futsu.pathdb
import tempfile

class TestPathDB(TestCase):

    def test_unique_unique(self):
        with tempfile.TemporaryDirectory() as tempdir:
            database_schema_data = {"TABLE_SCHEMA_DATA_LIST":[{
                'TABLE_NAME':'rs0',
                'COLUMN_NAME_LIST':['col0','col1'],
                'INDEX_DATA_LIST':[
                    {'COLUMN_NAME_LIST':['col0'],'UNIQUE':True},
                    {'COLUMN_NAME_LIST':['col1'],'UNIQUE':True},
                ],
            }]}
            database = futsu.pathdb.database(tempdir, database_schema_data)
            table = database.table('rs0')

            # check no row
            self.assertEqual(table.count(),0)
            self.assertEqual(len(table.all()),0)
            self.assertEqual(len(table.q(col0='r0c0').all()),0)
            self.assertEqual(len(table.q(col1='r0c1').all()),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)

            # check add row
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(table.q(col1=f'r{i}c1').all()),1)
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')
            self.assertEqual(table.count(),10)
            self.assertEqual(len(table.all()),10)

            # check del all
            table.rm()
            self.assertEqual(table.count(),0)
            self.assertEqual(len(table.all()),0)

            # check del row
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            table.q(col0='r0c0').rm()
            self.assertEqual(table.count(),9)
            self.assertEqual(len(table.all()),9)
            self.assertEqual(table.q(col0='r0c0').count(),0)
            self.assertEqual(table.q(col1='r0c1').count(),0)
            self.assertEqual(len(table.q(col0='r0c0')).all(),0)
            self.assertEqual(len(table.q(col1='r0c1')).all(),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)
            for i in range(1,10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(table.q(col1=f'r{i}c1').all()),1)
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')

            # check del row when no row
            table.rm()
            table.q(col0='r0c0').rm()

            # check add row other col1
            table.rm()
            table.q(col0='r0c0', col1='r0c1').add()
            with self.assertRaises(Exception):
                table.q(col0='r0c0', col1='rgegvkmh').add()

            # check add row 2nd times
            table.rm()
            table.q(col0='r0c0', col1='r0c1').add()
            with self.assertRaises(Exception):
                table.q(col0='r0c0', col1='r0c1').add()

    def test_unique_many(self):
        with tempfile.TemporaryDirectory() as tempdir:
            database_schema_data = {"TABLE_SCHEMA_DATA_LIST":[{
                'TABLE_NAME':'rs0',
                'COLUMN_NAME_LIST':['col0','col1'],
                'INDEX_DATA_LIST':[
                    {'COLUMN_NAME_LIST':['col0'],'UNIQUE':True},
                    {'COLUMN_NAME_LIST':['col1'],'UNIQUE':False},
                ],
            }]}
            database = futsu.pathdb.database(tempdir, database_schema_data)
            table = database.table('rs0')

            # check no row
            self.assertEqual(table.count(),0)
            self.assertEqual(len(table.all()),0)
            self.assertEqual(len(table.q(col0='r0c0').all()),0)
            self.assertEqual(len(table.q(col1='r0c1').all()),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)

            # check add row
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(table.q(col1=f'r{i}c1').all()),1)
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')
            self.assertEqual(table.count(),10)
            self.assertEqual(len(table.all()),10)

            # check del all
            table.rm()
            self.assertEqual(table.count(),0)
            self.assertEqual(len(table.all()),0)

            # check del row
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            table.q(col0='r0c0').rm()
            self.assertEqual(table.count(),9)
            self.assertEqual(len(table.all()),9)
            self.assertEqual(table.q(col0='r0c0').count(),0)
            self.assertEqual(table.q(col1='r0c1').count(),0)
            self.assertEqual(len(table.q(col0='r0c0')).all(),0)
            self.assertEqual(len(table.q(col1='r0c1')).all(),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)
            for i in range(1,10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(table.q(col1=f'r{i}c1').all()),1)
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')

            # check del row when no row
            table.rm()
            table.q(col0='r0c0').rm()

            # check add row other col1
            table.rm()
            table.q(col0='r0c0', col1='r0c1').add()
            with self.assertRaises(Exception):
                table.q(col0='r0c0', col1='r1c1').add()

            # check add row other col0
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1='rxc1').add()
            self.assertEqual(table.q(col1='rxc1').count(),10)
            self.assertEqual(len(table.q(col1='rxc1').all()),10)
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)

            # check add row 2nd times
            table.rm()
            table.q(col0='r0c0', col1='r0c1').add()
            with self.assertRaises(Exception):
                table.q(col0='r0c0', col1='r0c1').add()

    def test_many_many(self):
        with tempfile.TemporaryDirectory() as tempdir:
            database_schema_data = {"TABLE_SCHEMA_DATA_LIST":[{
                'TABLE_NAME':'rs0',
                'COLUMN_NAME_LIST':['col0','col1'],
                'INDEX_DATA_LIST':[
                    {'COLUMN_NAME_LIST':['col0'],'UNIQUE':False},
                    {'COLUMN_NAME_LIST':['col1'],'UNIQUE':False},
                ],
            }]}
            database = futsu.pathdb.database(tempdir, database_schema_data)
            table = database.table('rs0')

            # check no row
            self.assertEqual(table.count(),0)
            self.assertEqual(len(table.all()),0)
            self.assertEqual(len(table.q(col0='r0c0').all()),0)
            self.assertEqual(len(table.q(col1='r0c1').all()),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)

            # check add row
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(table.q(col1=f'r{i}c1').all()),1)
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')
            self.assertEqual(table.count(),10)
            self.assertEqual(len(table.all()),10)

            # check del all
            table.rm()
            self.assertEqual(table.count(),0)
            self.assertEqual(len(table.all()),0)

            # check del row
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            table.q(col0='r0c0').rm()
            self.assertEqual(table.count(),9)
            self.assertEqual(len(table.all()),9)
            self.assertEqual(table.q(col0='r0c0').count(),0)
            self.assertEqual(table.q(col1='r0c1').count(),0)
            self.assertEqual(len(table.q(col0='r0c0')).all(),0)
            self.assertEqual(len(table.q(col1='r0c1')).all(),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)
            for i in range(1,10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(table.q(col1=f'r{i}c1').all()),1)
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')

            # check del row when no row
            table.rm()
            table.q(col0='r0c0').rm()

            # check add row other col1
            table.rm()
            for i in range(10):
                table.q(col0='rxc0', col1=f'r{i}c1').add()
            self.assertEqual(table.q(col0='rxc0').count(),10)
            self.assertEqual(len(table.q(col0='rxc0').all()),10)
            for i in range(10):
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(table.q(col1=f'r{i}c1').all()),1)

            # check add row other col0
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1='rxc1').add()
            self.assertEqual(table.q(col1='rxc1').count(),10)
            self.assertEqual(len(table.q(col1='rxc1').all()),10)
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)

            # check add row 2nd times
            table.rm()
            table.q(col0='r0c0', col1='r0c1').add()
            with self.assertRaises(Exception):
                table.q(col0='r0c0', col1='r0c1').add()

    def test_search_no_index(self):
        with tempfile.TemporaryDirectory() as tempdir:
            database_schema_data = {"TABLE_SCHEMA_DATA_LIST":[{
                'TABLE_NAME':'rs0',
                'COLUMN_NAME_LIST':['col0','col1'],
                'INDEX_DATA_LIST':[
                    {'COLUMN_NAME_LIST':['col0'],'UNIQUE':False},
                ],
            }]}
            database = futsu.pathdb.database(tempdir, database_schema_data)
            table = database.table('rs0')

            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(table.q(col0=f'r{i}c0').all()),1)
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(table.q(col1=f'r{i}c1').all()),1)
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').all()[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')
            self.assertEqual(table.count(),10)
            self.assertEqual(len(table.all()),10)
