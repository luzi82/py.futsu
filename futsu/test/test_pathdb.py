from unittest import TestCase
import futsu.hash
import futsu.pathdb
import tempfile

class TestPathDB(TestCase):

    def test_get_index_hash_str(self):
        index_data = {'COLUMN_NAME_LIST':['col0','col1'],'UNIQUE':True}
        expected = futsu.hash.sha256_str('col0,col1')
        result = futsu.pathdb._get_index_hash_str(index_data)
        self.assertEqual(result, expected)

        index_data = {'COLUMN_NAME_LIST':['col1','col0'],'UNIQUE':True}
        expected = futsu.hash.sha256_str('col0,col1')
        result = futsu.pathdb._get_index_hash_str(index_data)
        self.assertEqual(result, expected)

    def test_get_index_search_hash_str(self):
        index_data = {'COLUMN_NAME_LIST':['col0','col1'],'UNIQUE':True}
        condition_dict = {'col0':'v0','col1':'v1'}
        expected = futsu.hash.sha256_str('col0:v0,col1:v1')
        result = futsu.pathdb._get_index_search_hash_str(index_data,condition_dict)
        self.assertEqual(result, expected)

        index_data = {'COLUMN_NAME_LIST':['col1','col0'],'UNIQUE':True}
        condition_dict = {'col1':'v1','col0':'v0'}
        expected = futsu.hash.sha256_str('col0:v0,col1:v1')
        result = futsu.pathdb._get_index_search_hash_str(index_data,condition_dict)
        self.assertEqual(result, expected)

    def test_is_match_condition(self):
        index_data = {'COLUMN_NAME_LIST':['col0'],'UNIQUE':False}
        condition_dict = {'col1':'v1'}
        self.assertFalse(futsu.pathdb._is_match_condition(index_data,condition_dict))

    def test_get_best_index_data(self):
        index_data_itr = [
            {'COLUMN_NAME_LIST':['col0'],'UNIQUE':True},
            {'COLUMN_NAME_LIST':['col1'],'UNIQUE':True},
        ]
        condition_dict = {'col1':'v1'}
        self.assertEqual(futsu.pathdb._get_best_index_data(index_data_itr,condition_dict),index_data_itr[1])

    def test_unique_unique(self):
        with tempfile.TemporaryDirectory() as tempdir:
            database_schema_data = {"TABLE_SCHEMA_DATA_DICT":{
                'rs0':{
                    'COLUMN_NAME_LIST':['col0','col1'],
                    'INDEX_DATA_LIST':[
                        {'COLUMN_NAME_LIST':['col0'],'UNIQUE':True},
                        {'COLUMN_NAME_LIST':['col1'],'UNIQUE':True},
                    ],
                },
            }}
            database = futsu.pathdb.database(tempdir, database_schema_data)
            table = database.table('rs0')

            # check no row
            self.assertEqual(table.count(),0)
            self.assertEqual(len(list(table.all())),0)
            self.assertEqual(len(list(table.q(col0='r0c0').all())),0)
            self.assertEqual(len(list(table.q(col1='r0c1').all())),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)

            # check add row
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(list(table.q(col1=f'r{i}c1').all())),1)
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')
            self.assertEqual(table.count(),10)
            self.assertEqual(len(list(table.all())),10)

            # check del all
            table.rm()
            self.assertEqual(table.count(),0)
            self.assertEqual(len(list(table.all())),0)

            # check del row
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            table.q(col0='r0c0').rm()
            self.assertEqual(table.count(),9)
            self.assertEqual(len(list(table.all())),9)
            self.assertEqual(table.q(col0='r0c0').count(),0)
            self.assertEqual(table.q(col1='r0c1').count(),0)
            self.assertEqual(len(list(table.q(col0='r0c0').all())),0)
            self.assertEqual(len(list(table.q(col1='r0c1').all())),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)
            for i in range(1,10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(list(table.q(col1=f'r{i}c1').all())),1)
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col1'],f'r{i}c1')
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
            database_schema_data = {"TABLE_SCHEMA_DATA_DICT":{
                'rs0':{
                    'COLUMN_NAME_LIST':['col0','col1'],
                    'INDEX_DATA_LIST':[
                        {'COLUMN_NAME_LIST':['col0'],'UNIQUE':True},
                        {'COLUMN_NAME_LIST':['col1'],'UNIQUE':False},
                    ],
                },
            }}
            database = futsu.pathdb.database(tempdir, database_schema_data)
            table = database.table('rs0')

            # check no row
            self.assertEqual(table.count(),0)
            self.assertEqual(len(list(table.all())),0)
            self.assertEqual(len(list(table.q(col0='r0c0').all())),0)
            self.assertEqual(len(list(table.q(col1='r0c1').all())),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)

            # check add row
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(list(table.q(col1=f'r{i}c1').all())),1)
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')
            self.assertEqual(table.count(),10)
            self.assertEqual(len(list(table.all())),10)

            # check del all
            table.rm()
            self.assertEqual(table.count(),0)
            self.assertEqual(len(list(table.all())),0)

            # check del row
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            table.q(col0='r0c0').rm()
            self.assertEqual(table.count(),9)
            self.assertEqual(len(list(table.all())),9)
            self.assertEqual(table.q(col0='r0c0').count(),0)
            self.assertEqual(table.q(col1='r0c1').count(),0)
            self.assertEqual(len(list(table.q(col0='r0c0').all())),0)
            self.assertEqual(len(list(table.q(col1='r0c1').all())),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)
            for i in range(1,10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(list(table.q(col1=f'r{i}c1').all())),1)
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col1'],f'r{i}c1')
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
            self.assertEqual(len(list(table.q(col1='rxc1').all())),10)
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)

            # check add row 2nd times
            table.rm()
            table.q(col0='r0c0', col1='r0c1').add()
            with self.assertRaises(Exception):
                table.q(col0='r0c0', col1='r0c1').add()

    def test_many_many(self):
        with tempfile.TemporaryDirectory() as tempdir:
            database_schema_data = {"TABLE_SCHEMA_DATA_DICT":{
                'rs0':{
                    'COLUMN_NAME_LIST':['col0','col1'],
                    'INDEX_DATA_LIST':[
                        {'COLUMN_NAME_LIST':['col0'],'UNIQUE':False},
                        {'COLUMN_NAME_LIST':['col1'],'UNIQUE':False},
                    ],
                },
            }}
            database = futsu.pathdb.database(tempdir, database_schema_data)
            table = database.table('rs0')

            # check no row
            self.assertEqual(table.count(),0)
            self.assertEqual(len(list(table.all())),0)
            self.assertEqual(len(list(table.q(col0='r0c0').all())),0)
            self.assertEqual(len(list(table.q(col1='r0c1').all())),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)

            # check add row
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(list(table.q(col1=f'r{i}c1').all())),1)
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')
            self.assertEqual(table.count(),10)
            self.assertEqual(len(list(table.all())),10)

            # check del all
            table.rm()
            self.assertEqual(table.count(),0)
            self.assertEqual(len(list(table.all())),0)

            # check del row
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            table.q(col0='r0c0').rm()
            self.assertEqual(table.count(),9)
            self.assertEqual(len(list(table.all())),9)
            self.assertEqual(table.q(col0='r0c0').count(),0)
            self.assertEqual(table.q(col1='r0c1').count(),0)
            self.assertEqual(len(list(table.q(col0='r0c0').all())),0)
            self.assertEqual(len(list(table.q(col1='r0c1').all())),0)
            self.assertEqual(table.q(col0='r0c0').one(),None)
            self.assertEqual(table.q(col1='r0c1').one(),None)
            for i in range(1,10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(list(table.q(col1=f'r{i}c1').all())),1)
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col1'],f'r{i}c1')
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
            self.assertEqual(len(list(table.q(col0='rxc0').all())),10)
            for i in range(10):
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(list(table.q(col1=f'r{i}c1').all())),1)

            # check add row other col0
            table.rm()
            for i in range(10):
                table.q(col0=f'r{i}c0', col1='rxc1').add()
            self.assertEqual(table.q(col1='rxc1').count(),10)
            self.assertEqual(len(list(table.q(col1='rxc1').all())),10)
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)

            # check add row 2nd times
            table.rm()
            table.q(col0='r0c0', col1='r0c1').add()
            table.q(col0='r0c0', col1='r0c1').add()

    def test_search_no_index(self):
        with tempfile.TemporaryDirectory() as tempdir:
            database_schema_data = {"TABLE_SCHEMA_DATA_DICT":{
                'rs0':{
                    'COLUMN_NAME_LIST':['col0','col1'],
                    'INDEX_DATA_LIST':[
                        {'COLUMN_NAME_LIST':['col0'],'UNIQUE':False},
                    ],
                },
            }}
            database = futsu.pathdb.database(tempdir, database_schema_data)
            table = database.table('rs0')

            for i in range(10):
                table.q(col0=f'r{i}c0', col1=f'r{i}c1').add()
            for i in range(10):
                self.assertEqual(table.q(col0=f'r{i}c0').count(),1)
                self.assertEqual(len(list(table.q(col0=f'r{i}c0').all())),1)
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col0=f'r{i}c0').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').count(),1)
                self.assertEqual(len(list(table.q(col1=f'r{i}c1').all())),1)
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col0'],f'r{i}c0')
                self.assertEqual(list(table.q(col1=f'r{i}c1').all())[0]['col1'],f'r{i}c1')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col0=f'r{i}c0').one()['col1'],f'r{i}c1')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col0'],f'r{i}c0')
                self.assertEqual(table.q(col1=f'r{i}c1').one()['col1'],f'r{i}c1')
            self.assertEqual(table.count(),10)
            self.assertEqual(len(list(table.all())),10)
