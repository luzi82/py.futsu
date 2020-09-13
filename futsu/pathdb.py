import uuid
import futsu.hash
import futsu.json
import futsu.storage

def database(path, database_schema_data):
    return Database(path, database_schema_data)

class Database:

    def __init__(self,path,database_schema_data):
        self._path = path
        self._database_schema_data = database_schema_data

    def table(self, table_name):
        return Q(self, table_name)

class Q:
    
    def __init__(self, database, table_name, condition_dict={}):
        #print(f'WZLNPKOC {condition_dict}')
        self._database = database
        self._table_name = table_name
        self._condition_dict = condition_dict
        #print(f'CHVBMKLC {self._condition_dict}')

    def q(self, **kwargs):
        #print(f'UOEVCIEQ kwargs={kwargs}')
        condition_dict = self._condition_dict.copy()
        condition_dict.update(kwargs)
        #print(f'SABZEOOK condition_dict={condition_dict}')
        return Q(self._database, self._table_name, condition_dict)

    def count(self):
        #print(f'MHEKPFPV {self._condition_dict}')
        best_index_data = _get_best_index_data(self._get_index_data_itr(), self._condition_dict)
        index_row_uuid_list = \
            self._get_all_uuid_itr() if best_index_data == None else \
            self._get_index_row_uuid_itr(best_index_data)
        index_row_uuid_list = list(index_row_uuid_list)
        
        if not self._exist_extra_condition(best_index_data):
            return len(list(index_row_uuid_list))

        row_data_list = map(self._get_row_data, index_row_uuid_list)
        row_data_list = filter(self._is_row_data_match_condition, row_data_list)
        row_count = len(list(row_data_list))
        return row_count

    def all(self):
        row_data_itr = self._get_best_index_row_uuid_itr()
        row_data_itr = map(self._get_row_data, row_data_itr)
        row_data_itr = filter(self._is_row_data_match_condition, row_data_itr)
        return row_data_itr

    def one(self):
        row_data_list = list(self.all())
        if len(row_data_list) == 1:
            return row_data_list[0]
        if len(row_data_list) == 0:
            return None
        raise Exception(f'EERDILLL len(row_data_list)={len(row_data_list)}')

    def add(self):
        # check condition contains all column
        column_name_itr = self._get_column_name_itr()
        unused_column_name_set = set(column_name_itr) - self._condition_dict.keys()
        if len(unused_column_name_set) > 0:
            raise Exception(f'ZGBCRBNK condition not complete, unused column: {list(unused_column_name_set)}')

        # check unique index not exist
        index_data_itr = self._get_index_data_itr()
        index_data_itr = filter(lambda i:i['UNIQUE'],index_data_itr)
        for index_data in index_data_itr:
            if len(list(self._get_index_row_uuid_itr(index_data)))>0:
                raise Exception(f'QIFKUIBD unique index collide')

        # add data to row set
        row_uuid = str(uuid.uuid4())
        row_path = futsu.storage.join(self._database._path,'table_set',self._table_name,'row_set',row_uuid)
        futsu.json.data_to_path(row_path, self._condition_dict)

        # add index
        index_data_itr = self._get_index_data_itr()
        for index_data in index_data_itr:
            index_hash_str = _get_index_hash_str(index_data)
            index_search_hash_str = _get_index_search_hash_str(index_data, self._condition_dict)
            index_path = futsu.storage.join(self._database._path,'table_set',self._table_name,'index_set',index_hash_str,index_search_hash_str)
            index_row_path = futsu.storage.join(index_path, row_uuid)
            futsu.storage.bytes_to_path(index_row_path,b'')

    def rm(self):
        # get rm row uuid list
        index_row_uuid_list = list(self._get_best_index_row_uuid_itr())
        row_data_list = list(map(self._get_row_data,index_row_uuid_list))
        row_uuid_data_list = zip(index_row_uuid_list,row_data_list)
        row_uuid_data_list = filter(lambda i:self._is_row_data_match_condition(i[1]), row_uuid_data_list)

        # prepare index data
        index_data0_list = list(map(lambda i:{
            'index_data':i,
            'index_hash_str':_get_index_hash_str(i)
        },self._get_index_data_itr()))

        for row_uuid, row_data in row_uuid_data_list:
            # rm index
            for index_data0 in index_data0_list:
                index_data = index_data0['index_data']
                index_hash_str = index_data0['index_hash_str']
                index_search_hash_str = _get_index_search_hash_str(index_data, row_data)
                index_path = futsu.storage.join(self._database._path,'table_set',self._table_name,'index_set',index_hash_str,index_search_hash_str)
                index_row_path = futsu.storage.join(index_path, row_uuid)
                futsu.storage.rm(index_row_path)

            # rm data
            row_path = futsu.storage.join(self._database._path,'table_set',self._table_name,'row_set',row_uuid)
            futsu.storage.rm(row_path)

    #def _get_best_index_data(self):
    #    index_data_list = list(self._get_index_data_itr())
    #    match_index_data_list = list(filter(lambda i:_is_match_condition(i, self._condition_dict), index_data_list))
    #    if len(match_index_data_list) <= 0:
    #        return None
    #    column_len_list = list(map(lambda i:len(i['COLUMN_NAME_LIST']), match_index_data_list))
    #    best_column_len = max(column_len_list)
    #    best_index_idx = column_len_list.index(best_column_len)
    #    ret = index_data_list[best_index_idx]
    #    print(f'MEAQTOKL self._condition_dict={self._condition_dict} ret={ret}')
    #    return ret
    #def _get_best_index_data(self):
    #    return _get_best_index_data(self._get_index_data_itr(), self._condition_dict)

    def _get_best_index_row_uuid_itr(self):
        best_index_data = _get_best_index_data(self._get_index_data_itr(), self._condition_dict)
        index_row_uuid_itr = \
            self._get_all_uuid_itr() if best_index_data == None else \
            self._get_index_row_uuid_itr(best_index_data)
        return index_row_uuid_itr

    def _get_index_row_uuid_itr(self, index_data):
        #print(f'OYSTJWJL {self._condition_dict}')
        index_hash_str = _get_index_hash_str(index_data)
        index_search_hash_str = _get_index_search_hash_str(index_data, self._condition_dict)
        index_path = futsu.storage.join(self._database._path,'table_set',self._table_name,'index_set',index_hash_str,index_search_hash_str)
        index_row_path_list = futsu.storage.find(index_path)
        index_row_uuid_list = map(futsu.storage.basename, index_row_path_list)
        return index_row_uuid_list

    def _get_row_data(self, row_uuid):
        row_path = futsu.storage.join(self._database._path,'table_set',self._table_name,'row_set',row_uuid)
        row_data = futsu.json.path_to_data(row_path)
        return row_data

    def _get_index_data_itr(self):
        return self._database._database_schema_data\
            ['TABLE_SCHEMA_DATA_DICT'][self._table_name]\
            ['INDEX_DATA_LIST']

    def _get_all_uuid_itr(self):
        row_set_path = futsu.storage.join(self._database._path,'table_set',self._table_name,'row_set')
        row_data_path_list = futsu.storage.find(row_set_path)
        row_uuid_list = map(futsu.storage.basename, row_data_path_list)
        return row_uuid_list

    def _get_column_name_itr(self):
        return self._database._database_schema_data\
            ['TABLE_SCHEMA_DATA_DICT'][self._table_name]\
            ['COLUMN_NAME_LIST']

    def _exist_extra_condition(self, index_data):
        column_name_list = [] if index_data == None else index_data['COLUMN_NAME_LIST']
        return len(self._condition_dict.keys()-set(column_name_list))>0

    def _is_row_data_match_condition(self, row_data):
        for k, v in self._condition_dict.items():
            if row_data[k] != v: return False
        return True

def _get_index_hash_str(index_data):
    ret = index_data['COLUMN_NAME_LIST']
    ret = sorted(ret)
    ret = ','.join(ret)
    ret = futsu.hash.sha256_str(ret)
    return ret

def _get_index_search_hash_str(index_data, condition_dict):
    # print(f'IJYNVQBB index_data={index_data} condition_dict={condition_dict}')
    ret = index_data['COLUMN_NAME_LIST']
    ret = sorted(ret)
    ret0 = map(lambda i:condition_dict[i],ret)
    ret = zip(ret,ret0)
    ret = map(lambda i:':'.join(i),ret)
    ret = ','.join(ret)
    ret = futsu.hash.sha256_str(ret)
    return ret

def _is_match_condition(index_data, condition_dict):
    column_name_list = index_data['COLUMN_NAME_LIST']
    return len(set(column_name_list)-condition_dict.keys())==0

def _get_best_index_data(index_data_itr, condition_dict):
    index_data_list = list(index_data_itr)
    match_index_data_list = list(filter(lambda i:_is_match_condition(i, condition_dict), index_data_list))
    #print(f'XRPOGIEF match_index_data_list={match_index_data_list}')
    if len(match_index_data_list) <= 0:
        return None
    column_len_list = list(map(lambda i:len(i['COLUMN_NAME_LIST']), match_index_data_list))
    best_column_len = max(column_len_list)
    best_index_idx = column_len_list.index(best_column_len)
    ret = match_index_data_list[best_index_idx]
    #print(f'MEAQTOKL condition_dict={condition_dict} ret={ret}')
    return ret
    