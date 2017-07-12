#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import os
import json

from Lib.pyp import Json
from Lib.pyp import Num
from Lib.pyp import Zip

from Lib.pyp import MDB
from Lib.pyp_hybrid.Supervisor import Supervisord

def reorder(tosort_list, order_list):
    """
        重新对序列进行排序

        Input :
            tosort_list : List
                要被重新排列的列表

            order_list : List
                重新排列的顺序
        
        Return : List
            按照 order_list 排列的新序列
    """

    for ipos in xrange(len(order_list)):

        while ipos != order_list[ipos]:

            iorderpos = order_list[ipos]
            tosort_list[ipos], tosort_list[iorderpos] = tosort_list[iorderpos], tosort_list[ipos]
            order_list[ipos], order_list[iorderpos] = order_list[iorderpos], order_list[ipos]

    return tosort_list

class PyMDB(Supervisord):
    """ 
        对程老师 MDB 的封装 
        需求 MDB 包
    """
    def __init__(self, login_dict = None, bUsePool = False, *args, **kwds):
        super(PyMDB, self).__init__(*args, **kwds)

        # 防止死循环调用 __getattr__
        self.mdb = None

        self.login_dict = login_dict
        self.bUsePool = bUsePool
        self.strUsedTableSet = set()

        self.login(login_dict, bUsePool)

    def __del__(self):
        try:
            for strUsedTable in self.strUsedTableSet:
                self.commit(strUsedTable)

            self.logout()
        except Exception as e:
            pass

    def _login(self, login_dict, bUsePool = False):

        if bUsePool is True:
            self.mdb = MDB
            return self

        if login_dict is None:
            return self

        self.mdb = MDB.DB()
        self.mdb.login(login_dict)
        return self

    def _use(self, str_table):

        self.mdb.use(str_table)
        self.strUsedTableSet.add(str_table)
        return self

    def login(self, login_dict, bUsePool = False):
        try:
            self._login(login_dict, bUsePool)
        except Exception as e:
            print "[ERROR] MDB -> login"
            raise e

    def use(self, str_table):
        try:
            self._use(str_table)
        except Exception as e:
            print "[ERROR] MDB -> use"
            raise e

    def selects(self, table, uuid, config = None):

        doc = self.mdb.selects(table, uuid, config)
        return reorder(doc["docs"], doc["rids"])

    def callback(self, *args, **kwds):

        self.login(self.login_dict, self.bUsePool)

        if self.bUsePool is False:
            for strUsedTable in self.strUsedTableSet:
                self.use(strUsedTable)

        return args, kwds

    def __getattr__(self, attr):
        res = getattr(self.mdb, attr, None)
        if res is None:
            return super(PyMDB, self).__getattribute__(attr)
        return res

    def _file_tail(self, file_obj, line_num = 20, BLOCK_SIZE = 1024):
        
        file_obj.seek(0, os.SEEK_END)
        cur_end_pos = file_obj.tell()

        recv_line_num = line_num + 1
        block_num = -1

        # blocks of size BLOCK_SIZE, in reverse order starting
        # from the end of the file
        blocks = []
        while recv_line_num > 0 and cur_end_pos > 0:
            if cur_end_pos - BLOCK_SIZE > 0:
                # read the last block we haven't yet read
                file_obj.seek(block_num * BLOCK_SIZE, os.SEEK_END)
                blocks.append(file_obj.read(BLOCK_SIZE))
            else:
                # file too small, start from begining
                file_obj.seek(0, os.SEEK_SET)
                # only read what was not read
                blocks.append(file_obj.read(cur_end_pos))

            found_line_num = blocks[-1].count("\n")
            recv_line_num -= found_line_num
            cur_end_pos -= BLOCK_SIZE
            block_num -= 1

        recv_text = "".join(reversed(blocks))
        return "\n".join(recv_text.splitlines()[-line_num:])

    def traverseX(self, str_table, position_list, int_step = 10000):
        """
            遍历 MDB 中的数据
            Inputs :
                str_table : String
                    MDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    格式为 : [(起始位置，终止位置), ...]

                int_step : Integer
                    每次请求 MDB 的条目数量

            Yield : (Dict, long)
                某一条 MDB 数据的信息
                格式为 : (数据字典, 数据位置)
        """

        for position in position_list:

            long_end = position[1] + 1
            for int_current_position in xrange(position[0], long_end, int_step):

                long_fetch_num = min(long_end - int_current_position, int_step)
                fetched_list = self.mdb.fetch(str_table, long(int_current_position), int(long_fetch_num), 1)
                
                for int_num, mdb_dict in enumerate(fetched_list):
                    yield mdb_dict, long(int_current_position) + int_num

    def traverse(self, str_table, position_list = None, int_step = 10000, check_path = None):
        """
            遍历 MDB 中的数据
            Inputs :
                str_table : String
                    MDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    格式为 : [(起始位置，终止位置), ...]
                    如果为 None，则调用 mdb.shards 遍历全部数据

                int_step : Integer
                    每次请求 MDB 的条目数量

                check_path : String
                    断点文件存储位置

            Yield : (Dict, long)
                某一条 MDB 数据的信息
                格式为 : (数据字典, 数据位置)
        """
        if check_path is not None:

            try:
                with open(check_path, "r") as file_obj:
                    data = self._file_tail(file_obj, 1)

                    try:
                        position_list = json.loads(data)
                    except Exception as e:
                        position_list = eval(data)   

                with open(check_path, "w") as file_obj:
                    file_obj.write(data + "\n")

            except Exception as e:
                pass

        if position_list is None:
            position_list = []
            for long_shard in self.mdb.shards(str_table):
                int_count, int_sid = Num.split(long_shard)
                position_list.append([Num.merge(1, int_sid, 1), long_shard])

        if check_path is None:
            return self.traverseX(str_table, position_list, int_step)
        else:
            return self.traverseC(str_table, position_list, int_step, check_path)

    def traverseC(self, str_table, position_list, int_step = 10000, check_path = None):

        """
            遍历 MDB 中的数据
            Inputs :
                str_table : String
                    MDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    格式为 : [(起始位置，终止位置), ...]
                    如果为 None，则调用 mdb.shards 遍历全部数据

                int_step : Integer
                    每次请求 MDB 的条目数量

                check_path : String
                    断点文件存储位置

            Yield : (Dict, long)
                某一条 MDB 数据的信息
                格式为 : (数据列表，起始位置)
        """
        def checkpoint(cp_obj, data):
            """
                存储断点
            """
            
            if cp_obj is None:
                return None

            try:
                str_data = json.dumps(data, ensure_ascii = False)
            except Exception as e:
                str_data = str(data)

            cp_obj.write(str_data + "\n")
            cp_obj.flush()

        def opencp(check_path):
            if check_path is None:
                return None

            return open(check_path, "a")

        def closecp(cp_obj):

            if cp_obj is None:
                return None

            try:
                cp_obj.close()
            except Exception as e:
                pass
        
        cp_obj = opencp(check_path)
        try:
            for position in position_list:
                long_end = position[1] + 1
                for icurpos in xrange(position[0], long_end, int_step):
                    long_fetch_num = min(long_end - icurpos, int_step)

                    position[0] = icurpos + long_fetch_num
                    checkpoint(cp_obj, position_list)

                    fetched_list = self.mdb.fetch(str_table, long(icurpos), int(long_fetch_num), 1)
                    yield fetched_list, long(icurpos)

        finally:
            closecp(cp_obj)
                