#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import os
import json

from Lib.pyp import KDB
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

        icount = 0
        while ipos != order_list[ipos]:

            icount += 1
            if icount > len(order_list):
                break

            iorderpos = order_list[ipos]
            tosort_list[ipos], tosort_list[iorderpos] = tosort_list[iorderpos], tosort_list[ipos]
            order_list[ipos], order_list[iorderpos] = order_list[iorderpos], order_list[ipos]

    return tosort_list
    
class PyKDB(Supervisord):
    """ 
        对程老师 KDB 的封装 
        需求 KDB 包
    """
    def __init__(self, loginDict = None, bUsePool = False, cluster = True, *args, **kwds):
        super(PyKDB, self).__init__(*args, **kwds)

        # 防止死循环调用 __getattr__
        self.kdb = None
        
        self.loginDict = loginDict
        self.bUsePool = bUsePool
        self.cluster = cluster

        self.strUsedTableSet = set()

        self.login(loginDict, bUsePool)

    def __del__(self):
        try:

            for strUsedTable in self.strUsedTableSet:
                self.commit(strUsedTable)

            self.logout()
        except Exception as e:
            pass

    def _login(self, loginDict, bUsePool = False):

        if bUsePool is True:
            self.kdb = KDB
            return self

        if loginDict is None:
            return self

        self.kdb = KDB.DB()
        self.kdb.login(loginDict)
        return self

    def _use(self, strTable):

        self.kdb.use(strTable)
        self.strUsedTableSet.add(strTable)
        return self

    def login(self, loginDict, bUsePool = False):
        try:
            self._login(loginDict, bUsePool)
        except Exception as e:
            print "[ERROR] KDB -> login"
            raise e

    def use(self, strTable):
        try:
            self._use(strTable)
        except Exception as e:
            print "[ERROR] KDB -> use"
            raise e

    def callback(self, *args, **kwds):

        self.login(self.loginDict, self.bUsePool)

        if self.bUsePool is False:
            for strUsedTable in self.strUsedTableSet:
                self.use(strUsedTable)

        return args, kwds

    def selects(self, str_table, config_dict):
        result = self.kdb.selects(str_table, config_dict)
        
        # try:
        #     reorder(result["docs"], result["rids"])
        # except Exception as e:
        #     pass
        reorder(result["docs"], result["rids"])

        return result["docs"]

    def queryX(self, strTable, queryData, formatDict):
        data = self.kdb.query(strTable, queryData, formatDict)

        queryDict = {}
        for strKey, strDoc in zip(data["keys"], data["docs"]):
            queryDict.setdefault(strKey, []).append(strDoc)

        return queryDict

    def count(self, strTable):
        
        icount = 0
        for sid in self.kdb.shards(strTable).get("list",[]):
            docs = self.kdb.keyCount(strTable, sid)
            icount += docs.get("count", 0)
            icount += docs.get("xcount", 0)

        return icount

    def _file_tail(self, file_obj, line_num = 20, BLOCK_SIZE = 1024):
        
        file_obj.seek(0, os.SEEK_END)
        # tell() 返回当前文件指针位置
        cur_end_pos = file_obj.tell()
        print "cur_end_pos",cur_end_pos

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
        print "recv_text",recv_text
        print "\n".join(recv_text.splitlines()[-line_num:])
        return "\n".join(recv_text.splitlines()[-line_num:])

    def traverse(self, str_table, position_list = None, int_step = 10000, check_path = None):
        """
            遍历 KDB 中的数据
            Inputs :
                str_table : String
                    KDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]
                    如果为 None，则调用 KDB.shards 遍历全部数据

                int_step : Integer
                    每次请求 KDB 的条目数量
                
                check_path : String
                    断点文件存储位置

            Return : List (Generator)
                KDB 数据的 Key
        """

        if check_path is not None:

            try:
                with open(check_path, "r") as file_obj:
                    data = self._file_tail(file_obj, 1)
                    print data
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
            for sid in self.kdb.shards(str_table)["list"]:
                docs = self.kdb.keyCount(str_table, sid)
                position_list.append({
                        "sid" : sid,
                        "count" : [[0, docs.get("count", 0)]],
                        "xcount" : [[0, docs.get("xcount", 0)]],
                    })

        if check_path is None:
            return self.traverseX(str_table, position_list, int_step)
        else:
            return self.traverseC(str_table, position_list, int_step, check_path)

    def traverseX(self, str_table, position_list, int_step = 10000):
        """
            遍历 KDB 中的数据
            Inputs :
                str_table : String
                    KDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]

                int_step : Integer
                    每次请求 KDB 的条目数量
                
            Return : List (Generator)
                KDB 数据的 Key
        """

        for position_dict in position_list:

            sid = position_dict["sid"]
            for begin, end in position_dict["count"]:
                for pos in xrange(begin, end, int_step):
                    yield self.kdb.logKeys(str_table, sid, pos, int_step)

            for begin, end in position_dict["xcount"]:
                for pos in xrange(begin, end, int_step):
                    yield self.kdb.datKeys(str_table, sid, pos, int_step)


    # def traverseC(self, str_table, position_list = None, int_step = 1000, check_path = None):
    #     """
    #         遍历 KDB 中的数据

    #         启动时会从 check_path 读取断点，新的断点会写入到 check_path 中
    #         注意：
    #             1. 断点无法保证数据完备
    #                断点只保证可以连续输出数据，无法探知输出数据的处理情况
    #                因此在程序中断后无法重传正在传输的数据
    #             2. 数据变动会导致断点失效，断点失效后无法保证数据的连续
    #                数据插入或删除或合并等操作可能会导致数据变动

    #         Inputs :
    #             str_table : String
    #                 KDB 表名

    #             position_list : List
    #                 遍历位置信息，需要部分遍历时使用
                    
    #                 格式为 : [{
    #                             'sid': 片号, 
    #                             'count': [(log起始位置, 终止位置), ...], 
    #                             'xcount': [(dat起始位置, 终止位置), ...],
    #                         }, ... ]

    #             int_step : Integer
    #                 每次请求 KDB 的条目数量
                
    #             check_path : String
    #                 断点文件存储位置

    #         Return : List (Generator)
    #             KDB 数据的 Key
    #     """

    #     if check_path is not None:
    #         try:
    #             with open(check_path, "r") as file_obj:
    #                 position_list = pickle.load(file_obj)
    #         except Exception as e:
    #             pass

    #     if position_list is None:
    #         position_list = []
    #         for sid in self.kdb.shards(str_table)["list"]:
    #             docs = self.kdb.keyCount(str_table, sid)
    #             position_list.append({
    #                     "sid" : sid,
    #                     "count" : [[0, docs.get("count", 0)]],
    #                     "xcount" : [[0, docs.get("xcount", 0)]],
    #                 })

    #     return self._traverseC(str_table, position_list, int_step, check_path)

    # def _traverseC(self, str_table, position_list, int_step = 1000, check_path = None):
    #     """
    #         遍历 KDB 中的数据

    #         启动时会从 check_path 读取断点，新的断点会写入到 check_path 中
    #         注意：
    #             1. 断点无法保证数据完备
    #                断点只保证可以连续输出数据，无法探知输出数据的处理情况
    #                因此在程序中断后无法重传正在传输的数据
    #             2. 数据变动会导致断点失效，断点失效后无法保证数据的连续
    #                数据插入或删除或合并等操作可能会导致数据变动

    #         Inputs :
    #             str_table : String
    #                 KDB 表名

    #             position_list : List
    #                 遍历位置信息，需要部分遍历时使用
                    
    #                 格式为 : [{
    #                             'sid': 片号, 
    #                             'count': [(log起始位置, 终止位置), ...], 
    #                             'xcount': [(dat起始位置, 终止位置), ...],
    #                         }, ... ]

    #             int_step : Integer
    #                 每次请求 KDB 的条目数量
                
    #             check_path : String
    #                 断点文件存储位置

    #         Return : List (Generator)
    #             KDB 数据的 Key
    #     """

    #     def checkpoint(cp_obj, data):
    #         """
    #             存储断点
    #         """
            
    #         if cp_obj is None:
    #             return None

    #         cp_obj.seek(0)
    #         cp_obj.truncate()
    #         pickle.dump(data, cp_obj)

    #     def opencp(check_path):
            
    #         if check_path is None:
    #             return None

    #         return open(check_path, "w")

    #     def closecp(cp_obj):
            
    #         if cp_obj is None:
    #             return None

    #         try:
    #             cp_obj.close()
    #         except Exception as e:
    #             pass

    #     cp_obj = opencp(check_path)
    #     try:
    #         for position_dict in position_list:

    #             sid = position_dict["sid"]
    #             for ipos in xrange(len(position_dict["count"])):
    #                 begin, end = position_dict["count"][ipos]

    #                 for pos in xrange(begin, end, int_step):
    #                     position_dict["count"][ipos][0] = pos + int_step
    #                     checkpoint(cp_obj, position_list)
    #                     yield self.kdb.logKeys(str_table, sid, pos, int_step)

    #             for ipos in xrange(len(position_dict["xcount"])):
    #                 begin, end = position_dict["xcount"][ipos]

    #                 for pos in xrange(begin, end, int_step):
    #                     position_dict["xcount"][ipos][0] = pos + int_step
    #                     checkpoint(cp_obj, position_list)
    #                     yield self.kdb.datKeys(str_table, sid, pos, int_step)
    #     finally:
    #         closecp(cp_obj)

    def traverseC(self, str_table, position_list, int_step = 1000, check_path = None):
        """
            遍历 KDB 中的数据

            启动时会从 check_path 读取断点，新的断点会写入到 check_path 中
            注意：
                1. 断点无法保证数据完备
                   断点只保证可以连续输出数据，无法探知输出数据的处理情况
                   因此在程序中断后无法重传正在传输的数据
                2. 数据变动会导致断点失效，断点失效后无法保证数据的连续
                   数据插入或删除或合并等操作可能会导致数据变动

            Inputs :
                str_table : String
                    KDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]

                int_step : Integer
                    每次请求 KDB 的条目数量
                
                check_path : String
                    断点文件存储位置

            Return : List (Generator)
                KDB 数据的 Key
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
            for position_dict in position_list:

                sid = position_dict["sid"]
                for ipos in xrange(len(position_dict["count"])):
                    begin, end = position_dict["count"][ipos]

                    for pos in xrange(begin, end, int_step):
                        position_dict["count"][ipos][0] = pos + int_step
                        checkpoint(cp_obj, position_list)
                        try:
                            yield self.kdb.logKeys(str_table, sid, pos, int_step)
                        except:
                            continue

                for ipos in xrange(len(position_dict["xcount"])):
                    begin, end = position_dict["xcount"][ipos]

                    for pos in xrange(begin, end, int_step):
                        position_dict["xcount"][ipos][0] = pos + int_step
                        checkpoint(cp_obj, position_list)
                        try:
                            yield self.kdb.datKeys(str_table, sid, pos, int_step)
                        except:
                            continue
                            # print "str_table {0},sid {1},pos {2},int_step {3}".format(str_table,sid,pos,int_step)
        finally:
            closecp(cp_obj)

    def __getattr__(self, attr):
        res = getattr(self.kdb, attr, None)
        if res is None:
            return super(PyKDB, self).__getattribute__(attr)
        return res
