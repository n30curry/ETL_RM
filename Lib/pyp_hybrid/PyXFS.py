# -*- coding:utf-8 -*- 

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import os

from Lib.pyp import Json
from Lib.pyp import Num
from Lib.pyp import Zip

from Lib.pyp import XFS
import Lib.pyp_hybrid.BitTreatment
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

class PyXFS(Supervisord):
    """ 
        对程老师 XFS 的封装 
        需求 XFS 包
    """
    def __init__(self, loginDict = None, bUsePool = False, cluster = True, *args, **kwds):
        super(PyXFS, self).__init__(*args, **kwds)

        # 防止死循环调用 __getattr__
        self.xfs = None

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
            self.xfs = XFS
            return self

        if loginDict is None:
            return self

        self.xfs = XFS.FS()
        self.xfs.login(loginDict)
        return self

    def _use(self, strTable):

        self.xfs.use(strTable)
        self.strUsedTableSet.add(strTable)
        return self

    def login(self, loginDict, bUsePool = False):
        try:
            self._login(loginDict, bUsePool)
        except Exception as e:
            print "[ERROR] XFS -> login"
            raise e

    def use(self, strTable):
        try:
            self._use(strTable)
        except Exception as e:
            print "[ERROR] XFS -> use"
            raise e

    def selects(self, table, uuids, flag):

        doc = self.xfs.selects(table, uuids, flag)
        return {
            "metas" : reorder(doc["metas"], list(doc["rids"])),
            "streams" : reorder(doc["streams"], list(doc["rids"])),
        }

    def fetchs(self, table, uuids):

        doc = self.xfs.fetchs(table, uuids)
        return {
            "metas" : reorder(doc["metas"], list(doc["rids"])),
            "streams" : reorder(doc["streams"], list(doc["rids"])),
        }

    def callback(self, *args, **kwds):

        self.login(self.loginDict, self.bUsePool)

        if self.bUsePool is False:
            for strUsedTable in self.strUsedTableSet:
                self.use(strUsedTable)

        return args, kwds

    def __getattr__(self, attr):
        res = getattr(self.xfs, attr, None)
        if res is None:
            return super(PyXFS, self).__getattribute__(attr)
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

    def traverseC(self, str_table, position_list, int_step = 10000, check_path = None):
        """
            遍历 XFS 中的数据
            Inputs :
                str_table : String
                    XFS表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    格式为 : [(起始位置，终止位置), ...]

                int_step : Integer
                    每次请求 XFS 的条目数量

                check_path : String
                    断点文件存储位置

            Yield : (Dict, Dict, long)
                某一条 XFS 数据的信息
                格式为 : (元数据字典, 数据字典, 数据位置)
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
                for int_current_position in xrange(position[0], long_end, int_step):
                    long_fetch_num = min(long_end - int_current_position, int_step)

                    position[0] = int_current_position + long_fetch_num
                    checkpoint(cp_obj, position_list)

                    fetched_dict = self.xfs.fetch(str_table, long(int_current_position), int(long_fetch_num), -1)

                    for int_num, str_meta_dict in enumerate(fetched_dict["metas"]):
                        yield str_meta_dict, fetched_dict["streams"][int_num], long(int_current_position) + int_num
        finally:
            closecp(cp_obj)

    def traverseX(self, str_table, position_list, int_step = 10000):
        """
            遍历 XFS 中的数据
            Inputs :
                str_table : String
                    XFS表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    格式为 : [(起始位置，终止位置), ...]

                int_step : Integer
                    每次请求 XFS 的条目数量

            Yield : (Dict, Dict, long)
                某一条 XFS 数据的信息
                格式为 : (元数据字典, 数据字典, 数据位置)
        """

        for position in position_list:

            long_end = position[1] + 1
            for int_current_position in xrange(position[0], long_end, int_step):
                long_fetch_num = min(long_end - int_current_position, int_step)

                fetched_dict = self.xfs.fetch(str_table, long(int_current_position), int(long_fetch_num), -1)

                for int_num, str_meta_dict in enumerate(fetched_dict["metas"]):
                    yield str_meta_dict, fetched_dict["streams"][int_num], long(int_current_position) + int_num


    def traverse(self, str_table, position_list = None, int_step = 10000, check_path = None):
        """
            遍历 XFS 中的数据
            Inputs :
                str_table : String
                    XFS表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    格式为 : [(起始位置，终止位置), ...]
                    如果为 None，则调用 xfs.shards 遍历全部数据

                int_step : Integer
                    每次请求 XFS 的条目数量

                check_path : String
                    断点文件存储位置

            Yield : (Dict, Dict, long)
                某一条 XFS 数据的信息
                格式为 : (元数据字典, 数据字典, 数据位置)
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
            for long_shard in self.xfs.shards(str_table):
                int_count, int_sid = Num.split(long_shard)
                position_list.append([Num.merge(1, int_sid, 1), long_shard])

        if check_path is None:
            return self.traverseX(str_table, position_list, int_step)
        else:
            return self.traverseC(str_table, position_list, int_step, check_path)

    # def length(self, strTable):
    #     iLength = 0
    #     for sliceDict in self.shards(strTable):
    #         for iFilePosition in sliceDict['list']:
    #             iEndPosition, iBeginPosition = BitTreatment.intSplit(iFilePosition, 8, 16)
    #             iLength += iEndPosition - iBeginPosition
    #         iLength -= len(sliceDict['xlist'])
    #     return iLength

    # def validFileIndex(self, strTable, step = 1):
    #     iCounter = 0
    #     for sliceDict in self.shards(strTable):

    #         excludeDict = {BitTreatment.intStitch(sliceDict['sid'], iExceptPosition, 8) : '' \
    #                             for iExceptPosition in sliceDict['xlist']}

    #         # excludeList = set(sliceDict['xlist'])
    #         for iFilePosition in sliceDict['list']:
    #             iEndPosition, iBeginPosition = BitTreatment.intSplit(iFilePosition, 8, 16)
    #             for iPosition in xrange(iBeginPosition, iEndPosition+1):
    #                 # if iPosition in excludeList:
    #                 if iPosition in excludeDict:
    #                     continue
    #                 if iCounter % step is 0:
    #                     iCounter = 0
    #                     yield BitTreatment.intStitch(sliceDict['sid'], iPosition, 8)
    #                 iCounter += 1

    # def traversalSingle(self, strTable):
    #     for iFileIndex in self.validFileIndex(strTable):
    #         bShowStatus = yield self.select(strTable, iFileIndex)
    #         if bShowStatus is True:
    #             yield iFileIndex

    # def traversalMultiple(self, strTable, step):
    #     for iFileIndex in self.validFileIndex(strTable, step):
    #         bShowStatus = yield self.fetch(strTable, iFileIndex, step)
    #         if bShowStatus is True:
    #             yield iFileIndex

    # def traversal(self, strTable, step = 1):

    #     def __formatStep(step):
    #         try:
    #             iStep = int(step)
    #         except Exception as e:
    #             return 1

    #         return 1 if iStep < 1 else iStep

    #     def __update(feedback):
    #         """
    #             可接受 feedback:
    #                 ["status"]
    #                     返回当前文件Index
    #                 ["step", newStep]
    #                     更改 step 长度
    #         """
    #         try:
    #             if feedback[0] is "status":
    #                 return iFileIndex
    #             elif feedback[0] is "step":
    #                 step = __formatStep(feedback[1])
    #                 return 1
    #             return -1
    #         except Exception as e:
    #             print e
    #             return -1

    #     step = __formatStep(step)

    #     for iFileIndex in self.validFileIndex(strTable, step):
    #         if step is 1:
    #             feedback = yield self.select(strTable, iFileIndex)
    #         else:
    #             feedback = yield self.query(strTable, iFileIndex, step)
    #         if feedback is not None:
    #             yield __update(feedback)
