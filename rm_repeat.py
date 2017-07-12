#!/usr/bin/python
#coding=utf-8

from Lib.pyp import NDB
from Lib.pyp import KDB
from multiprocessing import Pool
from multiprocessing.managers import BaseManager
import sys
import time
import random
import itertools

NDB_HOST = "127.0.0.1"
NDB_PORT = 9869
NDB_TABLE = "test"
NUMBER = 10000
D = 500.0

KDB_HOST = '127.0.0.1'
KDB_PORT = 9909
KDB_CENTER = 'v6xj_kdb_center'
KDB_SUB = 'v6xj_kdb_sub'


class BarrierManager(BaseManager):
    pass

BarrierManager.register("barriers")
BarrierManager.register("lock")

IP = "127.0.0.1"
PORT = 50000
AUTHKEY = "Put password here!"

class Worker(object):
    """docstring for Worker"""
    def __init__(self):
        super(Worker, self).__init__()

        manager = BarrierManager(address=(IP,PORT,), authkey=AUTHKEY)
        manager.connect()

        self.lock = manager.lock()
        self.barriers = manager.barriers()

    def wait_start(self, step_id):
        while self.barriers.isready(step_id) is False:
            time.sleep(0.1)

    def increasese(self, step_id):
        self.lock.acquire()
        self.barriers.incref(step_id)
        self.lock.release()

    def decrease(self, step_id):
        self.lock.acquire()
        self.barriers.decref(step_id)
        self.lock.release()

    def run(self, worker_id):

        step_list = ["STEP-A", "STEP-B", "STEP-C"]
        for ipos, step in enumerate(step_list):

            print "[{0}] Wait ".format(worker_id), step_list[ipos]
            print self.barriers.state()
            self.wait_start(ipos)
            print "[{0}] Start ".format(worker_id), step_list[ipos]

            # self.increasese(ipos)
            time.sleep(random.random())
            if worker_id == 2:
                time.sleep(2)
            print "[{0}] Finished {1}".format(worker_id, step_list[ipos])

            self.decrease(ipos)



class V6xj_rm(object):
	"""docstring for V6xj_rm"""
	def __init__(self):
		super(V6xj_rm, self).__init__()
		
		self.ndb = NDB.DB()
		self.ndb.login({'host':NDB_HOST,'port':NDB_PORT})
		self.ndb.use(NDB_TABLE)
		self.ndb.loads(NDB_TABLE)
		self.count = self.ndb.count(NDB_TABLE)
		print "ndb success"

		# self.kdb1 = KDB.DB()
		# self.kdb1.login({'host':KDB_HOST,'port':KDB_PORT})
		# self.kdb1.use(KDB_CENTER)
		# self.kdb2 = KDB.DB()
		# self.kdb2.login({'host':KDB_HOST,'port':KDB_PORT})
		# self.kdb2.use(KDB_SUB)
		# print 'kdb success'

		self.work = Worker()

	def _range(self, procid, procnum):

		ranges = []
		for piceid in self.ndb.shards(NDB_TABLE)['list']:
			if piceid % procnum != procid:
			    continue

			ranges.append(piceid)
		return ranges[1:]

	def relation1(slef,big_list, small_list):

		temp_list = []
		for ipos,ndoes in enumerate(small_list):

			for node in nodes:
				if node != None:
					temp_list.append([big_list[ipos], node])

		return temp_list

	def relation2(slef,big_list, small_list):

		temp_list = []
		for ipos,ndoes in enumerate(big_list):

			for node in nodes:
				if node != None:
					temp_list.append(node, [small_list[ipos]])

		return temp_list

	def sm_list(self, small_list):

		temp_list = []
		for smalls in small_list:
			for small in smalls:
				if small not in temp_list:
					temp_list.append(small)

		return temp_list

	def run(self, procid, procnum):

		ranges = self._range(procid, procnum)
		step_list = range(65, 101, 5)
		step_list.reverse()
		count = itertools.count()

		for ipos, rank in enumerate(step_list):

			self.work.wait_start(ipos)
			num = count.next()
			if num == 0:
				big_list = []
				for piceid in ranges:
					kid_count = self.ndb.kidCount(NDB_TABLE, rank, piceid)
					bi_list = self.ndb.kidNext(NDB_TABLE, rank, piceid, 0, kid_count)
					big_list.extend(bi_list)
				small_list = self.ndb.kidFetch(NDB_TABLE,big_list)['xkids']
				relation_list1 = self.relation1(big_list, small_list)

				small_listX = self.sm_list(small_list)
				big_listX = self.ndb.kidFetchX(NDB_TABLE, small_listX)['nkids']
				relation_list2 = self.relation2(big_listX, small_listX)
				print "ready"
				relation = [var for var in relation1 if var in relation2]
				
				print "success"
			else:
				big_list = []
				for link in relation:
					big_list.append(link[1])
				small_list = self.ndb.kidFetch(NDB_TABLE,big_list)['xkids']
				relation_list1 = self.relation1(big_list, small_list)

				small_listX = self.sm_list(small_list)
				big_listX = self.ndb.kidFetchX(NDB_TABLE, small_listX)['nkids']
				relation_list2 = self.relation2(big_listX, small_listX)

				relation = [var for var in relation1 if var in relation2]

			self.work.decrease(ipos)

		# #temp_list = self.ndb.execFetch(NDB_TABLE, start, count, D)
		# count = self.count / NUMBER + 1
		# print 'count',count
		# term_count = count / pronum

		# start = 0 + term_count * proid
		# # print "proid>>>>",proid
		# for num in range(term_count*proid, term_count*(proid+1)):

		# 	self.ndb.execLoad(NDB_TABLE)
		# 	temp_list = self.ndb.execFetch(NDB_TABLE, start, NUMBER, D)
		# 	print "temp_list",temp_list
		# 	start += NUMBER
		# 	'''
		# 	将 中心点和从属点插入

		# 	'''

		# 	if len(temp_list) == 0:
		# 		break

		# 	temp_list = map(lambda x: self.ndb.unmap(NDB_TABLE,x) , temp_list)

		# 	kdb_center_list = []
		# 	kdb_sub_list = []
		# 	# print 'temp_list',temp_list
		# 	for term_list in temp_list:

		# 		if len(term_list) > 0:
		# 			# print term_list
		# 			# for i in range(len(term_list) - 1):
		# 			for i in range(len(term_list)):

		# 				# kdb_center_list.append(term_list[i] + '\0' + term_list[i+1])
		# 				# kdb_center_list.append(term_list[i] + '\0' + term_list[i])
		# 				# kdb_sub_list.append(term_list[i+1] + '\0' + term_list[i])
		# 				# kdb_sub_list.append(term_list[i+1] + '\0' + term_list[i+1])


		# 				kdb_center_list.append(term_list[0] + '\0' + term_list[i])
		# 				kdb_sub_list.append(term_list[i] + '\0' + term_list[0])
		# 	# print kdb_center_list
		# 	# exit()
		# 	print "ready inserts"
		# 	self.kdb1.inserts(KDB_CENTER,kdb_center_list,[])
		# 	self.kdb2.inserts(KDB_SUB,kdb_sub_list,[])
		# 	print "inserts success"


def run_as_cmd(procid, procnum):

    loads = V6xj_rm()
    loads.run(procid, procnum)

if __name__ == '__main__':

	begin = time.time()
	startid = int(sys.argv[1])
	endid = int(sys.argv[2])
	procnum = int(sys.argv[3])

	pool = Pool(processes = endid - startid)

	for procid in xrange(startid, endid):
	    pool.apply_async(run_as_cmd, (procid, procnum))

	    time.sleep(0.1)

	pool.close()
	pool.join()


	print "finished useing {}s".format(time.time() - begin)


