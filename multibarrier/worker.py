#!/usr/bin/env python
# coding=utf-8

import time
import random
from multiprocessing import Pool
from multiprocessing.managers import BaseManager

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

def _run(worker_id):
    Worker().run(worker_id)

pool = Pool(processes = 10)
for worker_id in xrange(10):
    pool.apply_async(_run, (worker_id,))
    time.sleep(0.01)

pool.close()
pool.join()

