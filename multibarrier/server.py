#!/usr/bin/env python
# coding=utf-8

import multiprocessing
from multiprocessing.managers import BaseManager

PROCESS_NUM=10

class BarrierManager(BaseManager):
    pass

class Barrier(object):
    def __init__(self):
        super(Barrier, self).__init__()
        self._list = [-1]
    
    def incref(self, idx):
        if len(self._list) > idx is False:
            self._list.extend([PROCESS_NUM,PROCESS_NUM,PROCESS_NUM,PROCESS_NUM,PROCESS_NUM])
        self._list[idx+1] += 1

    def decref(self, idx):
        reference = self._list[idx+1] - 1
        self._list[idx+1] = -1 if reference == 0 else reference

    def isready(self, idx):
        return self._list[idx] < 0

    def state(self):
        return self._list

    def __call__(self):
        return self

barriers = Barrier()
barriers._list = [-1, PROCESS_NUM, PROCESS_NUM, PROCESS_NUM]

lock = multiprocessing.Lock()
BarrierManager.register("barriers", callable=lambda : barriers)
BarrierManager.register("lock", callable=lambda : lock)

IP = "127.0.0.1"
PORT = 50000
AUTHKEY = "Put password here!"

manager = BarrierManager(address=(IP,PORT,), authkey=AUTHKEY)
manager.get_server().serve_forever()