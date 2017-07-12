#!/usr/bin/env python
# coding=utf-8

import os
import Queue
from multiprocessing.managers import BaseManager
from consumer import Consumer

IP = "192.168.0.203"
# IP = '200.200.200.109'
PORT = 60000
AUTHKEY = "Text password here!"
PUTWAIT = 1

class QueueManager(BaseManager):
    pass

queue = Queue.Queue()
QueueManager.register("queue")

class Productor(object):
    """docstring for Productor"""
    def __init__(self):
        super(Productor, self).__init__()

        manager = QueueManager(address=(IP,PORT), authkey=AUTHKEY)
        manager.connect()

        self.queue = manager.queue()

    def produce(self, product):
        
        while True:
            
            try:
                self.queue.put(product, block=True, timeout=PUTWAIT)
                print "[{0}]Put a product".format(os.getpid())
                # if self.queue.full():
                #     time.sleep(1)
                return None
            except Queue.Full as e:
                print "[{0}]Put wait".format(os.getpid())

import time
if __name__ == '__main__':
    
    pid = os.getpid()
    productor = Productor()
    print "[{0}] Start Productor".format(pid)

    for ipos in xrange(10):
        # product = "_".join(["Data", str(pid)])
        product = "_".join(["Data", str(pid), str(ipos)])
        productor.produce(product)
        time.sleep(1)
    