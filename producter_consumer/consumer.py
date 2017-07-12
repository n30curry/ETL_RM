#!/usr/bin/env python
# coding=utf-8

import os
import Queue
from multiprocessing.managers import BaseManager

IP = "192.168.0.203"
# IP = '200.200.200.109'
PORT = 60000
AUTHKEY = "Text password here!"
GETWAIT = 3

class QueueManager(BaseManager):
    pass

queue = Queue.Queue()
QueueManager.register("queue")

class Consumer(object):
    """docstring for Consumer"""
    def __init__(self):
        super(Consumer, self).__init__()

        manager = QueueManager(address=(IP,PORT), authkey=AUTHKEY)
        manager.connect()

        self.queue = manager.queue()

    def consume(self):
        
        while True:
            
            try:
                data = self.queue.get(block=True, timeout=GETWAIT)
                # print "[{0}]Get product {1}".format(os.getpid(), data)
                print "Get product"
                return data
            except Queue.Empty as e:
                print "[{0}]Get wait".format(os.getpid())

import time
if __name__ == '__main__':

    pid = os.getpid()
    consumer = Consumer()
    print "[{0}] Start Consumer".format(pid)

    while True:
        
        try:
            data = consumer.consume()
            time.sleep(1)
        except KeyboardInterrupt as e:
            break

    print ""
    print "Finished"