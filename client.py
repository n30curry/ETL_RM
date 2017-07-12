#!/usr/bin/env python
# coding=utf-8

from multiprocessing.managers import BaseManager

class QueueManager(BaseManager):
    pass

QueueManager.register("get_queue")

manager = QueueManager(address=("192.168.0.200", 50000,), authkey="bunny")
manager.connect()

queue = manager.get_queue()
#queue.put("hello")

print queue.get()
