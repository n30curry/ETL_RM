#!/usr/bin/env python
# coding=utf-8

import Queue
from multiprocessing.managers import BaseManager

IP = ""
# IP = '200.200.200.109'
PORT = 60000
AUTHKEY = "Text password here!"
MAXSIZE = 30

class QueueManager(BaseManager):
    pass

queue = Queue.Queue(MAXSIZE)
QueueManager.register("queue", callable=lambda : queue)

if __name__ == '__main__':
    manager = QueueManager(address=(IP,PORT), authkey=AUTHKEY)
    manager.get_server().serve_forever()

