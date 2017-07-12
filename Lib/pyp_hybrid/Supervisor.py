# -*- coding:utf-8 -*- 

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

import time
from functools import wraps

class Supervisor(object):
    """ 用于监管函数的运行 """
    def __init__(self, retryList = None, bLog = True, logger = None, callback = None):
        """
            retryList:List
                记录运行失败后重试间隔时间
                默认不重试
            bLog:Boolean
                运行失败时是否记录
                记录方式为调用 logger 函数
                默认为记录
            logger:Function
                记录函数
            callback:Function
                运行失败重试前运行的函数
                函数输入:
                    和被监控输入相同
                函数输出会作为新的输入赋给输入函数

        """
        super(Supervisor, self).__init__()
        self.retryList = retryList or []
        self.logger = (logger or self.logger) if bLog is True else lambda x: None
        self.callback = callback or self.callback

    def logger(self, msg):
        strTimeFormat = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        logFormatter = "[{Time}] 运行错误：{Error}".format
        print logFormatter(Time = strTimeFormat, Error = msg)

    def callback(self, *args, **kwargs):
        return args, kwargs

    def __call__(self, func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):

            bRetry = False
            if len(self.retryList) is 0:
                return func(*args, **kwargs)

            for iSleep in self.retryList:
                try:
                    if bRetry is True:
                        args, kwargs = self.callback(*args, **kwargs)
                    res = func(*args, **kwargs)
                except Exception as e:
                    self.logger(str(e))
                    time.sleep(iSleep)
                    bRetry = True
                    continue
                break
            else:
                raise e

            return res
        return wrapped_function

class Supervisord(object):
    """
        监督管理类，是对 Supervisor 装饰器类的封装
        该类会监视重启所有派生类中的运行的方法 (以 "_" 开头的方法除外)
    """
    def __init__(self, retryList = None, bLog = True):
        super(Supervisord, self).__init__()
        self.retryList = retryList
        self.bLog = bLog

    def _getAttribute(self, attr):
        try:
            return object.__getattribute__(self, attr)
        except AttributeError as e:
            object.__setattr__(self, attr, None)
            return None

    def __getattribute__(self, attr):
        ref = object.__getattribute__(self, attr)

        if attr.startswith("_") is True:
            return ref

        if hasattr(ref, "__call__") is False:
            return ref

        logger = self._getAttribute("logger")
        callback = self._getAttribute("callback")
        return Supervisor(self.retryList, self.bLog, logger, callback)(ref)


##################################################################

# x = Supervisor(retryList = [1,1])

# @x
# def myFunc():
# 	raise Exception("MyException")

# @Supervisor(retryList = [1,1])
# def otherFunc(x):
# 	raise Exception("MyException")

# myFunc()
# otherFunc(100)

#################################################################

# class TestClass(Supervisord):
#     """docstring for TestClass"""
#     def __init__(self, retryList = None):
#         super(TestClass, self).__init__(retryList = [1,2])
#         self.b = True
    
#     def logger(self, msg):
#         strTimeFormat = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
#         logFormatter = "[TEST][{Time}] 运行错误：{Error}".format
#         print logFormatter(Time = strTimeFormat, Error = msg)

#     def callback(self, *args, **kwargs):
#         self.b = False
#         return args, kwargs
#     def func1(self, attr = ""):
#         print "Func1: {0}".format(attr)
#     def func2(self):
#         if self.b is True:
#             raise Exception("b is True")
#         print "Func2"

# a = TestClass()
# a.func1(500)
# a.func2()