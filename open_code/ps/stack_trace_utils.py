import os
import threading
import ctypes

def gettid():
    SYS_gettid = 186
    libc = ctypes.cdll.LoadLibrary('libc.so.6')
    tid = libc.syscall(SYS_gettid)
    return tid

def get_thread_identifier():
    string = 'pid: %d, ' % os.getpid()
    string += 'tid: %d, ' % gettid()
    string += 'thread: 0x%x' % threading.current_thread().ident
    return string
