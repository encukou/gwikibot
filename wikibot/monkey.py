import gevent.monkey

def patch():
    gevent.monkey.patch_socket()
