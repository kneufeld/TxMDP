"""
Twisted implementation of ZeroMQ Majordomo Protocol

MDP specification can be found at http://rfc.zeromq.org/spec:7
"""

__license__ = 'MIT'
__author__ = 'Kurt Neufeld'
__email__ = 'kneufeld@burgundywall.com'

import logging

logger = logging.getLogger('txmdp')

handler = logging.StreamHandler()
handler.setFormatter( logging.Formatter('%(asctime)s %(name)s: %(message)s', "%H:%M:%S") )
logger.handlers = [handler]

logger.setLevel( logging.DEBUG )

import txzmq

# monkey patch the __str__ methods so they're compact and legible
txzmq.ZmqEndpoint.__str__ = lambda self: "(%s,%s)" % (self[0],self[1])
txzmq.ZmqConnection.__str__ = lambda self: "%s(%s)" % (self.__class__.__name__,",".join(map(str,self.endpoints)))

factory = txzmq.ZmqFactory()
factory.registerForShutdown()

default_broker_frontend = 'tcp://127.0.0.1:5657'
default_broker_backend = 'tcp://127.0.0.1:5656'

def make_socket( mdp_type, endpoint, service ):
    """
    mdp_type: str: client, worker, broker
    endpoint: str, tcp://127.0.0.1:5555
    service: str, client, broker, worker-foo, etc
    """
    if mdp_type == 'client':
        return TxMDPClient( factory, endpoint, service )
    if mdp_type == 'broker':
        endpoint = endpoint or default_broker_backend
        service = service or default_broker_frontend
        return TxMDPBroker( factory, endpoint, service )
    if mdp_type == 'worker':
        return TxMDPWorker( factory, endpoint, service )


class RequestTimeout(RuntimeError):
    """
    Exception that is raised when a request times out
    """

from client import TxMDPClient
from broker import TxMDPBroker
from worker import TxMDPWorker

