"""
Twisted implementation of ZeroMQ Majordomo Protocol

MDP specification can be found at http://rfc.zeromq.org/spec:7
"""

__license__ = 'MIT'
__author__ = 'Kurt Neufeld'
__email__ = 'kneufeld@burgundywall.com'

import txzmq

factory = txzmq.ZmqFactory()
factory.registerForShutdown()

def make_socket( mdp_type, endpoint, service ):
    """
    mdp_type: str: client, worker, broker
    endpoint: str, tcp://127.0.0.1:5555
    service: str, client, broker, worker-foo, etc
    """
    if mdp_type == 'client':
        return TxMDPClient( factory, endpoint, service )
    if mdp_type == 'broker':
        return TxMDPBroker( factory, endpoint, frontend_ep=service )
    if mdp_type == 'worker':
        return txzmq.ZmqREQConnection( factory, endpoint, service )


class RequestTimeout(RuntimeError):
    """
    Exception that is raised when a request times out
    """

from client import TxMDPClient
from broker import TxMDPBroker
#from worker import TxMDPWorker

