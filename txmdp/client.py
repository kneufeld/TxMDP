"""
Twisted implementation of ZeroMQ Majordomo Protocol

MDP specification can be found at http://rfc.zeromq.org/spec:7
"""

__license__ = 'MIT'
__author__ = 'Kurt Neufeld'
__email__ = 'kneufeld@burgundywall.com'

import logging
logger = logging.getLogger('txmdp.client')

from twisted.internet import reactor, defer, error
from twisted.python.failure import Failure

import txzmq

from txmdp import RequestTimeout

class TxMDPClient( txzmq.ZmqREQConnection ):
    """
    MDP client class

    """

    _mdp_ver = b'MDPC01'

    def __init__(self, factory, endpoint, service):
        """
        Initialize TxMDPClient

        :param factory:  likely txmdp.factory
        :type factory:   ZmqFactory
        :param endpoint: the zmq connection string
        :type endpoint:  str
        :param service:  the worker service to use
        :type service:   str
        """
        self.endpoint = txzmq.ZmqEndpoint(txzmq.ZmqEndpointType.connect, endpoint)
        super(TxMDPClient,self).__init__(factory, self.endpoint, service)

        self.d_waiting = None
        self.d_timeout = None

        logger.info( "creating client: %s", self )

    @property
    def service(self):
        return self.identity

    @property
    def _prefix(self):
        return [self._mdp_ver, self.service]

    @property
    def is_open(self):
        return self.socket is not None

    def shutdown(self):
        self.reset()
        if self.is_open:
            super(TxMDPClient,self).shutdown()

    def reset(self):
        """
        call after an error or received message
        """
        self._cancel_timeout()
        self.d_waiting = None

    def request(self, msg, timeout=None):
        """
        send request to broker

        :param msg:     message frames
        :type msg:      str or list of str
        :param timeout: time to wait in seconds.
        :type timeout:  float

        :rtype Deferred
        """
        logger.debug( "request: client(%s) -> %s", self.service, msg )

        if not self.is_open:
            raise RuntimeError("socket is closed")

        if self.d_waiting is not None:
            self.d_waiting.errback( RuntimeError("already waiting for a response") )
            self.reset()

            # the choice is to not reset() and return the error'd d_waiting
            # or to reset() and return None so the next call to request() will work
            return

        if not isinstance( msg, list ):
            msg = [msg]

        outgoing = self._prefix + msg

        self.d_waiting = self.sendMsg( *outgoing )
        self.d_waiting.addCallback( self._on_message )

        self._start_timeout(timeout)

        return self.d_waiting

    def _start_timeout(self, timeout):
        """
        start the timeout callback

        :param timeout: fractional time in seconds
        :type timeout:  float
        """
        if not timeout:
            return

        self.d_timeout = reactor.callLater( timeout, self._on_timeout )

    def _cancel_timeout(self):
        """
        cancel the timeout callback
        """
        try:
            self.d_timeout.cancel()
        except (AttributeError,error.AlreadyCalled,error.AlreadyCancelled):
            pass

        self.d_timeout = None

    def _on_timeout(self):
        """
        internal callback for when our request times out
        fire d_waiting as an error
        """
        logger.warn( "client(%s) timed out", self.service )
        self.d_waiting.errback( RequestTimeout() )

    def _on_message(self, msg):
        """
        internal callback for when we receive a message
        fires d_waiting with the msg

        :param msg:   list of message frames
        :type msg:    list of str
        """
        logger.debug( "client(%s) <- %s", self.service, msg )
        self._cancel_timeout()

        msg.pop(0) # proto ver
        msg.pop(0) # service
        return msg


if __name__ == "__main__":
    from twisted.internet import task

    def stop(*args):
        reactor.stop()

    from txmdp import make_socket
    endpoint = 'tcp://127.0.0.1:5656'
    client = make_socket( 'client', endpoint, 'service_a' )
    d = client.request("get me some", 0.25)
    d.addBoth( stop )

    reactor.run()
