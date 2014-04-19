"""
Twisted implementation of ZeroMQ Majordomo Protocol - Client

MDP specification can be found at http://rfc.zeromq.org/spec:7
"""

import logging
logger = logging.getLogger('txmdp.client')

from twisted.internet import reactor, defer, error
from twisted.python.failure import Failure

import uuid
import txzmq

from txmdp import RequestTimeout

class TxMDPClient( txzmq.ZmqREQConnection ):
    """
    MDP client class

    """

    _mdp_ver = b'MDPC01'

    def __init__(self, factory, endpoint, service=None):
        """
        Initialize TxMDPClient

        :param factory:  likely txmdp.factory
        :type factory:   ZmqFactory
        :param endpoint: the zmq connection string
        :type endpoint:  str
        :param service: the default service, caller can use as a cache
        :type service:  str
        """
        identity = str(uuid.uuid4())[:8] # 8 random chars outta be good enough for anybody

        self.endpoint = txzmq.ZmqEndpoint(txzmq.ZmqEndpointType.connect, endpoint)
        super(TxMDPClient,self).__init__(factory, self.endpoint, identity )

        self.service = service # write only in this class

        logger.info( "creating %s to %s", self, self.endpoint )

    def __str__(self):
        return "client(%s)" % self.identity

    @property
    def is_open(self):
        return self.socket is not None

    def shutdown(self):
        if self.is_open:
            super(TxMDPClient,self).shutdown()

    def request(self, service, msg, timeout=None):
        """
        send request to broker

        :param msg:     message frames
        :type msg:      str or list of str
        :param timeout: time to wait in seconds.
        :type timeout:  float

        :rtype Deferred
        """
        logger.debug( "%s -> request to %s", self, service )

        if not self.is_open:
            logger.error( "%s socket is closed", service )
            raise RuntimeError("socket is closed")

        self.service = service # so it's always showing the last service call

        if type(msg) not in ( tuple, list ):
            msg = [msg]

        outgoing = [self._mdp_ver, service]
        outgoing.extend( msg )

        # some shenanigans here, the one defer cancels/fires the other
        d_waiting = self.sendMsg( *outgoing )
        d_timeout = self._make_timeout(timeout, d_waiting)

        d_waiting.addCallback( self._cancel_timeout, d_timeout )
        d_waiting.addCallback( self._on_message )

        return d_waiting

    def _make_timeout(self, timeout, d_waiting):
        """
        start the timeout callback

        :param timeout: fractional time in seconds
        :type timeout:  float
        """
        if not timeout:
            return None

        return reactor.callLater( timeout, self._on_timeout, d_waiting )

    def _cancel_timeout(self, msg, d_timeout):
        """
        cancel the timeout callback
        """
        try:
            d_timeout.cancel()
        except (AttributeError,error.AlreadyCalled,error.AlreadyCancelled):
            pass

        return msg

    def _on_timeout(self, d_waiting):
        """
        internal callback for when our request times out
        fire d_waiting as an error
        """
        logger.warn( "%s timed out", self )
        d_waiting.errback( RequestTimeout() )

    def _on_message(self, msg):
        """
        internal callback for when we receive a message
        fires d_waiting with the msg

        :param msg:   list of message frames
        :type msg:    list of str
        """
        logger.debug( "%s <- num frames %d", self, len(msg) )

        msg.pop(0) # strip proto ver
        msg.pop(0) # strip service
        return msg


if __name__ == "__main__":
    from twisted.internet import task

    def stop(*args):
        reactor.stop()

    from txmdp import make_socket
    endpoint = 'tcp://127.0.0.1:5656'
    client = make_socket( 'client', endpoint, 'service_a' )
    d = client.request('service_a', 'get me some', 0.25)
    d.addBoth( stop )

    reactor.run()
