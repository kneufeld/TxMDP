"""
Twisted implementation of ZeroMQ Majordomo Protocol

MDP specification can be found at http://rfc.zeromq.org/spec:7
"""

__license__ = 'MIT'
__author__ = 'Kurt Neufeld'
__email__ = 'kneufeld@burgundywall.com'

import logging
logger = logging.getLogger('txmdp.worker')

from twisted.internet import reactor, defer, error
from twisted.python.failure import Failure

import uuid
import txzmq
import zmq

from util import split_address


class ConnectionNotReadyError(RuntimeError):
    """Exception raised when attempting to use the MDPWorker before the handshake took place.
    """
    pass

class MissingHeartbeat(UserWarning):
    """Exception raised when a heartbeat was not received on time.
    """
    pass

class TxMDPWorker( txzmq.ZmqDealerConnection ):

    _hb_interval = 2.0
    _hb_liveness = 4

    _mdp_ver = b'MDPW01'

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
        identity = str(uuid.uuid4())[:8] # 8 random chars outta be good enough for anybody

        self.endpoint = txzmq.ZmqEndpoint(txzmq.ZmqEndpointType.connect, endpoint)
        super(TxMDPWorker,self).__init__(factory, self.endpoint, identity)

        self.service = service
        self.gotMessage = self._on_message

        self._send_ready()

        self.hb_send()
        self.hb_recv()

    @property
    def is_open(self):
        return self.socket is not None

    def hb_start_timer(self):
        self._hb_send_timer = reactor.callLater( self._hb_interval, self.hb_send )

    def hb_send(self):
        """Construct and send HB message to broker.
        """
        hb_msg = [ b'', self._mdp_ver, b'\x04' ]
        self.sendMultipart( hb_msg )
        self.hb_start_timer()

    def hb_recv(self):
        self._hb_recv_timer = reactor.callLater( self._hb_interval, self.hb_recv_dec )

    def hb_recv_dec(self):
        self.curr_liveness -= 1
        if self.curr_liveness <= 0:
            self._cancel( self._hb_send_timer )
            raise RuntimeError("got no heartbeat from broker")
            # FIXME: don't raise, restart socket and whatnot

        self.hb_recv()

    def _send_ready(self):
        """Helper method to prepare and send the workers READY message.
        """
        ready_msg = [ b'', self._mdp_ver, b'\x01', self.service ]
        d = self.sendMultipart( ready_msg )
        self.curr_liveness = self._hb_liveness

        logger.debug( "worker(%s) -> ready message", self.service )


    def _cancel( self, call ):
        try:
            call.cancel()
        except (AttributeError,error.AlreadyCalled,error.AlreadyCancelled):
            pass
        call = None

    def shutdown(self):
        """Method to deactivate the worker connection completely.

        Will delete the stream and the underlying socket.
        """
        self._cancel( self._hb_send_timer )
        self._cancel( self._hb_recv_timer )
        super(TxMDPWorker,self).shutdown()

    def reply(self, msg):
        """Send the given message.

        msg can either be a byte-string or a list of byte-strings.
        """
        if not isinstance(msg, list):
            msg = [msg]

        to_send = self.envelope
        to_send.extend(msg)

        self.envelope = None

        logger.debug( "worker(%s) -> %s", self.identity, to_send )
        self.sendMultipart(to_send)


    def _on_message(self, *msg):
        """Helper method called on message receive.

        msg is a list w/ the message parts
        """
        msg = list(msg)

        logger.debug( "worker(%s) <- %s", self.identity, msg )

        msg.pop(0)              # 1st part is empty
        proto = msg.pop(0)      # 2nd part is protocol version, TODO check ver
        msg_type = msg.pop(0)   # 3rd part is message type

        if msg_type == b'\x05': # disconnect
            self.curr_liveness = 0 # reconnect will be triggered by hb timer
        elif msg_type == b'\x04': # heartbeat
            self.curr_liveness = self._hb_liveness
        elif msg_type == b'\x02': # request
            # remaining parts are the user message
            envelope, msg = split_address(msg)
            envelope.append(b'')
            envelope = [ b'', self._mdp_ver, b'\x03'] + envelope # REPLY
            self.envelope = envelope

            self.on_request(self,msg)
        else:
            logger.warn( "worker(%s) <- unknown message: %s", self.service, msg )


    def on_request(self, worker, msg):
        """Public method called when a request arrived.

        Must be overloaded!
        """
        pass

class TxWorkerEcho( TxMDPWorker ):
    def on_request( self, _, msg ):
        self.reply(msg)


if __name__ == "__main__":
    from txmdp import make_socket
    endpoint = 'tcp://127.0.0.1:5656'
    broker = make_socket( 'worker', endpoint, 'service_a' )
    reactor.run()
