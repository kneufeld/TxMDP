"""
Twisted implementation of ZeroMQ Majordomo Protocol

MDP specification can be found at http://rfc.zeromq.org/spec:7
"""

__license__ = 'MIT'
__author__ = 'Kurt Neufeld'
__email__ = 'kneufeld@burgundywall.com'

import logging
logger = logging.getLogger('txmdp.broker')

from twisted.internet import reactor, defer, error
from twisted.python.failure import Failure

import txzmq

from util import split_address


class TxMDPBroker(object):
    _hb_interval = 2.0
    _hb_liveness = 4

    _mdp_client_ver = b'MDPC01'  #: Client protocol identifier
    _mdp_worker_ver = b'MDPW01'  #: Worker protocol identifier

    def __init__(self, factory, backend_ep, frontend_ep=None):
        """
        Initialize TxMDPBroker

        :param factory:     likely txmdp.factory
        :type factory:      ZmqFactory
        :param backend_ep:  endpoint that the workers connect to
        :type backend_ep:   str
        :param frontend_ep: the optional endpoint that clients connect to
        :type frontend_ep:  str or None, defaults to backend_ep
        """

        self.backend_ep = txzmq.ZmqEndpoint(txzmq.ZmqEndpointType.bind, backend_ep)
        self.backend = txzmq.ZmqRouterConnection(factory, self.backend_ep, 'broker_backend')
        self.backend.messageReceived = self.on_message

        logger.info( "backend listening on: %s", self.backend )

        if frontend_ep is None:
            self.frontend_ep = self.backend_ep
            self.frontend = self.backend

            logger.info( "frontend using same socket as backend" )
        else:
            self.frontend_ep = txzmq.ZmqEndpoint(txzmq.ZmqEndpointType.bind, frontend_ep)
            self.frontend = txzmq.ZmqRouterConnection(factory, self.frontend_ep, 'broker_frontend')
            self.frontend.messageReceived = self.on_message

            logger.info( "frontend listening on: %s", self.frontend )

        self._workers = {} # worker.id -> worker

        # services contain the worker queue and the request queue
        self._services = {}
        self._worker_cmds = { b'\x01': self.on_ready,
                              b'\x03': self.on_reply,
                              b'\x04': self.on_heartbeat,
                              b'\x05': self.on_disconnect, }

        self.hb_start_timer()

    def _get_worker( self, wid ):
        if type(wid) is list:
            wid = wid[0]

        wrep = self._workers[wid]
        return wrep

    def hb_start_timer(self):
        self.hb_check_timer = reactor.callLater( self._hb_interval, self.on_timer )

    def on_timer(self):
        """
        Heartbeat timer callback

        unregisters dead workers
        """
        for wrep in self._workers.values():
            if not wrep.is_alive():
                self.unregister_worker(wrep.id)

        self.hb_start_timer()

    def shutdown(self):
        """
        shutdown broker and shutdown sockets
        """
        self.reset()

        if self.backend.socket:
            self.backend.shutdown()

        if self.frontend.socket:
            self.frontend.shutdown() # shutdown() not idempotent

        self._services = {}

    def _cancel( self, timer ):
        try:
            timer.cancel()
        except (AttributeError,error.AlreadyCalled,error.AlreadyCancelled):
            pass
        timer = None

    def reset(self):
        self._cancel( self.hb_check_timer )

        for wrep in self._workers.values():
            self.unregister_worker( wrep.id )

        self._workers = {}

    def register_worker(self, wid, service):
        """Register the worker id and add it to the given service.

        Does nothing if worker is already known.

        :param wid:    the worker id.
        :type wid:     str
        :param service:    the service name.
        :type service:     str

        :rtype: None
        """
        logger.debug( "registering worker: %s", wid )

        if wid in self._workers:
            return

        self._workers[wid] = WorkerRep(self, wid, service)

        if service in self._services:
            wqueue, wr = self._services[service]
            wqueue.put(wid)
        else:
            wqueue = ServiceQueue()
            wqueue.put(wid)
            self._services[service] = (wqueue, [])

        self.check_for_work( service )


    def unregister_worker(self, wid):
        """Unregister the worker with the given id.

        If the worker id is not registered, nothing happens.

        Will stop all timers for the worker.

        :param wid:    the worker id.
        :type wid:     str

        :rtype: None
        """
        try:
            wrep = self._get_worker(wid)
        except KeyError:
            # not registered, ignore
            return

        logger.debug( "unregistering worker: %s", wid )

        wrep.shutdown()
        service = wrep.service

        if service in self._services:
            wqueue, wr = self._services[service]
            wqueue.remove(wid)

        del self._workers[wid]

    def check_for_work( self, service ):
        wqueue, req_queue = self._services[service]

        if req_queue:
            proto, rp, msg = req_queue.pop(0)
            self.on_client(proto, rp, msg)

    def disconnect(self, wid):
        """Send disconnect command and unregister worker.

        If the worker id is not registered, nothing happens.

        :param wid:    the worker id.
        :type wid:     str

        :rtype: None
        """
        try:
            wrep = self._get_worker(wid)
        except KeyError:
            # not registered, ignore
            return

        logger.info( "disconnecting worker(%s)", wid )

        to_send = [ '', TxMDPBroker._mdp_worker_ver, b'\x05' ]
        self.backend.sendMultipart( wid, to_send )
        self.unregister_worker(wid)


    def client_response(self, rp, service, msg):
        """Package and send reply to client.

        :param rp:       return address stack
        :type rp:        list of str
        :param service:  name of service
        :type service:   str
        :param msg:      message parts
        :type msg:       list of str

        :rtype: None
        """
        logger.debug( "client(%s) -> response: num frames: %d", rp[0], len(msg) )

        to_send = rp[1:] + [ b'', TxMDPBroker._mdp_client_ver, service]
        to_send.extend(msg)

        self.frontend.sendMultipart( rp[0], to_send )


    def on_ready(self, rp, msg):
        """Process worker READY command.

        Registers the worker for a service.

        :param rp:  return address stack
        :type rp:   list of str
        :param msg: message parts
        :type msg:  list of str

        :rtype: None
        """
        logger.debug( "worker(%s) <- ready message", rp[0] )

        ret_id = rp[0]
        self.register_worker(ret_id, msg[0])


    def on_reply(self, rp, msg):
        """Process worker REPLY command.

        Route the `msg` to the client given by the address(es) in front of `msg`.

        :param rp:  return address stack
        :type rp:   list of str
        :param msg: message parts
        :type msg:  list of str

        :rtype: None
        """
        logger.debug( "worker(%s) <- reply: num frames: %d", rp[0], len(msg) )

        ret_id = rp[0]

        try:
            wrep = self._get_worker(ret_id)
        except IndexError:
            logger.warn( "<- unknown worker(%s), ignoring message", ret_id )
            return

        service = wrep.service

        # make worker available again
        try:
            wqueue, req_queue = self._services[service]
            cp, msg = split_address(msg)

            self.client_response(cp, service, msg)

            wqueue.put(wrep.id)

            self.check_for_work( service )
        except KeyError:
            logger.warn( "worker(%s): unknown service: %s", rp[0], service )
            self.disconnect(ret_id)

    def on_heartbeat(self, rp, msg):
        """Process worker HEARTBEAT command.

        :param rp:  return address stack
        :type rp:   list of str
        :param msg: message parts
        :type msg:  list of str

        :rtype: None
        """
        #logger.debug( "worker(%s) <- heartbeat", rp[0] )

        ret_id = rp[0]

        try:
            worker = self._workers[ret_id]

            if worker.is_alive():
                worker.on_heartbeat()
        except KeyError:
            pass # ignore HB for unknown worker


    def on_disconnect(self, rp, msg):
        """Process worker DISCONNECT command.

        Unregisters the worker who sent this message.

        :param rp:  return address stack
        :type rp:   list of str
        :param msg: message parts
        :type msg:  list of str

        :rtype: None
        """
        wid = rp[0]
        self.unregister_worker(wid)
        return

    def on_mmi(self, rp, service, msg):
        """Process MMI request.

        For now only mmi.service is handled.

        :param rp:      return address stack
        :type rp:       list of str
        :param service: the protocol id sent
        :type service:  str
        :param msg:     message parts
        :type msg:      list of str

        :rtype: None
        """
        if service == b'mmi.service':
            s = msg[0]
            ret = b'404'
            for wr in self._workers.values():
                if s == wr.service:
                    ret = b'200'
                    break
            self.client_response(rp, service, [ret])
        else:
            self.client_response(rp, service, [b'501'])


    def on_client(self, proto, rp, msg):
        """Method called on client message.

        Frame 0 of msg is the requested service.
        The remaining frames are the request to forward to the worker.

        .. note::

           If the service is unknown to the broker the message is
           ignored.

        .. note::

           If currently no worker is available for a known service,
           the message is queued for later delivery.

        If a worker is available for the requested service, the
        message is repackaged and sent to the worker. The worker in
        question is removed from the pool of available workers.

        If the service name starts with `mmi.`, the message is passed to
        the internal MMI_ handler.

        .. _MMI: http://rfc.zeromq.org/spec:8

        :param proto: the protocol id sent
        :type proto:  str
        :param rp:    return address stack
        :type rp:     list of str
        :param msg:   message parts
        :type msg:    list of str

        :rtype: None
        """
        #logger.debug( "client(%s) <- %s", rp[0], msg )

        service = msg.pop(0)

        if service.startswith(b'mmi.'):
            self.on_mmi(rp, service, msg)
            return

        try:
            wqueue, wr = self._services[service]
            wid = wqueue.get()
            if not wid:
                logger.debug( "no worker ready, queue message" )
                msg.insert(0, service)
                wr.append( (proto, rp, msg) )
                return

            to_send = [ b'', self._mdp_worker_ver, b'\x02']
            to_send.extend(rp)
            to_send.append(b'')
            to_send.extend(msg)

            logger.debug( "%s -> worker(%s)", to_send, wid )
            self.backend.sendMultipart( wid, to_send )
        except KeyError:
            # unknown service, ignore request
            logger.warn( "client(%s) asked for unknown service: %s", rp[0], service )
            self.client_response( rp, service, ['error','unknown service'] )


    def on_worker(self, proto, rp, msg):
        """Method called on worker message.

        Frame 0 of msg is the command id.
        The remaining frames depend on the command.

        This method determines the command sent by the worker and
        calls the appropriate method. If the command is unknown the
        message is ignored and a DISCONNECT is sent.

        :param proto: the protocol id sent
        :type proto:  str
        :param rp:  return address stack
        :type rp:   list of str
        :param msg: message parts
        :type msg:  list of str

        :rtype: None
        """
        #logger.debug( "worker(%s) <- %s", rp[0], msg )

        cmd = msg.pop(0)

        if cmd in self._worker_cmds:
            fnc = self._worker_cmds[cmd]
            fnc(rp, msg)
        else:
            # unknown command, DISCONNECT worker
            logger.error( "worker(%s) <- unknown command: %s", rp[0], msg )
            self.disconnect(rp[0])
        return

    def on_message(self, msg):
        """Processes given message.

        Decides what kind of message it is -- client or worker -- and
        calls the appropriate method. If unknown, the message is
        ignored.

        :param msg: message parts
        :type msg:  list of str

        :rtype: None
        """
        #logger.debug( "INCOMING: %s", msg )
        rp, msg = split_address(msg)

        # dispatch on first frame after path
        proto = msg.pop(0)

        if proto.startswith(self._mdp_worker_ver):
            self.on_worker(proto, rp, msg)
        elif proto.startswith(self._mdp_client_ver):
            self.on_client(proto, rp, msg)
        else:
            logger.debug( "bad msg: %s", msg )
            logger.warn( "unknown protocol: %s", proto )


class WorkerRep(object):

    """Helper class to represent a worker in the broker.

    Instances of this class are used to track the state of the attached worker
    and carry the timers for incomming and outgoing heartbeats.

    :param proto:    the worker protocol id.
    :type wid:       str
    :param wid:      the worker id.
    :type wid:       str
    :param service:  service this worker serves
    :type service:   str
    :param stream:   the ZMQStream used to send messages
    :type stream:    ZMQStream
    """

    def __init__(self, broker, wid, service):
        self.broker = broker
        self.id = wid
        self.service = service
        self.curr_liveness = TxMDPBroker._hb_liveness

        self.hb_send()


    def hb_start_timer(self):
        self._hb_send_timer = reactor.callLater( TxMDPBroker._hb_interval, self.hb_send )

    def hb_send(self):
        """Construct and send HB message to broker.
        """

        self.curr_liveness -= 1
        if self.curr_liveness == 0:
            self.broker.unregister_worker( self.id )
            return

        #logger.debug( "heartbeat -> worker(%s)", self.id )

        hb_msg = [ b'', TxMDPBroker._mdp_worker_ver, b'\x04' ]
        self.broker.backend.sendMultipart( self.id, hb_msg )
        self.hb_start_timer()

    def on_heartbeat(self):
        """Called when a heartbeat message from the worker was received.

        Sets current liveness to HB_LIVENESS.
        """
        self.curr_liveness = TxMDPBroker._hb_liveness


    def is_alive(self):
        """Returns True when the worker is considered alive.
        """
        return self.curr_liveness > 0

    def shutdown(self):
        """Cleanup worker.

        Stops timer.
        """
        self.broker._cancel( self._hb_send_timer )


class ServiceQueue(object):
    """
    A given service can have several workers, rotate amongst them
    when assigning work.
    """

    def __init__(self):
        """Initialize queue instance.
        """
        self.q = []
        return

    def __contains__(self, wid):
        """Check if given worker id is already in queue.

        :param wid:    the workers id
        :type wid:     str
        :rtype:        bool
        """
        return wid in self.q

    def __len__(self):
        return len(self.q)

    def remove(self, wid):
        try:
            self.q.remove(wid)
        except ValueError:
            pass

    def put(self, wid, *args, **kwargs):
        if wid not in self.q:
            self.q.append(wid)

    def get(self):
        if not self.q:
            return None
        return self.q.pop(0)


if __name__ == "__main__":
    from txmdp import make_socket
    endpoint = 'tcp://127.0.0.1:5656'
    broker = make_socket( 'broker', endpoint, None )
    reactor.run()
