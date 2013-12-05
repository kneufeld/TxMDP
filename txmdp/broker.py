"""
Twisted implementation of ZeroMQ Majordomo Protocol

MDP specification can be found at http://rfc.zeromq.org/spec:7
"""

__license__ = 'MIT'
__author__ = 'Kurt Neufeld'
__email__ = 'kneufeld@burgundywall.com'

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

        print "broker listening on:",self.backend

        if frontend_ep is None:
            print "front is back"
            self.frontend_ep = self.backend_ep
            self.frontend = self.backend
        else:
            print "front is diff"
            self.frontend_ep = txzmq.ZmqEndpoint(txzmq.ZmqEndpointType.bind, frontend_ep)
            self.frontend = txzmq.ZmqRouterConnection(factory, self.frontend_ep, 'broker_frontend')
            self.frontend.messageReceived = self.on_message

            print "broker listening on:",self.frontend

        self._workers = {}
        # services contain the worker queue and the request queue
        self._services = {}
        self._worker_cmds = { b'\x01': self.on_ready,
                              b'\x03': self.on_reply,
                              b'\x04': self.on_heartbeat,
                              b'\x05': self.on_disconnect, }

        self.hb_start_timer()

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
        print "registering worker", wid, service

        if wid in self._workers:
            return
        self._workers[wid] = WorkerRep(self, wid, service)
        if service in self._services:
            wq, wr = self._services[service]
            wq.put(wid)
        else:
            q = ServiceQueue()
            q.put(wid)
            self._services[service] = (q, [])


    def unregister_worker(self, wid):
        """Unregister the worker with the given id.

        If the worker id is not registered, nothing happens.

        Will stop all timers for the worker.

        :param wid:    the worker id.
        :type wid:     str

        :rtype: None
        """
        try:
            wrep = self._workers[wid]
        except KeyError:
            # not registered, ignore
            return

        print "unregister_worker", wid

        wrep.shutdown()
        service = wrep.service
        if service in self._services:
            wq, wr = self._services[service]
            wq.remove(wid)
        del self._workers[wid]


    def disconnect(self, wid):
        """Send disconnect command and unregister worker.

        If the worker id is not registered, nothing happens.

        :param wid:    the worker id.
        :type wid:     str

        :rtype: None
        """
        try:
            wrep = self._workers[wid]
        except KeyError:
            # not registered, ignore
            return

        to_send = [ '', TxMDPBroker._mdp_worker_ver, b'\x05' ]
        self.backend.sendMultipart( wid, to_send )
        self.unregister_worker(wid)
        return

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
        print "client_response", rp, service, msg
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
        print "on_reply",msg

        ret_id = rp[0]
        wrep = self._workers.get(ret_id)
        if not wrep:
            # worker not found, ignore message
            return

        service = wrep.service

        # make worker available again
        try:
            wq, wr = self._services[service]
            cp, msg = split_address(msg)

            self.client_response(cp, service, msg)

            wq.put(wrep.id)
            if wr:
                proto, rp, msg = wr.pop(0)
                self.on_client(proto, rp, msg)
        except KeyError:
            # unknown service
            self.disconnect(ret_id)

    def on_heartbeat(self, rp, msg):
        """Process worker HEARTBEAT command.

        :param rp:  return address stack
        :type rp:   list of str
        :param msg: message parts
        :type msg:  list of str

        :rtype: None
        """
        ret_id = rp[0]

        #print "got heartbeat from", ret_id
        try:
            worker = self._workers[ret_id]
            if worker.is_alive():
                worker.on_heartbeat()
        except KeyError:
            # ignore HB for unknown worker
            pass
        return

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
        return

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
        print "BROKER: on_client", msg
        service = msg.pop(0)

        if service.startswith(b'mmi.'):
            self.on_mmi(rp, service, msg)
            return

        try:
            wq, wr = self._services[service]
            wid = wq.get()
            if not wid:
                # no worker ready
                # queue message
                msg.insert(0, service)
                wr.append((proto, rp, msg))
                return

            to_send = [ b'', self._mdp_worker_ver, b'\x02']
            to_send.extend(rp)
            to_send.append(b'')
            to_send.extend(msg)

            self.backend.sendMultipart( wid, to_send )
        except KeyError:
            # unknwon service
            # ignore request
            print 'broker has no service "%s"' % service
        return

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
        print "BROKER: on_worker",msg

        cmd = msg.pop(0)
        if cmd in self._worker_cmds:
            fnc = self._worker_cmds[cmd]
            fnc(rp, msg)
        else:
            # ignore unknown command
            # DISCONNECT worker
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
        print "BROKER: on_message", msg

        rp, msg = split_address(msg)
        # dispatch on first frame after path
        t = msg.pop(0)
        if t.startswith(self._mdp_worker_ver):
            self.on_worker(t, rp, msg)
        elif t.startswith(self._mdp_client_ver):
            self.on_client(t, rp, msg)
        else:
            print 'Broker unknown Protocol: "%s"' % t


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

        #print "broker sending heartbeat"

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

    """Class defining the Queue interface for workers for a service.

    The methods on this class are the only ones used by the broker.
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
