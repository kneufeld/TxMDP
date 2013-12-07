#!/usr/bin/env trial

import logging
logging.disable(logging.CRITICAL)

from twisted.trial import unittest
from twisted.internet import task

from twisted.internet import reactor, defer
import txmdp
from txmdp.worker import TxWorkerEcho

class TestBroker( unittest.TestCase ):

    def setUp(self):
        self.clock = task.Clock()
        #reactor.callLater = self.clock.callLater

        self.endpoint = 'tcp://127.0.0.1:15656'
        self.broker = txmdp.make_socket( 'broker', self.endpoint, None )

        self.service = 'service_a'
        self.worker = TxWorkerEcho( txmdp.factory, self.endpoint, self.service )
        self.client = txmdp.make_socket( 'client', self.endpoint, self.service )

    def tearDown(self):
        self.client.shutdown()
        self.worker.shutdown()
        self.broker.shutdown()

    def test_creation(self):
        self.assertTrue( isinstance(self.broker, txmdp.TxMDPBroker) )
        self.assertIs( self.broker.backend, self.broker.frontend )
        self.assertIsNot( self.broker.hb_check_timer, None )

    def test_creation_2(self):
        self.broker.shutdown()
        self.broker = txmdp.make_socket( 'broker', self.endpoint, 'tcp://127.0.0.1:15657' )
        self.assertIsNot( self.broker.backend, self.broker.frontend )

    def test_recv_1(self):
        """
        frontend == backend
        """
        def asserts( reply, original ):
            self.assertEqual( reply[0], original )

        msg = 'get me some'
        d = self.client.request( self.service, msg, 1 )
        d.addCallback( asserts, msg )

        return d

    def test_recv_2(self):
        """
        frontend != backend
        """
        def asserts( reply, original, things ):
            self.assertEqual( reply[0], original )
            map( lambda t: t.shutdown(), things )

        broker = txmdp.make_socket( 'broker', 'tcp://127.0.0.1:25656', 'tcp://127.0.0.1:25657' )
        worker = TxWorkerEcho( txmdp.factory, broker.backend_ep[1], self.service )
        client = txmdp.make_socket( 'client', broker.frontend_ep[1], self.service )

        msg = 'get me some'
        d = self.client.request( self.service, msg, 1 )
        d.addCallback( asserts, msg, [broker,worker,client] )
        return d

if __name__ == '__main__':
    unittest.main()

