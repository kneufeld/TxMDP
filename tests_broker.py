#!/usr/bin/env trial

#import unittest
import time
from twisted.trial import unittest
from twisted.internet import task

from twisted.internet import reactor, defer
import txmdp

class TestBroker( unittest.TestCase ):

    def setUp(self):
        self.endpoint = 'tcp://127.0.0.1:5656'
        self.broker = txmdp.make_socket( 'broker', self.endpoint, None )
        self.clock = task.Clock()
        #reactor.callLater = self.clock.callLater

    def tearDown(self):
        self.broker.shutdown()

    def stop(self, msg):
        reactor.stop()
        return msg

    def test_creation(self):
        self.assertTrue( isinstance(self.broker, txmdp.TxMDPBroker) )
        self.assertIs( self.broker.backend, self.broker.frontend )
        self.assertIsNot( self.broker.hb_check_timer, None )

    def test_creation_2(self):
        self.broker.shutdown()
        self.broker = txmdp.make_socket( 'broker', self.endpoint, 'tcp://127.0.0.1:5657' )
        self.assertIsNot( self.broker.backend, self.broker.frontend )

    def test_recv_garbage(self):

        called = []
        def my_callback(msg):
            called.append(True)
            print "my_callback", msg
            return msg

        def on_request( msg ):
            print "on_request", msg
            print msg

        #self.broker.shutdown()
        #self.broker = txmdp.make_socket( 'broker', self.endpoint, 'tcp://127.0.0.1:5657' )

        worker = txmdp.make_socket( 'worker', self.endpoint, 'service_a' )
        worker.on_request = on_request

        endpoint = 'tcp://127.0.0.1:5657'
        client = txmdp.make_socket( 'client', endpoint, 'service_a' )

        msg = 'get me some'
        d = task.deferLater( reactor, 1, client.request, msg, 1 )
        d.addCallback( my_callback )
        d.addBoth( self.stop )

        reactor.run()

        self.assertEqual( called, [True] )
        self.assertEqual( [ msg ], self.successResultOf(d) )
        client.shutdown()


    def _test_timeout(self):
        d = self.broker.request( "an important message", 0.1 )
        self.assertIsInstance( d, defer.Deferred )

        self.clock.advance(0.1)
        self.failureResultOf(d).trap( txmdp.RequestTimeout )

    def _test_timeout_2(self):
        # this is more for my benefit of learning Twisted than a real test
        # as I believe it's functionaly identical to prev test

        called = []

        def my_errback( f, *args ):
            called.append(True)
            self.assertIsInstance( f.value, txmdp.RequestTimeout )

        d = self.broker.request( "an important message", 0.1 )
        d.addErrback( my_errback )

        self.clock.advance(0.1)
        self.assertEqual( called, [True] )

    def _test_double_send(self):
        d = self.broker.request( "an important message" )
        self.broker.request( "an important message" )
        self.failureResultOf(d).trap( RuntimeError )

        # now make sure we everything was correctly reset after internal error
        d = self.broker.request( "an important message" )
        self.assertIsInstance( d, defer.Deferred )


if __name__ == '__main__':
    unittest.main()

