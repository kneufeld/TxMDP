#!/usr/bin/env trial

#import unittest
from twisted.trial import unittest
from twisted.internet import task

from twisted.internet import reactor, defer
import txmdp

class TestBroker( unittest.TestCase ):

    def setUp(self):
        self.endpoint = 'tcp://127.0.0.1:5656'
        self.broker = txmdp.make_socket( 'broker', self.endpoint, None )
        self.clock = task.Clock()
        reactor.callLater = self.clock.callLater

    def tearDown(self):
        self.broker.shutdown()

    def stop(self):
        reactor.stop()

    def test_creation(self):
        self.assertTrue( isinstance(self.broker, txmdp.TxMDPBroker) )
        self.assertIs( self.broker.backend, self.broker.frontend )
        self.assertIsNot( self.broker.hb_check_timer, None )

    def test_creation_2(self):
        self.broker.shutdown()
        self.broker = txmdp.make_socket( 'broker', self.endpoint, 'tcp://127.0.0.1:5657' )
        self.assertIsNot( self.broker.backend, self.broker.frontend )

    def _test_recv_garbage(self):

        called = []
        def my_callback(self,msg):
            called.append(True)
            print msg

        client = txmdp.make_socket( 'client', self.endpoint, 'service_a' )
        d = client.request( 'gimmee stuff' )
        d.addCallback( my_callback )
        d.addBoth( self.stop )

        print "here"
        reactor.run()

        msg = self.successResultOf(d)
        #msg = yield d
        print "msg",msg

        #self.assertEqual( called, [True] )
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
