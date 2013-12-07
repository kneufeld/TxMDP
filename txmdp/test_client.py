#!/usr/bin/env trial

import logging
logging.disable(logging.CRITICAL)

import time
from twisted.trial import unittest
from twisted.internet import task

from twisted.internet import reactor, defer
import txmdp

class TestClient( unittest.TestCase ):

    def setUp(self):
        self.endpoint = 'tcp://127.0.0.1:15656'
        self.client = txmdp.make_socket( 'client', self.endpoint, 'service_a' )
        self.clock = task.Clock()
        reactor.callLater = self.clock.callLater

    def tearDown(self):
        self.client.shutdown()

    def test_creation(self):
        self.assertTrue( isinstance(self.client, txmdp.TxMDPClient) )
        self.assertEqual( self.client.service, 'service_a' )

    def test_shutdown(self):
        self.client.shutdown()
        self.client.shutdown() # idempotent

    def test_cant_request(self):
        self.client.shutdown()
        self.assertRaises( RuntimeError, self.client.request, 's', 'foo' )

    def test_timeout(self):
        d = self.client.request( self.client.service, "an important message", 0.1 )
        self.assertIsInstance( d, defer.Deferred )

        self.clock.advance(0.1)
        self.failureResultOf(d).trap( txmdp.RequestTimeout )

    def test_timeout_2(self):
        # this is more for my benefit of learning Twisted than a real test
        # as I believe it's functionaly identical to prev test

        called = []

        def my_errback( f, *args ):
            called.append(True)
            self.assertIsInstance( f.value, txmdp.RequestTimeout )

        d = self.client.request( self.client.service, "an important message", 0.1 )
        d.addErrback( my_errback )

        self.clock.advance(0.1)
        self.assertEqual( called, [True] )

    def test_double_send(self):
        d1 = self.client.request( self.client.service, "an important message" )
        d2 = self.client.request( self.client.service, "another important message" )
        self.assertIsNot( d1, d2 )


if __name__ == '__main__':
    unittest.main()

