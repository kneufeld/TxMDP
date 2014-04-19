# TxMDP - Twisted Majordomo Protocol

This is an implementation of the [ØMQ](http://zeromq.org/)
[majordomo protocol](http://rfc.zeromq.org/spec:7) 
using [Twisted](https://twistedmatrix.com/trac/).

## History

Like all projects, this one stands on the shoulders of those that
came before. In particular, the following:

* [guidog/pyzmq-mdp](https://github.com/guidog/pyzmq-mdp)
* [smira/txZMQ](https://github.com/smira/txZMQ)

TxMDP is more or less a direct conversion of pyzmq-mdp. Where pyzmq-mdp
used the zmq event loop, this uses the Twisted reactor. But, since we
weren't directly using the zmq library any more, we now needed the excellent
sockets provided by txZMQ.

## Quality

TxMDP hasn't seen hard production use. Having said that, it did
get a reasonable shakedown in a dev environment and never once behaved
poorly. So I'd consider TxMDP as beta software and would put it in production
with some more load testing.

## TODO

* get on [PyPy](http://pypy.org)!
* more tests!
* documentation!
* clean room re-implementation of txZMQ to re-release this under MIT license

## License

Both pyzmq-mdp and txZMQ were released under the
[GPL](http://opensource.org/licenses/gpl-license.php) so as a result
so must TxMDP. However, I'd prefer to release TxMDP under the
[MIT license](http://opensource.org/licenses/mit-license.php) so you may
regard my changes as being under that more liberal license.

## Authors

 * Kurt Neufeld (https://github.com/kneufeld)

_if you submit a patch make sure you append your name here_
