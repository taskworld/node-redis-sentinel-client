/*
test a failover scenario
should lose no data (atomic set/get or pub/sub) during the failover.

to use this,
  - ./node_modules/.bin/mocha --ui tdd --reporter spec --bail test/test-full
*/

var assert = require('assert'),
    RedisSentinel = require('../index'),
    RedisClient = require('redis'),
    start = require('./start-redis'),
    async = require('async'),
    events = require('events'),
    util = require('util'),
    _suite,
    debug = require('debug')('test'),
    password = 'h3rr0'

var ports = {
  redis1: 5379,
  redis2: 5380,
  sentinel1: 8379,
  sentinel2: 8380
}


suite('sentinel full', function () {

  // (want setup to run once, using BDD-style `before`)
  before(function (done) {
    _suite = this;

    this.hashKey = "test-sentinel-" + Math.round(Math.random() * 1000000);
    this.key = function (s) { return this.hashKey + ":" + s }
    debug("Using test hash", this.hashKey)

    // start up external redis'
    this.processes = {
      redis1: start.redis('./test/redis1.conf', ports.redis1, null, password),
      redis2: start.redis('./test/redis2.conf', ports.redis2, ports.redis1, password),
      sentinel1: start.sentinel('./test/sentinel1.conf', ports.sentinel1, ports.redis1, password),
      sentinel2: start.sentinel('./test/sentinel2.conf', ports.sentinel2, ports.redis1, password)
    }

    setTimeout(onTimeout, 1000)

    function onTimeout() {
      _suite.clients = {
        redis1: RedisClient.createClient(ports.redis1,null,{auth_pass:password}),
        redis2: RedisClient.createClient(ports.redis2,null,{auth_pass:password}),
        sentinel1: RedisClient.createClient(ports.sentinel1),
        sentinel2: RedisClient.createClient(ports.sentinel2)
      }
      _suite.masterClient = _suite.clients.redis1
      done()
    }

    _suite.createSentinelClient = function () {
      return RedisSentinel.createClient({
        sentinels: [
          ['127.0.0.1', 8379],
          ['127.0.0.1', 8380]
        ],
        masterName: 'testmaster',
        master_auth_pass: password,
      }).on('error', function (err) {
        debug('error', err)
      })
    }

  }); //setup


  test('redis master is ready', function (done) {
    var cli = _suite.clients.redis1
    cli.ready ? done() : cli.once('ready', done)
  })

  test('redis slave is ready', function (done) {
    var cli = _suite.clients.redis2
    cli.ready ? done() : cli.once('ready', done)
  })

  test('redis slaveof is working', function (done) {
    var cli1 = _suite.clients.redis1
      , cli2 = _suite.clients.redis2

    cli1.set(_suite.key('getset'), _suite.hashKey, function (err) {
      assert.ifError(err)
      cli1.get(_suite.key('getset'), function (err, rval) {
        assert.ifError(err)
        assert.equal(rval, _suite.hashKey)

        cli2.get(_suite.key('getset'), function (err, sval) {
          assert.ifError(err)
          assert.equal(rval, _suite.hashKey)
          done()
        })
      })
    })
  })

  test('sentinel1 ready', function (done) {
    var cli = _suite.clients.sentinel1
    cli.ready ? done() : cli.once('ready', done)
  })

  test('sentinel ready', function (done) {
    var cli = _suite.clients.sentinel2
    cli.ready ? done() : cli.once('ready', done)
  })

  test('sentinel1 monitor redis', function (done) {
    var cli = _suite.clients.sentinel1
    cli.send_command('sentinel', ['master', 'testmaster'], function (err) {
      assert.ifError(err)
      done()
    })
  })

  test('sentinel2 monitor redis', function (done) {
    var cli = _suite.clients.sentinel2
    cli.send_command('sentinel', ['master', 'testmaster'], function (err) {
      assert.ifError(err)
      done()
    })
  })

  test('setup sentinel client', function (done) {
    var cli = _suite.clients.sentinelClient = _suite.createSentinelClient()
    cli.on('error', function (err) {
      console.log('Error in sentinel client:', err)
    })
    cli.once('ready', done)
  })

  test('setup sentinel pubsub', function (done) {
    var pub = _suite.clients.sentinelPub = _suite.createSentinelClient()
      sub =  _suite.clients.sentinelSub = _suite.createSentinelClient()

    var done2 = donen(2, done)

    sub.on('ready', function () {
      console.log('sub ready')
      sub.on('subscribe', function () { done2() })
      sub.subscribe(_suite.key('counter'))
    })
    pub.on('ready', function () {
      console.log('pub ready')
      done2()
    })
  })

  test('use sentinel client after ready', testSentinelClient)

  test('who is master', function (done) {
    _suite.clients.sentinel1.send_command('sentinel', ['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
      assert.ifError(err)
      assert.equal(bulk[1], ports.redis1)
      done()
    })
  })

  test('failover', function (done) {
    this.timeout(20000)
    setTimeout(onTimeout.bind(null, 0), 2000)

    var done2 = donen(2, done)
    _suite.clients.sentinelClient.once('failover start', done2.bind(null, null))
    _suite.clients.sentinelClient.once('failover end', done2.bind(null, null))

    function onTimeout(cnt) {
      _suite.clients.sentinel1.send_command('sentinel', ['failover', 'testmaster'], function (err) {
        if (cnt<9 && err && /NOGOODSLAVE/.test(err.toString())) {
          debug('Cant failover yet('+cnt+'): ', err.toString())
          return setTimeout(onTimeout.bind(null, cnt+1), 2000)
        }
        debug('redis failover started')
        assert.ifError(err)
      })
    }
  })

  test('who is master', function (done) {
    _suite.clients.sentinel1.send_command('sentinel', ['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
      assert.ifError(err)
      assert.equal(bulk[1], ports.redis2)
      done()
    })
  })

  test('use sentinel client after failover', testSentinelClient)

  test('configuration propogation', function (done) {
    this.timeout(4000)
    setTimeout(function () {
      _suite.clients.sentinel2.send_command('sentinel', ['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
        assert.ifError(err)
        assert.equal(bulk[1], ports.redis2)
        done()
      })
    }, 2000)
  })

  test('set sdown detection low', function (done) {
    _suite.clients.sentinel2.send_command('sentinel',['set', 'testmaster', 'down-after-milliseconds', 1000], done)
  })

  test('kill master', function (done) {
    this.timeout(30000)
    setTimeout(onTimeout, 4000)
    _suite.clients.sentinelClient.once('switch master', done)

    function onTimeout() {
      _suite.clients.redis2.end()
      _suite.processes.redis2.kill()
      delete(_suite.clients.redis2)
      delete(_suite.processes.redis2)

      debug('redis master killed')
    }
  })

  test('who is master', function (done) {
    _suite.clients.sentinel2.send_command('sentinel', ['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
      assert.ifError(err)
      assert.equal(bulk[1], ports.redis1)
      done()
    })
  })

  test('kill sentinel1', function (done) {
    this.timeout(10000)
    setTimeout(onTimeout, 1)

    function onTimeout() {
      _suite.clients.sentinel1.end()
      _suite.processes.sentinel1.kill()
      delete(_suite.clients.sentinel1)
      delete(_suite.processes.sentinel1)

      var done3 = donen(3, done)
      _suite.clients.sentinelClient.once('sentinel disconnected', done3)
      _suite.clients.sentinelClient.once('sentinel connect', done3.bind(null,null))

      _suite.clients.sentinelClient.once('sentinel connected', function (pair) {
        assert.equal(pair[1], ports.sentinel2)
        done3()
      })
    }
  })

  test('use sentinel client after sentinel kill', testSentinelClient)

  test('use SENTINEL command through client', function (done) {
    var cli = _suite.clients.sentinelClient

    cli.sentinel(['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
      assert.ifError(err)
      assert.equal(bulk[1], ports.redis1)
      done()
    })
  })

  after(function (done) {
    Object.keys(_suite.clients).forEach(function (cli) { _suite.clients[cli].end() })
    Object.keys(_suite.processes).forEach(function (proc) { _suite.processes[proc].kill() })
    setTimeout(done, 1000)
  })
})

process.on('uncaughtException', function (err) {
  Object.keys(_suite.clients).forEach(function (cli) { _suite.clients[cli].end() })
  Object.keys(_suite.processes).forEach(function (proc) { _suite.processes[proc].kill() })
})

function donen(n, done) {
  var cnt = 0
  return function (err) {
    cnt++
    if (err && cnt <= n) {
      cnt = n
      done(err)
    } else if (cnt === n) {
      done()
    }
  }
}

function testSentinelClient(done) {
  async.parallel([
    testGetSet.bind(null, _suite.clients.sentinelClient, _suite.key('getset')),
    testHmGetSet.bind(null, _suite.clients.sentinelClient, _suite.key('hmgetset')),
    testPubSub.bind(null, _suite.clients.sentinelPub, _suite.clients.sentinelSub)
  ], done)
}

function testGetSet(cli, key, cb) {
  var val = new Date().toString()
  cli.set(key, val, function (err) {
    assert.ifError(err)
    cli.get(key, function (err, rval) {
      assert.ifError(err)
      assert.equal(rval, val)
      cb()
    })
  })
}

function testHmGetSet(cli, key, cb) {
  var k1 = Date.now().toString()
    , v1 = new Date().toString()
    , obj = { 'other': 'things' }
  obj[k1] = v1

  debug('testing hmget and hmset at key', key, 'with value', obj)
  cli.hmset(key, obj, function (err) {
    assert.ifError(err)
    cli.hmget(key, k1, 'other', function (err, reply) {
      assert.ifError(err)
      debug('hmget response', reply, {k1:v1})
      assert.equal(reply[0], v1)
      assert.equal(reply[1], 'things')
      cb()
    })
  })
}

function testPubSub(pub, sub, cb) {
  var val = Date.now()
  sub.once('message', function (chan, sval) {
    assert.equal(chan, _suite.key('counter'))
    assert.equal(sval, val)
    cb()
  })
  pub.publish(_suite.key('counter'), val, assert.ifError)
}