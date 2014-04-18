/**
Redis Sentinel client, add-on for node_redis client.
See readme for details/usage.
*/

var RedisSingleClient = require('redis'),
    events = require('events'),
    util = require('util'),
    reply_to_object = require('redis/lib/util.js').reply_to_object,
    to_array = require('redis/lib/to_array.js'),
    commands = RedisSingleClient.commands;


/*
options includes:
- host
- port
- masterOptions
- masterName
- logger (e.g. winston)
- debug (boolean)
*/
function RedisSentinelClient(options) {

  // RedisClient takes (stream,options). we don't want stream, make sure only one.
  if (arguments.length > 1) {
    throw new Error("Sentinel client takes only options to initialize");
  }

  // make this an EventEmitter (also below)
  events.EventEmitter.call(this);

  var self = this;

  this.options = options = options || {};

  this.options.masterName = this.options.masterName || 'mymaster';

  // no socket support for now (b/c need multiple connections).
  if (options.port == null || options.host == null) {
    throw new Error("Sentinel client needs a host and port");
  }


  // if debugging is enabled for sentinel client, enable master client's too.
  // (standard client just uses console.log, not the 'logger' passed here.)
  if (options.master_debug) {
    RedisSingleClient.debug_mode = true;
  }

  var masterOptions = self.masterOptions = options.masterOptions || {};
  masterOptions.disable_flush = true; // Disables flush_and_error, to preserve queue

  // if master & slaves need a password to authenticate,
  // pass it in as 'master_auth_pass'.
  // (corresponds w/ 'auth_pass' for normal client,
  // but differentiating b/c we're not authenticating to the *sentinel*, rather to the master/slaves.)
  // by setting it to 'auth_pass' on master client, it should authenticate to the master (& slaves on failover).
  // note, sentinel daemon's conf needs to know this same password too/separately.
  masterOptions.auth_pass = options.master_auth_pass || masterOptions.auth_pass;

  // used for logging & errors
  this.myName = 'sentinel-' + this.options.host + ':' + this.options.port + '-' + this.options.masterName;

  this.on('error', function (err) {
    this.log('redis master error: ', err)
  })

  /*
  what a failover looks like:
  - master fires ECONNREFUSED errors a few times
  - sentinel listener gets:
    +sdown
    +odown
    +try-failover
    +failover-state-wait-start
    +failover-state-select-slave
    +selected-slave
    +failover-state-send-slaveof-noone
    +failover-state-wait-promotion
    +promoted-slave
    +failover-state-reconf-slaves
    +slave-reconf-sent
    +slave-reconf-inprog
    +slave-reconf-done
    +failover-end
    +switch-master

  (see docs @ http://redis.io/topics/sentinel)

  note, these messages don't specify WHICH master is down.
  so if a sentinel is listening to multiple masters, and we have a RedisSentinelClient
  for each sentinel:master relationship, every client will be notified of every master's failovers.
  But that's fine, b/c reconnect() checks if it actually changed, and does nothing if not.
  */

  // one client to query ('talker'), one client to subscribe ('listener').
  // these are standard redis clients.
  // talker is used by reconnect() below
  this.sentinelTalker = new RedisSingleClient.createClient(options.port, options.host);
  this.sentinelTalker.on('connect', function(){
    self.debug('connected to sentinel talker');
  });
  this.sentinelTalker.on('error', function(error){
    error.message = self.myName + " talker error: " + error.message;
    self.onError.call(self, error);
  });
  this.sentinelTalker.on('end', function(){
    self.debug('sentinel talker disconnected');
    // @todo emit something?
    // @todo does it automatically reconnect? (supposed to)
  });

  var sentinelListener = new RedisSingleClient.createClient(options.port, options.host);
  this.sentinelListener = sentinelListener;
  sentinelListener.on('connect', function(){
    self.debug('connected to sentinel listener');
  });
  sentinelListener.on('error', function(error){
    error.message = self.myName + " listener error: " + error.message;
    self.onError(error);
  });
  sentinelListener.on('end', function(){
    self.debug('sentinel listener disconnected');
    // @todo emit something?
  });

  // Connect on load
  this.reconnect();

  // Subscribe to all messages
  sentinelListener.psubscribe('*');

  sentinelListener.on('pmessage', function(channel, msg) {
    self.debug('sentinel message', msg);

    // pass up, in case app wants to respond
    self.emit('sentinel message', msg);

    switch(msg) {
      case '+sdown':
        self.debug('Down detected');
        self.emit('down-start');
        break;

      case '+try-failover':
        self.debug('Failover detected');
        self.emit('failover-start');
        break;

      case '+switch-master':
        self.debug('Reconnect triggered by ' + msg);
        self.emit('failover-end');
        self.reconnect();
        break;
    }
  });

}

util.inherits(RedisSentinelClient, events.EventEmitter);


// [re]connect activeMasterClient to the master.
// destroys the previous connection and replaces w/ a new one,
// (transparently to the user)
// but only if host+port have changed.
RedisSentinelClient.prototype.reconnect = function reconnect() {
  var self = this;
  self.debug('reconnecting');

  self.sentinelTalker.send_command("SENTINEL", ["get-master-addr-by-name", self.options.masterName], function(error, newMaster) {
    if (error) {
      error.message = self.myName + " Error getting master: " + error.message;
      self.onError(error);
      return;
    }
    try {
      newMaster = {
        host: newMaster[0],
        port: newMaster[1]
      };
      self.debug('new master info', newMaster);
      if (!newMaster.host || !newMaster.port) throw new Error("Missing host or port");
    }
    catch(error) {
      error.message = self.myName + ' Unable to reconnect master: ' + error.message;
      self.onError(error);
    }

    if (self.activeMasterClient &&
        newMaster.host === self.activeMasterClient.host &&
        newMaster.port === self.activeMasterClient.port) {
      self.debug('Master has not changed, nothing to do');
      return;
    }


    self.debug("Changing master from " +
      (self.activeMasterClient ? self.activeMasterClient.host + ":" + self.activeMasterClient.port : "[none]") +
      " to " + newMaster.host + ":" + newMaster.port);

    self._connect(newMaster.port, newMaster.host);
  });
};

RedisSentinelClient.prototype._connect = function (port, host) {
  var self = this

  // this client will always be connected to the active master.
  // using 9999 as initial; expected to fail; is replaced & re-connected to real port later.
  var thisClient = self.activeMasterClient = new RedisSingleClient.createClient(port, host, self.masterOptions);

  // pass up messages
  ['message', 'pmessage', 'unsubscribe', 'end', 'reconnecting', 'connect', 'ready', 'error'].forEach(function (evt) {
    self.activeMasterClient.on(evt, function () {
      if (self.activeMasterClient == thisClient)
        self.emit.apply(self, [evt].concat(Array.prototype.slice.call(arguments)))
    })
  })

  // @todo use no_ready_check = true? then change this 'ready' to 'connect'

  self.once(self.masterOptions.no_ready_check ? 'connect' : 'ready', function(){
    self.debug('New master is ready');
    // anything outside holding a ref to activeMasterClient needs to listen to this,
    // and refresh its reference. pass the new master so it's easier.
    self.emit('reconnected', self.activeMasterClient);
  });

};

//
// pass thru all client commands from RedisSentinelClient to activeMasterClient
//
RedisSentinelClient.prototype.send_command = function (command, args, callback) {
  // this ref needs to be totally atomic
  var client = this.activeMasterClient;
  return client.send_command.apply(client, arguments);
};

// adapted from index.js for RedisClient
commands.forEach(function (command) {
  RedisSentinelClient.prototype[command.toUpperCase()] =
  RedisSentinelClient.prototype[command] = function (args, callback) {
    var sentinel = this;

    sentinel.debug('command', command, args);

    if (Array.isArray(args) && typeof callback === "function") {
      return sentinel.send_command(command, args, callback);
    } else {
      return sentinel.send_command(command, to_array(arguments));
    }
  };
});

// automagically handle multi & exec?
// (tests will tell...)

// this multi is on the master client, so don't hold onto it too long!
var Multi = RedisSingleClient.Multi;

// @todo make a SentinelMulti that queues within the sentinel client?
//  would need to handle all Multi.prototype methods, etc.
//  for now let multi's queue die if the master dies.

RedisSentinelClient.prototype.multi =
RedisSentinelClient.prototype.MULTI = function (args) {
  return new Multi(this.activeMasterClient, args);
};

['hmget', 'hmset', 'done'].forEach(function(staticProp){
  RedisSentinelClient.prototype[staticProp] =
  RedisSentinelClient.prototype[staticProp.toUpperCase()] = function(){
    var client = this.activeMasterClient;
    return client[staticProp].apply(client, arguments);
  };
});

// helper to get client.
// (even tho activeMasterClient is public, this is clearer)
RedisSentinelClient.prototype.getMaster = function getMaster() {
  return this.activeMasterClient;
};

RedisSentinelClient.prototype.getSentinel = function () {
  return this.sentinelTalker
};

// commands that must be passed through to the sentinel Redises as well as the active master
[ 'quit', 'end', 'unref' ].forEach(function(staticProp) {
  RedisSentinelClient.prototype[staticProp] =
  RedisSentinelClient.prototype[staticProp.toUpperCase()] = function(){
    this.sentinelTalker[staticProp].apply(this.sentinelTalker, arguments);
    this.sentinelListener[staticProp].apply(this.sentinelListener, arguments);
    return this.activeMasterClient[staticProp].apply(this.activeMasterClient, arguments);
  };
});


// get static values from client, also pass-thru
// not all of them... @review!
[ 'connection_id', 'ready', 'connected', 'connections', 'commands_sent', 'connect_timeout',
  'monitoring', 'closing', 'server_info',
  'stream' /* ?? */
  ].forEach(function(staticProp){
    RedisSentinelClient.prototype.__defineGetter__(staticProp, function(){
      if (this.activeMasterClient) {
        return this.activeMasterClient[staticProp];
      } else {
        return null;
      }
    });

    // might as well have a setter too...?
    RedisSentinelClient.prototype.__defineSetter__(staticProp, function(newVal){
      return this.activeMasterClient[staticProp] = newVal;
    });
  });


// log(type, msgs...)
// type is 'info', 'error', 'debug' etc
RedisSentinelClient.prototype.log = function log() {
  var logger = this.options.logger || console;
  var args = Array.prototype.slice.call(arguments);
  logger.log.apply(logger, args);
};
// debug(msgs...)
RedisSentinelClient.prototype.debug = function debug() {
  if (this.options.debug) {
    var args = ['debug'].concat(Array.prototype.slice.call(arguments));
    this.log.apply(this, args);
  }
};


exports.RedisSentinelClient = RedisSentinelClient;



// called by RedisClient::createClient() when options.sentinel===true
// similar args for backwards compat,
// but does not support sockets (see above)
exports.createClient = function (port, host, options) {
  // allow the arg structure of RedisClient, but collapse into options for constructor.
  //
  // note, no default_host or default_port,
  // see http://redis.io/topics/sentinel.
  // also no net_client or allowNoSocket, see above.
  // this could be a problem w/ backwards compatibility.
  if (arguments.length === 1) {
    options = arguments[0] || {};
  }
  else {
    options = options || {};
    options.port = port;
    options.host = host;
  }

  return new RedisSentinelClient(options);
};
