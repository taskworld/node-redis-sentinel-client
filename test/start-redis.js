var spawn = require('child_process').spawn
  , fs = require('fs')
  , debug = require('debug')('redis-processes')

exports.redis = function (conf, port, slaveof, pass) {
  var args = [conf]
  fs.writeFileSync(conf, configureRedis(port, slaveof, pass))
  return spawnRedis(args, 'redis:' + port)
}

exports.sentinel = function (conf, port, monitor, password) {
  var args = [conf, '--sentinel']
  fs.writeFileSync(conf, configureSentinel(port,monitor,password))
  return spawnRedis(args, 'sentinel:'+port)
}

function spawnRedis(args, name) {
  var proc = spawn('redis-server', args)

  proc.stderr.setEncoding('utf-8')
  proc.stderr.on('data', function (data) {
    console.log('REDIS ' + name + ':', data)
  })

  proc.stdout.setEncoding('utf-8')
  proc.stdout.on('data', function (data) {
    debug('REDIS ' + name + ':', data)
  })


  return proc
}

function configureRedis(port, slaveof, password) {
  return [
    'port %PORT'.replace('%PORT', port),
    slaveof ? 'slaveof 127.0.0.1 %SLOF'.replace('%SLOF',slaveof) : '#',
    'requirepass %PASS'.replace('%PASS', password),
    'masterauth %PASS'.replace('%PASS', password),
  ].join('\n')
}

function configureSentinel(port, monitor, password) {
  return [
    'port %PORT'.replace('%PORT', port),
    'sentinel monitor testmaster 127.0.0.1 %MONITOR 1'.replace('%MONITOR',monitor),
    'sentinel parallel-syncs testmaster 2',
    'sentinel auth-pass testmaster %PASSWORD'.replace('%PASSWORD',password)
  ].join('\n')
}