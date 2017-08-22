const thrift = require('thrift')
const genericPool = require('generic-pool')
const HBase = require('./gen/Hbase')

const CLOSE_MESSAGE = 'HBASE client connection closed'
const TIMEOUT_MESSAGE = 'Hbase client timeout'

/**
 * HbaseClient
 * HBase client class
 */

function HbaseClient(options) {
  const servers = options.servers || []

  if (!servers.length) {
    servers.push({
      host: options.host,
      port: options.port
    })
  }

  /**
   * create
   */

  function create() {
    return new Promise(function(resolve, reject) {
      const i = Math.floor(Math.random() * servers.length)
      const server = servers[i]
      const connection = thrift.createConnection(server.host, server.port, {
        transport: thrift.TFramedTransport,
        protocol: thrift.TBinaryProtocol,
        timeout: options.timeout || 30000
      })

      connection.once('connect', () => {
        connection.connection.setKeepAlive(true)
        connection.client = thrift.createClient(HBase, connection)
        resolve(connection)
      })

      connection.on('error', err => {
        connection._ended = true
        reject(err)
      })

      connection.on('close', () => {
        connection._ended = true
        reject(CLOSE_MESSAGE)
      })

      connection.on('timeout', () => {
        connection._ended = true
        reject(TIMEOUT_MESSAGE)
      })
    })
  }

 /**
   * destroy
   */

  function destroy(connection) {
    console.log('destroy')
    connection.end()
  }

  /**
   * validate
   */

  function validate(connection) {
    return !connection._ended
  }

  const factory = {
    create: create,
    destroy: destroy,
    validate: validate
  }

  const params = {
    testOnBorrow: true,
    max: options.max_connections || 1000,
    min: options.min_connections || 5,
    acquireTimeoutMillis: options.timeout || 30000,
    idleTimeoutMillis: options.timeout || 30000
  }

  return genericPool.createPool(factory, params)
}

module.exports = HbaseClient
