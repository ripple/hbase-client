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

  /**
   * create
   */

  function create() {
    return new Promise(function(resolve, reject) {
      const connection = thrift.createConnection(options.host, options.port, {
        transport: thrift.TFramedTransport,
        protocol: thrift.TBinaryProtocol,
        timeout: options.timeout || 30000
      })

      connection.once('connect', () => {
        connection.connection.setKeepAlive(true)
        connection.client = thrift.createClient(HBase, connection)
        connection._new = true
        resolve(connection)
      })

      connection.once('error', err => {
        reject(err)
      })

      connection.once('close', () => {
        reject(CLOSE_MESSAGE)
      })

      connection.once('timeout', () => {
        reject(TIMEOUT_MESSAGE)
      })
    })
  }

 /**
   * destroy
   */

  function destroy(connection) {
    connection.end()
  }

  const factory = {
    create: create,
    destroy: destroy
  }

  const params = {
    max: options.max_sockets || 1000,
    min: 5
  }

  const pool = genericPool.createPool(factory, params)

  /**
   * aquire
   */

  function acquire() {
    return pool.acquire()
    .then(connection => {

      function onError(e) {
        throw e
      }

      function onClose() {
        throw new Error(CLOSE_MESSAGE)
      }

      function onTimeout() {
        throw new Error(TIMEOUT_MESSAGE)
      }

      if (connection._new) {
        connection.on('error', onError)
        connection.on('timeout', onTimeout)
        connection.on('close', onClose)
        delete connection._new
      }

      return connection
    })
  }

  /**
   * release
   */

  function release(connection) {
    pool.release(connection)
  }

  return {
    acquire: acquire,
    release: release
  }
}

module.exports = HbaseClient
