const thrift = require('thrift')
const HBase = require('./gen/Hbase')
const HBaseTypes = require('./gen/Hbase_types')

const ACQUIRE_TIMEOUT = 2000;
const DEFAULT_TIMEOUT = 30000;
const DEFAULT_PORT = 9090;
const RETRY_INTERVAL = 20;

function client(options) {
  const self = this;

  this._timeout = options.timeout || DEFAULT_TIMEOUT;
  this._host = options.host;
  this._port = options.port || DEFAULT_PORT;
  this._connection;

  this.getConnection = () => {
    return new Promise((resolve, reject) => {
      let timer;
      let acquire = setTimeout(() => {
        clearTimeout(timer);
        reject('thrift client resource timeout');
      }, ACQUIRE_TIMEOUT);

      function getConnection() {
        if (!self.connection || self.connection.closed) {
          createConnection();

        } else if (self.connection && self.connection.connected) {
          clearTimeout(acquire);
          resolve(self.connection);
          return;
        }

        timer = setTimeout(getConnection, RETRY_INTERVAL);
      }

      getConnection();
    });
  }

  function createConnection() {
    const timer = setTimeout(() => {
      connection.error('thrift client resource timeout');
    }, ACQUIRE_TIMEOUT);

    const connection = thrift.createConnection(self._host, self._port, {
      transport: thrift.TFramedTransport,
      protocol: thrift.TBinaryProtocol,
      timeout: self._timeout,
    });

    // handle errors
    connection.error = function(err) {
      // destroy the connection
      this.connection.destroy()
      this.closed = true;

      // execute any callbacks, then delete
      if (this.client) {
        for (var key in this.client._reqs) {
          this.client._reqs[key](err)
        }
      }
    };

    connection.on('timeout', function() {
      this.error('thrift client connection timeout')
    });

    connection.on('close', function() {
      this.error('thrift connection closed')
    });

    connection.on('error', function(err) {
      this.error('thrift connection error: ' + err)
    });

    connection.once('connect', function() {
      clearTimeout(timer);
      this.client = thrift.createClient(HBase, this)
    });

    self.connection = connection;
  }
}

module.exports = client;