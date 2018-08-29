const thrift = require('thrift')
const HBase = require('./gen/Hbase')
const HBaseTypes = require('./gen/Hbase_types')

const ACQUIRE_TIMEOUT = 2000;
const DEFAULT_TIMEOUT = 30000;
const DEFAULT_PORT = 9090;
const RETRY_INTERVAL = 10;
const DEFAULT_MAX = 50;
const DEFAULT_MIN = 10;

function client(options) {
  const self = this;

  this._timeout = options.timeout || DEFAULT_TIMEOUT;
  this._host = options.host;
  this._port = options.port || DEFAULT_PORT;
  this._MAX = options.max_sockets || DEFAULT_MAX;
  this._MIN = options.min_sockets || DEFAULT_MIN;
  this._connections = [];

  let count = 0;
  while(count++ < this._MIN) {
    createConnection();
  }

  this.getConnection = () => {
    return new Promise((resolve, reject) => {
      let timer;
      let acquire = setTimeout(() => {
        clearTimeout(timer);
        reject('thrift client resource timeout');
      }, ACQUIRE_TIMEOUT);

      function getConnection() {
        let found = false;
        self._connections.sort((a, b) => a.queries - b.queries);
        self._connections.every(d => {
          if (d.connected) {
            d.queries++;
            clearTimeout(acquire);
            resolve(d);
            found = true;
            return false;
          }

          return true;
        });


        if (self._connections.length < self._MIN ||
          (self._connections[0].queries > 20 && self._connections.length < self._MAX)) {
          createConnection();
        }

        if (!found) {
          timer = setTimeout(getConnection, RETRY_INTERVAL);
        }
      }

      getConnection();
    });
  }

  this.release = connection => {
    connection.queries--;
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

      // remove from pool
      for (var j = 0; j < self._connections.length; j++) {
        if (self._connections[j] === this) {
          delete self._connections[j];
          self._connections.splice(j, 1)
          break;
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

    connection.queries = 0;
    self._connections.push(connection);
  }
}

module.exports = client;