const thrift = require('thrift')
const HBase = require('./gen/Hbase')
const HBaseTypes = require('./gen/Hbase_types')

const ACQUIRE_TIMEOUT = 5000;
const DEFAULT_TIMEOUT = 30000;
const DEFAULT_PORT = 9090;
const DEFAULT_MAX_SOCKETS = 1;

function client(options) {
  this._timeout = options.timeout || DEFAULT_TIMEOUT;
  this._host = options.host;
  this._port = options.port || 9090;
  this._connection;

  this.getConnection = () => {
    const self = this;

    return new Promise((resolve, reject) => {
      let timer;
      let acquire = setTimeout(() => {
        clearTimeout(timer);
        reject('thrift client resource timeout');
      }, ACQUIRE_TIMEOUT);

      function getConnection() {
        if (self.connection &&
            self.connection.connected &&
           Object.keys(self.connection.client._reqs).length < 2000) {
          clearTimeout(acquire);
          resolve(self.connection);
          return;
        }

        // wait for connect
        if (self.connection) {
          timer = setTimeout(getConnection, 20);
          return;
        }

        createConnection()
        .then(connection => {
          clearTimeout(acquire);
          resolve(connection);
        })
        .catch(reject);
      }

      getConnection();
    });

    function createConnection() {
      return new Promise(function(resolve, reject) {

        const connection = thrift.createConnection(self._host, self._port, {
          transport: thrift.TFramedTransport,
          protocol: thrift.TBinaryProtocol,
          timeout: self._timeout
        });

        // handle errors
        connection.error = function(err) {
          this.connected = false

          // execute any callbacks, then delete
          if (this.client) {
            for (var key in this.client._reqs) {
              this.client._reqs[key](err)
              delete (this.client._reqs[key])
            }
          }

          // destroy the connection
          this.connection.destroy()
          delete self.connection;
        };

        self.connection = connection;
        connection.once('error', handleNewConnectionError)
        connection.once('connect', onConnect)

        function handleNewConnectionError(err) {
          reject('error opening connection: ' + err)
          clearTimeout(acquire);
        }


        function onConnect() {
          this.removeListener('error', handleNewConnectionError)
          this.client = thrift.createClient(HBase, this)

          this.on('timeout', function() {
            this.error('thrift client connection timeout')
          })

          this.on('close', function() {
            this.error('hbase connection closed')
          })

          this.on('error', function(err) {
            this.error('thrift connection error: ' + err)
          })

          resolve(this);
        }
      });
    }
  }
}

module.exports = client;