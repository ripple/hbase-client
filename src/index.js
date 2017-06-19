const thrift = require('thrift')
const HBase = require('./gen/Hbase')
const HBaseTypes = require('./gen/Hbase_types')
const Logger = require('./logger')

/**
 * formatRows
 */

function formatRows(data, includeFamilies) {
  const rows = []
  data.forEach(function(row) {
    const r = {
      rowkey: row.row.toString('utf8'),
      columns: {}
    }

    let key
    let parts

    for (key in row.columns) {
      if (includeFamilies) {
        r.columns[key] = row.columns[key].value.toString('utf8')

      } else {
        parts = key.split(':')
        r.columns[parts[1]] = row.columns[key].value.toString('utf8')
      }
    }

    rows.push(r)
  })

  return rows
}

/**
 * prepareColumn
 * create a columnValue object
 * for the given column
 */

function prepareColumn(key, value) {
  let column

  const v = typeof value !== 'string' ?
    JSON.stringify(value) : value

  // default family to 'd' for data
  const name = key.split(':')
  column = name[1] ? name[0] : 'd'
  column += ':' + (name[1] ? name[1] : name[0])

  return new HBaseTypes.Mutation({
    column: column,
    value: v
  })
}

/**
 * prepareColumns
 * create an array of columnValue
 * objects for the given data
 */

function prepareColumns(data) {
  const columns = []
  let column
  let value

  for (column in data) {
    value = data[column]

    // ignore empty rows
    if (!value && value !== 0) {
      continue
    }

    columns.push(prepareColumn(column, value))
  }

  return columns
}

/**
 * HbaseClient
 * HBase client class
 */

function HbaseClient(options) {
  const self = this

  if (!options) {
    throw Error('initialization options required.')
  } else if (!options.host || !options.port) {
    throw Error('host and port required required.')
  }

  this.max_sockets = options.max_sockets || 5000
  this._prefix = options.prefix || ''
  this._servers = options.servers || null
  this._timeout = options.timeout || 30000 // also acts as keepalive
  this._connection = null
  this.hbase = null
  this.logStats = (!options.logLevel || options.logLevel > 3) ? true : false
  this.log = new Logger({
    scope: 'hbase-thrift',
    level: options.logLevel,
    file: options.logFile
  })

  this.pool = []

  if (!this._servers) {
    this._servers = [{
      host: options.host,
      port: options.port
    }]
  }

  // report the number of connections
  // every 60 seconds
  if (this.logStats) {
    setInterval(function() {
      self.log.debug('connections:' + self.pool.length)
    }, 60 * 1000)
  }
}

HbaseClient.prototype.query = function() {
  const self = this
  const args = Array.prototype.slice.call(arguments)
  const name = args.shift()
  let d = Date.now()

  function executeQuery(connection) {
    return new Promise((resolve, reject) => {

      function handleResponse(err, resp) {

        // log stats
        if (self.logStats) {
          d = (Date.now() - d) / 1000
          self.log.debug(name,
          'time:' + d + 's')
        }

        if (err) {
          reject(err)
        } else {
          resolve(resp)
        }


      }

      args.push(handleResponse)
      connection.client[name].apply(connection.client, args)
    })
  }

  return this._getConnection()
  .then(executeQuery)
}


/**
 * _getConnection
 * get an hbase connection from the pool
 */

HbaseClient.prototype._getConnection = function() {
  const self = this
  return new Promise((resolve, reject) => {

    const timer = setTimeout(() => {
      reject('unable to get open connection, ' +
        self.pool.length + ' of ' + self.max_sockets + ' in use')
    }, self._timeout)

    /**
     * handleNewConnectionError
     */

    function handleNewConnectionError(err) {
      this.error('error opening connection: ' + err)
      reject(err)
    }

    /**
     * onConnect
     */

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

      resolve(this)
    }

    /**
     * openNewSocket
     */

    function openNewSocket(i) {
      const server = self._servers[i || 0]

      // create new connection
      const connection = thrift.createConnection(server.host, server.port, {
        transport: thrift.TFramedTransport,
        protocol: thrift.TBinaryProtocol,
        timeout: self._timeout
      })


      // handle errors
      connection.error = function(err) {
        this.connected = false

        // execute any callbacks, then delete
        if (this.client) {
          for (const key in this.client._reqs) {
            this.client._reqs[key](err)
            delete (this.client._reqs[key])
          }
        }

        // destroy the connection
        this.connection.destroy()

        // remove from pool
        for (let j = 0; j < self.pool.length; j++) {
          if (self.pool[j] === this) {
            delete self.pool[j]
            self.pool.splice(j, 1)
            break
          }
        }
      }

      self.pool.push(connection)
      connection.once('error', handleNewConnectionError)
      connection.once('connect', onConnect)
      self.log.debug('# connections:', self.pool.length)
    }

    /**
     * getOpenConnection
     */

    function getConnection() {
      let i = self.pool.length

      // look for a free socket
      while (i--) {
        if (self.pool[i].client &&
            self.pool[i].connected &&
            Object.keys(self.pool[i].client._reqs).length < 1) {

          clearTimeout(timer)
          resolve(self.pool[i])
          // self.log.debug('# connections:', self.pool.length, '- current:', i)
          return
        }
      }

      // open a new socket if there is room in the pool
      if (self.pool.length < self.max_sockets) {
        openNewSocket(self.pool.length % self._servers.length)
      } else {
        setTimeout(getConnection, 10)
      }
    }

    getConnection()
  })
}


/**
 * getTables
 */

HbaseClient.prototype.getTables = function() {
  const self = this

  self.log.debug('getTables')
  return self.query('getTableNames')
  .then(resp => {
    return resp && resp.length
      ? resp.toString('utf8').split(',') : []
  })
}


/**
 * createTable
 */

HbaseClient.prototype.createTable = function(options) {
  const self = this
  const cf = []
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  if (options.columnFamilies) {
    options.columnFamilies.forEach(d => {
      cf.push(new HBaseTypes.ColumnDescriptor({
        name: d
      }))
    })
  }

  self.log.debug('createTable:', table)
  return self.query('createTable', table, cf)
}


/**
 * disableTable
 */

HbaseClient.prototype.disableTable = function(name) {
  const self = this

  self.log.debug('disableTable:', name)
  return self.query('disableTable', name)
  .catch(err => {
    if (err.message &&
        err.message.includes('TableNotEnabledException')) {
      self.log.info('table: ' + name + ' not enabled')

    } else {
      throw(err)
    }
  })
}


/**
 * deleteTable
 */

HbaseClient.prototype.deleteTable = function(name) {
  const self = this

  self.log.debug('deleteTable:', name)
  return self.query('deleteTable', name)
  .catch(err => {
    if (err.message &&
        err.message.includes('TableNotEnabledException')) {
      self.log.info('table: ' + name + ' not enabled')

    } else if (err.message &&
        err.message.includes('table does not exist')) {
      throw('table: \'' + name + '\' not found')

    } else {
      throw(err)
    }
  })
}


/**
 * getRow
 */

HbaseClient.prototype.getRow = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  function handleResponse(rows) {

    if (rows) {
      return formatRows(rows, options.includeFamilies)[0]
    }
  }

  if (options.columns) {
    self.log.debug('getRowWithColumns:', table, options.rowkey, options.columns)
    return self.query('getRowWithColumns',
                      table,
                      options.rowkey,
                      options.columns,
                      null)
    .then(handleResponse)
  } else {
    self.log.debug('getRow:', table, options.rowkey)
    return self.query('getRow',
                      table,
                      options.rowkey,
                      null)
    .then(handleResponse)
  }
}

/**
 * deleteColumns
 */

HbaseClient.prototype.deleteColumns = function(options) {
  const self = this

  return Promise.map(options.columns, function(d) {
    return self.deleteColumn({
      prefix: options.prefix,
      table: options.table,
      rowkey: options.rowkey,
      column: d
    })
  })
}

/**
 * deleteColumn
 */

HbaseClient.prototype.deleteColumn = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  self.log.debug('deleteColumn:', table, options.rowkey, options.column)
  return self.query('deleteAll',
                    table,
                    options.rowkey,
                    options.column,
                    null)
}

/**
 * putRows
 * upload multiple rows for a single
 * table into HBase
 */

HbaseClient.prototype.putRows = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table
  const data = []
  const list = []
  const chunkSize = 500
  let columns
  let rowkey

  // format rows
  for (rowkey in options.rows) {
    columns = prepareColumns(options.rows[rowkey])

    if (!columns.length) {
      continue
    }

    data.push(new HBaseTypes.BatchMutation({
      row: rowkey,
      mutations: columns
    }))
  }

  // only send it if we have data
  if (!data.length) {
    return Promise.resolve()
  }

  function putChunk(chunk) {
    self.log.debug('putRows:', table, chunk.length + ' rows')
    return self.query('mutateRows', table, chunk, null)
    .then(resp => {
      return chunk.length
    })
  }

  // chunk data at no more than 100 rows
  for (let i = 0, j = data.length; i < j; i += chunkSize) {
    list.push(putChunk(data.slice(i, i + chunkSize)))
  }

  return Promise.all(list)
}

/**
 * putRow
 * save a single row
 */

HbaseClient.prototype.putRow = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table
  const columns = prepareColumns(options.columns)

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }

  function removeEmpty() {
    let key

    if (!options.removeEmptyColumns) {
      return Promise.resolve()
    }

    const removed = []
    for (key in options.columns) {
      if (!options.columns[key] && options.columns[key] !== 0) {
        removed.push(key)
      }
    }

    return self.deleteColumns({
      prefix: options.prefix,
      table: options.table,
      rowkey: options.rowkey,
      columns: removed
    })
  }

  self.log.debug('putRow:', table, options.rowkey)
  return removeEmpty()
  .then(() => {
    return self.query('mutateRow', table, options.rowkey, columns, null)
  })
}

HbaseClient.prototype.getScan = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table
  const scanOpts = {}
  let limit = options.limit
  let d = Date.now()
  let swap

  if (limit && !options.excludeMarker) {
    limit += 1
  }

  if (options.marker && options.descending === true) {
    options.stopRow = options.marker
  } else if (options.marker) {
    options.startRow = options.marker
  }

  if (options.marker && options.descending === true) {
    options.stopRow = options.marker
  } else if (options.marker) {
    options.startRow = options.marker
  }

  // invert stop and start index
  if (options.descending === true) {
    scanOpts.stopRow = options.stopRow.toString()
    scanOpts.startRow = options.startRow.toString()
    scanOpts.reversed = true

    if (scanOpts.startRow < scanOpts.stopRow) {
      swap = scanOpts.startRow
      scanOpts.startRow = scanOpts.stopRow
      scanOpts.stopRow = swap
    }
  } else {
    scanOpts.startRow = options.stopRow.toString()
    scanOpts.stopRow = options.startRow.toString()

    if (scanOpts.startRow > scanOpts.stopRow) {
      swap = scanOpts.startRow
      scanOpts.startRow = scanOpts.stopRow
      scanOpts.stopRow = swap
    }
  }

  if (options.batchSize) {
    scanOpts.batchSize = options.batchSize
  }

  if (options.caching) {
    scanOpts.caching = options.caching
  }

  if (options.columns) {
    scanOpts.columns = options.columns
  }

  if (options.filterString && options.filterString !== '') {
    scanOpts.filterString = options.filterString
  }

  /**
   * getScan
   */

  function getScan() {
    const results = []
    let scan

    /**
     * getResults
     */

    function getResults(id) {
      const batchSize = 5000
      let page = 1
      let max



      return new Promise((resolve, reject) => {

        /**
         * recursiveGetResults
         */

        function recursiveGetResults() {
          let count

          if (limit) {
            count = Math.min(batchSize, limit - (page - 1) * batchSize)
            max = limit
          } else {
            max = Infinity
            count = batchSize
          }

          self.query('scannerGetList', id, count)
          .then(rows => {
            results.push(...formatRows(rows, options.includeFamilies))

            // recursively get more
            // results if we hit the
            // count and are under the limit
            if (rows.length === count &&
                page * batchSize < max) {
              page++
              setImmediate(recursiveGetResults)
              return
            }

            resolve(id)
          })
          .catch(reject)
        }

        // recursively get results
        recursiveGetResults()
      })
    }

    function closeScan(id) {
      self.log.debug('close scan:', id)
      self.query('scannerClose', id)
      .catch(e => {
        self.log.error('error closing scanner:', e)
      })
    }

    scan = new HBaseTypes.TScan(scanOpts)

    return self.query('scannerOpenWithScan', table, scan, null)
    .then(getResults)
    .then(closeScan)
    .then(() => {
      return results
    })
  }

  function handleResponse(rows) {
    self.log.debug('scan:', table, 'rows:' + rows.length)
    if (rows.length === limit &&
       !options.excludeMarker) {
      const marker = rows.pop().rowkey
      return {
        rows: rows,
        marker: marker
      }
    } else {
      return {
        rows: rows
      }
    }
  }

  return getScan()
  .then(handleResponse)
}


/**
 * deleteRow
 * delete a single row
 */

HbaseClient.prototype.deleteRow = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }

  self.log.debug('deleteRow:', table, options.rowkey)
  return self.query('deleteAllRow', table, options.rowkey, null)
}

/**
 * deleteRows
 * delete multiple rows
 * in a table
 */

HbaseClient.prototype.deleteRows = function(options) {
  const self = this
  const list = []

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkeys) {
    return Promise.reject('missing required parameter: rowkeys')
  }

  options.rowkeys.forEach(rowkey => {
    list.push(self.deleteRow({
      prefix: options.prefix,
      table: options.table,
      rowkey: rowkey
    }))
  })

  return Promise.all(list)
  .then(function(resp) {
    self.log.debug(options.table, 'rows removed:', resp.length)
    return resp.length
  })
}

/**
 * deleteColumns
 */

HbaseClient.prototype.deleteColumns = function(options) {
  const self = this
  const list = []

  options.columns.forEach(d => {
    list.push(self.deleteColumn({
      prefix: options.prefix,
      table: options.table,
      rowkey: options.rowkey,
      column: d
    }))
  })

  return Promise.all(list)
}

/**
 * deleteColumn
 */

HbaseClient.prototype.deleteColumn = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  self.log.debug('deleteColumn:', table, options.rowkey, options.column)
  return self.query('deleteAll', table, options.rowkey, options.column, null)
}


/*
 * buildSingleColumnValueFilters
 * helper to build column value filters
 */

HbaseClient.prototype.buildFilterString = function(filters) {
  const filterString = filters.map(function(o) {
    if (o.value && o.qualifier) {
      const filterMissing = o.filterMissing === false ? false : true
      const latest = o.latest === false ? false : true

      return [
        'SingleColumnValueFilter (\'',
        o.family, '\', \'',
        o.qualifier, '\', ',
        o.comparator, ', \'binary:',
        o.value, '\', ',
        filterMissing, ', ',
        latest, ')'
      ].join('')

    } else {
      return undefined
    }
  })
  .filter(n => {
    return n !== undefined
  })
  .join(' AND ')

  return filterString
}


module.exports = HbaseClient
