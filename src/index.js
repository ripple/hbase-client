const thrift = require('thrift')
const Logger = require('./logger')
const genericPool = require('generic-pool')
const HBase = require('./gen/Hbase')
const HBaseTypes = require('./gen/Hbase_types')

const TIMEOUT_MESSAGE = 'HBase client timeout'
const CLOSE_MESSAGE = 'HBase client connection closed'

function addFilters(filters) {
  const list = []

  if (!filters || !filters.length) {
    return
  }

  filters.forEach(filter => {

    if (filter.type === 'FamilyFilter') {
      list.push('FamilyFilter (=, \'binary:' + filter.family + '\')')

    } else if (filter.type === 'FirstKeyOnlyFilter') {
      list.push('FirstKeyOnlyFilter()')

    } else if (filter.type === 'KeyOnlyFilter') {
      list.push('KeyOnlyFilter()')

    } else if (filter.type === 'DependentColumnFilter') {
      list.push('DependentColumnFilter(\'' +
                filter.family + '\', \'' +
                filter.qualifier + '\')')

    } else if (filter.type) {
      throw Error('invalid filter: ' + filter.type)

    } else {
      list.push(addSingleColumnValueFilter(filter))
    }
  })

  return list.join(' AND ')

  function addSingleColumnValueFilter(d) {
    const filterMissing = d.filterMissing === false ? false : true
    const latest = d.latest === false ? false : true

    return [
      'SingleColumnValueFilter (\'',
      d.family, '\', \'',
      d.qualifier, '\', ',
      d.comparator, ', \'binary:',
      d.value, '\', ',
      filterMissing, ', ',
      latest, ')'
    ].join('')
  }
}

/**
 * formatRows
 */

function formatRows(data, includeFamilies) {
  const rows = []
  for (let i=0; i<data.length; i++) {
    const r = {
      rowkey: data[i].row.toString('utf8'),
      columns: {}
    }

    let key
    let parts

    for (key in data[i].columns) {
      if (includeFamilies) {
        r.columns[key] = data[i].columns[key].value.toString('utf8')

      } else {
        parts = key.split(':')
        r.columns[parts[1]] = data[i].columns[key].value.toString('utf8')
      }
    }

    rows.push(r)
  }

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
  this._prefix = options.prefix || ''
  this.logStats = (options.logLevel && options.logLevel > 3) ? true : false

  this.log = new Logger({
    scope: 'hbase-client',
    level: options.logLevel,
    file: options.logFile
  })

  const servers = options.servers || []

  if (!servers.length) {
    servers.push({
      host: options.host,
      port: options.port || 9090
    })
  }

  const factory = {
    create: function() {
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
          reject(err)
        })

        connection.on('close', () => {
          reject(CLOSE_MESSAGE)
        })

        connection.on('timeout', () => {
          reject(TIMEOUT_MESSAGE)
        })
      })
    },
    destroy: client => {}
  }

  const params = {
    testOnBorrow: false,
    max: options.max_sockets || 100,
    min: options.min_sockets || 5,
    acquireTimeoutMillis: options.timeout || 30000,
    idleTimeoutMillis: options.timeout || 30000
  }

  this.pool = genericPool.createPool(factory, params)
}

HbaseClient.prototype.acquire = function(reject) {
  return this.pool.acquire()
  .then(client => {
    client.on('error', reject)
    client.on('timeout', reject.bind(this, TIMEOUT_MESSAGE))
    client.on('close', reject.bind(this, CLOSE_MESSAGE))
    return client
  })
}

HbaseClient.prototype.release = function(client) {
  client.removeAllListeners()
  this.pool.release(client)
}

/**
 * getRow
 */

HbaseClient.prototype.getRow = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(connection => {

      if (options.columns) {
        self.log.debug('getRowWithColumns:', table, options.rowkey, options.columns)
        connection.client.getRowWithColumns(
                          table,
                          options.rowkey,
                          options.columns,
                          null,
                          handleResponse)

      } else {
        self.log.debug('getRow:', table, options.rowkey)
        connection.client.getRow(
                          table,
                          options.rowkey,
                          null,
                          handleResponse)
      }

      function handleResponse(err, rows) {
        self.release(connection)

        if (err) {
          reject(err)
        }

        resolve(rows ?
          formatRows(rows, options.includeFamilies)[0] : undefined)
      }
    })
    .catch(reject)
  })
}

/**
 * getRows
 */

HbaseClient.prototype.getRows = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(connection => {
      let d = Date.now()

      function handleResponse(err, rows) {
        self.release(connection)

        if (err) {
          reject(err)
        }

        d = ((Date.now() - d)/1000) + 's'
        self.log.info('query:getRows',
                      'table:' + table,
                      'time:' + d,
                      'rowcount:' + rows.length)

        resolve(rows ? formatRows(rows, options.includeFamilies) : [])
      }

      if (options.columns) {
        return connection.client.getRowsWithColumns(
                  table,
                  options.rowkeys,
                  options.columns,
                  null,
                  handleResponse)
      } else {
        return connection.client.getRows(
                  table,
                  options.rowkeys,
                  null,
                  handleResponse)
      }
    })
    .catch(reject)
  })
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
  const rows = []
  let columns

  // format rows
  for (rowkey in options.rows) {
    columns = prepareColumns(options.rows[rowkey])

    if (!columns.length) {
      continue
    }

    rows.push(new HBaseTypes.BatchMutation({
      row: rowkey,
      mutations: columns
    }))
  }

  if (!rows.length) {
    return Promise.resolve(0)
  }


  return self._removeEmptyColumns({
    prefix: options.prefix,
    table: options.table,
    rows: options.rows
  }, !options.removeEmptyColumns)
  .then(() => {
    return new Promise((resolve, reject) => {
      self.acquire(reject)
      .then(connection => {
        connection.client.mutateRows(table, rows, null, function(err, resp) {
          self.release(connection)

          if (err) {
            return reject(err)
          }

          resolve(rows.length)

        })
      })
      .catch(reject)
    })
  })
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

  const rows = {}

  if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }


  rows[options.rowkey] = options.columns


  return self._removeEmptyColumns({
    prefix: options.prefix,
    table: options.table,
    rows: rows
  }, !options.removeEmptyColumns)
  .then(() => {
    return new Promise((resolve, reject) => {
      self.acquire(reject)
      .then(connection => {
        connection.client.mutateRow(
              table,
              options.rowkey,
              columns,
              null,
              handleResponse)

        function handleResponse(err, resp) {
          self.release(connection)

          if (err) {
            reject(err)
          }

          resolve(resp)
        }
      })
      .catch(reject)
    })
  })
}

HbaseClient.prototype.getScan = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table
  const scanOpts = {
    reversed: options.descending === true,
    filterString: addFilters(options.filters)
  }

  let limit = Number(options.limit)
  let swap

  if (limit && !options.excludeMarker) {
    limit += 1
  }

  scanOpts.startRow = options.startRow ?
    options.startRow.toString() : undefined
  scanOpts.stopRow = options.stopRow ?
    options.stopRow.toString() : undefined

  if (scanOpts.reversed &&
      scanOpts.startRow < scanOpts.stopRow) {
    swap = scanOpts.startRow
    scanOpts.startRow = scanOpts.stopRow
    scanOpts.stopRow = swap

  } else if (!scanOpts.reversed &&
             scanOpts.startRow > scanOpts.stopRow) {
    swap = scanOpts.startRow
    scanOpts.startRow = scanOpts.stopRow
    scanOpts.stopRow = swap
  }

  if (options.columns) {
    scanOpts.columns = options.columns
  }

  if (options.marker) {
    scanOpts.startRow = options.marker.toString()
  }


  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(connection => {
      const scan = new HBaseTypes.TScan(scanOpts)
      const results = []
      let d = Date.now()

      function handleResponse(rows) {
        self.release(connection)
        d = ((Date.now() - d)/1000) + 's'
        self.log.info('query:getScan',
                      'table:' + table,
                      'time:' + d,
                      'rowcount:' + rows.length)

        if (rows.length === limit &&
           !options.excludeMarker) {
          const marker = rows.pop().rowkey

          resolve({
            rows: rows,
            marker: marker
          })

        } else {
          resolve({
            rows: rows
          })
        }
      }

      function getResults(id) {
        const batchSize = 1000
        let page = 1
        let max

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

          connection.client.scannerGetList(id, count, (err, rows) => {
            if (err) {
              reject(err)
              return
            }

            results.push(...formatRows(rows, options.includeFamilies))

            // recursively get more
            // results if we hit the
            // count and are under the limit
            if (rows.length === count &&
                page * batchSize < max) {
              page++
              setImmediate(recursiveGetResults)

            } else {
              connection.client.scannerClose(id, err => {
                if (err) {
                  self.log.error('error closing scanner:', err)
                }
              })

              handleResponse(results)
            }
          })
        }

        // recursively get results
        recursiveGetResults()
      }

      connection.client.scannerOpenWithScan(table, scan, null, function(err, resp) {
        if (err) {
          reject(err)
        } else {
          getResults(resp)
        }
      })
    })
    .catch(reject)
  })
}

/**
 * deleteRow
 * delete a single row
 */

HbaseClient.prototype.deleteRow = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }

  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(connection => {

      function handleResponse(err, resp) {
        self.release(connection)

        if (err) {
          reject(err)
        }

        resolve(resp)
      }

      connection.client.deleteAllRow(table,
                                    options.rowkey,
                                    null,
                                    handleResponse)
    })
    .catch(reject)
  })
}

/**
 * deleteRows
 * delete multiple rows
 * in a table
 */

HbaseClient.prototype.deleteRows = function(options) {
  const self = this
  const list = []

  if (!options.rowkeys) {
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
  let column = options.column.split(':')

  if (column.length === 1) {
    column = 'd:' + column[0]
  } else {
    column = column.join(':')
  }

  if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }

  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(connection => {

      function handleResponse(err, resp) {
        self.release(connection)

        if (err) {
          reject(err)
        }

        resolve(resp)
      }

      connection.client.deleteAll(table,
                                  options.rowkey,
                                  column,
                                  null,
                                  handleResponse)
    })
    .catch(reject)
  })
}

HbaseClient.prototype._delete = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table
  const del = new hbase.Delete(options.rowkey)

  if (options.columns) {
    options.columns.forEach(c => {
      const parts = c.split(':')
      const family = parts[1] ? parts[0] : 'd'
      const qualifier = parts[1] ? parts[1] : parts[0]
      del.deleteColumns(family, qualifier)
    })
  }

  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(client => {
      client.delete(table, del, function(err, resp) {
        self.release(client)
        if (err) {
          return reject(err)
        }

        resolve()
      })
    })
    .catch(reject)
  })
}


HbaseClient.prototype._removeEmptyColumns = function(options, ignore) {
  const self = this
  const list = []
  let key

  if (ignore) {
    return Promise.resolve()
  }

  Object.keys(options.rows).forEach(rowkey => {
    const removed = []

    for (key in options.rows[rowkey]) {
      if (!options.rows[rowkey][key] &&
          options.rows[rowkey][key] !== 0) {
        removed.push(key)
      }
    }

    if (removed.length) {
      list.push(self.deleteColumns({
        prefix: options.prefix,
        table: options.table,
        rowkey: rowkey,
        columns: removed
      }))
    }
  })

  return Promise.all(list)
}

module.exports = HbaseClient
