/* eslint prefer-spread: 1 */
const HbaseClientPool = require('./pool')
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
 * HbaseClient
 * HBase client class
 */

function HbaseClient(options) {
  this._prefix = options.prefix || ''
  this.logStats = (options.logLevel && options.logLevel > 3) ? true : false
  this.pool = new HbaseClientPool(options)
  this.log = new Logger({
    scope: 'hbase-client',
    level: options.logLevel,
    file: options.logFile
  })
}

HbaseClient.prototype._query = function(options) {
  const self = this
  
  return new Promise((resolve, reject) => {
    self.pool.acquire()
    .then(client => {

      client.client.on('error', err => {
        reject(Error('hbase client error: ' + err))
      }) 

      return client[options.name](options.params)
      .then(resp => {
        client.client.removeAllListeners()
        self.pool.release(client)
        return options.cb ? options.cb(resp) : undefined
      })
      .then(resolve)
    }) 
    .catch(reject)
  })
}

/**
 * getRow
 */

HbaseClient.prototype.getRow = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  return this._query({
    name: 'getRow',
    params: {
      table: table,
      rowkey: options.rowkey
    },
    cb: handleResponse
  })
  
  function handleResponse(row) {
    if (options.columns) {
      Object.keys(row.columns).forEach(c => {
        if (!options.columns.includes(c)) {
          delete row.columns[c]
        }
      })
    }
    return row ? formatRows([row], options.includeFamilies)[0] : undefined
  }
}

/**
 * getRows
 */

HbaseClient.prototype.getRows = function(options) {  
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table
  
  function handleResponse(rows) {
    let filtered = rows
    if (options.columns) {
      rows.forEach(row => {
        Object.keys(row.columns).forEach(c => {
          if (!options.columns.includes(c)) {
            delete row.columns[c]
          }
        })  
      })
      
      filtered = rows.filter(d => {
        return Boolean(Object.keys(d.columns).length)
      })
    }
    return filtered ? formatRows(filtered, options.includeFamilies) : []
  }

  return this._query({
    name: 'getRows',
    params: {
      table: table,
      rowkeys: options.rowkeys
    },
    cb: handleResponse
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
  const data = []
  const list = []
  const chunkSize = 500
  let columns
  let rowkey

  return this._query({
    name: 'putRows',
    params: {
      table: table, 
      rows: options.rows, 
      removeEmpty: options.removeEmptyColumns
    }
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

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }
  
  return this._query({
    name: 'putRow',
    params: {
      table: table,
      rowkey: options.rowkey,
      columns: options.columns,
      removeEmpty: options.removeEmptyColumns
    }
  })
}

HbaseClient.prototype.getScan = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table
  const scanOpts = {
    table: table,
    limit: options.limit,
    excludeMarker: options.excludeMarker,
    reversed: options.descending === true,
    filters: options.filters
  }
  
  let d = Date.now()
  let limit = options.limit
  let swap

  scanOpts.startRow = options.startRow ? 
    options.startRow.toString() : undefined
  scanOpts.stopRow = options.stopRow ? 
    options.stopRow.toString() : undefined
  
  if (options.marker) {
    scanOpts.startRow = options.marker.toString()
  }
  
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
  
  function handleResponse(resp) {
    let filtered = resp.rows
    if (options.columns) {
      resp.rows.forEach(row => {
        Object.keys(row.columns).forEach(c => {
          if (!options.columns.includes(c)) {
            delete row.columns[c]
          }
        })  
      })
      
      filtered = resp.rows.filter(d => {
        return Boolean(Object.keys(d.columns).length)
      })
    }
    
    return {
      rows: filtered ? formatRows(filtered, options.includeFamilies) : [],
      marker: resp.marker
    }
  }
  
  return this._query({
    name: 'getScan',
    params: scanOpts,
    cb: handleResponse
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

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }

  return this._query({
    name: 'delete',
    params: {
      table: table,
      rowkey: options.rowkey
    }
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
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }
  
  
  return this._query({
    name: 'delete',
    params: {
      table: table,
      rowkey: options.rowkey,
      columns: options.columns
    }
  })
}

/**
 * deleteColumn
 */

HbaseClient.prototype.deleteColumn = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }
  
  return this._query({
    name: 'delete',
    params: {
      table: table,
      rowkey: options.rowkey,
      columns: [options.column]
    }
  })
}

module.exports = HbaseClient
