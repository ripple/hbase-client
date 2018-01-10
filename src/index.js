/* eslint prefer-spread: 1 */
const HbaseClientPool = require('./pool')
const HBaseTypes = require('./gen/Hbase_types')
const Logger = require('./logger')

const CLOSE_MESSAGE = 'HBASE client connection closed'
const TIMEOUT_MESSAGE = 'Hbase client timeout'


function removeEmpty(connection, options) {  
  let key
  
  if (!options.remove) {
    return Promise.resolve()
  }

  const list = [] 
  
  Object.keys(options.rows).forEach(key => {
    list.push(removeEmptyColumns(key, options.rows[key]))
  })
  
  return Promise.all(list)
  
  
  function removeEmptyColumns(rowkey, columns) {
    const removed = []
    
    for (key in columns) {
      if (!columns[key] && columns[key] !== 0) {
        removed.push(key)
      }
    }
    
    if (!removed.length) {
      return Promise.resolve()  
    }
    
    return connection.delete({
      table: options.table,
      rowkey: rowkey,
      columns: removed
    })   
  }
}

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


/**
 * getRow
 */

HbaseClient.prototype.getRow = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table

  return self.pool.acquire()
  .then(connection => {
    return connection.getRow({
      table: table,
      rowkey: options.rowkey
    }).then(resp => {
      self.pool.release(connection)
      return handleResponse(resp)
    })
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

  return self.pool.acquire()
  .then(connection => {
    return connection.getRows({
      table: table,
      rowkeys: options.rowkeys
    }).then(resp => {
      self.pool.release(connection)
      return handleResponse(resp)
    })
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

  return self.pool.acquire()
  .then(connection => {
    return removeEmpty(connection, {
      table: table, 
      rows: options.rows, 
      remove: options.removeEmptyColumns
    })
    .then(() => {
      return connection.putRows({
        table: table,
        rows: options.rows
      }).then(() => {
        self.pool.release(connection)
      })    
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

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }
  
  return self.pool.acquire()
  .then(connection => {
    const rows = {}
    rows[options.rowkey] = options.columns
    return removeEmpty(connection, {
      table: table, 
      rows: rows, 
      remove: options.removeEmptyColumns
    })
    .then(() => {
      return connection.putRow({
        table: table,
        rowkey: options.rowkey,
        columns: options.columns
      }).then(() => {
        self.pool.release(connection)
      })  
    })
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
    reversed: options.descending === true
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
  
/*  
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
    
    if (scanOpts.startRow > scanOpts.stopRow) {
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
  */
  
  
  return self.pool.acquire()
  .then(connection => {
    return connection.getScan(scanOpts)
    .then(resp => {
      self.pool.release(connection)
      return handleResponse(resp)
    })
  })
  
  
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

  return self.pool.acquire()
  .then(connection => {
    return connection.delete({
      table: table,
      rowkey: options.rowkey
    })
    .then(resp => {
      self.pool.release(connection)
    })
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
  
  return self.pool.acquire()
  .then(connection => {
    return connection.delete({
      table: table,
      rowkey: options.rowkey,
      columns: options.columns
    })
    .then(resp => {
      self.pool.release(connection)
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

  if (!options.table) {
    return Promise.reject('missing required parameter: table')
  } else if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }
  
  return self.pool.acquire()
  .then(connection => {
    return connection.delete({
      table: table,
      rowkey: options.rowkey,
      columns: [options.column]
    })
    .then(resp => {
      self.pool.release(connection)
    })
  })  
}

module.exports = HbaseClient
