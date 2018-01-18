const Logger = require('./logger')
const hbase = require('hbase-rpc-client')
const genericPool = require('generic-pool')
const TIMEOUT_MESSAGE = 'HBase client timeout'

function addFilters(scan, filters) {
  
  if (!filters.length) {
    return  
  }
  
  const list = new hbase.FilterList()

  filters.forEach(filter => {
    const comp = filter.comparator === '=' ?
      'EQUAL' : (filter.comparator || 'EQUAL')
          
    if (filter.type === 'FamilyFilter') {
    /*
      list.addFilter({
        FamilyFilter: {
          compareFilter: {
            compareOp: 'EQUAL',
          }
        }
      })
    */
    } else if (filter.type === 'FirstKeyOnlyFilter') {
      list.addFilter({
        firstKeyOnlyFilter: {}
      })
      
    } else if (filter.type === 'KeyOnlyFilter') {
      list.addFilter({
        KeyOnlyFilter: {
          lenAsVal: false
        }
      })  
      
    } else if (filter.type === 'DependentColumnFilter') {
      list.addFilter({
        DependentColumnFilter: {
          compareFilter: {
            compareOp: 'EQUAL'
          },
          columnFamily: filter.family,
          columnQualifier: filter.qualifier
        }
      })        
      
    } else if (filter.type) {
      throw Error('invalid filter: ' + filter.type)  
    
    } else {    

      list.addFilter({
        singleColumnValueFilter: {
          columnFamily: filter.family,
          columnQualifier: filter.qualifier,
          compareOp: comp,
          comparator: {
            SubstringComparator: {
              substr: filter.value
            }
          },
          filterIfMissing: filter.latest === false ? false : true,
          latestVersionOnly: filter.latest === false ? false : true
        }
      })   
    }
  }) 
  
  scan.setFilter(list)
}

function makePut(options) {
  const put = new hbase.Put(options.rowkey)

  Object.keys(options.columns).forEach(key => {
    const parts = key.split(':')
    const family = parts[1] ? parts[0] : 'd'
    const qualifier = parts[1] ? parts[1] : parts[0]
    
    let value = options.columns[key]
    
    // stringify JSON and arrays and convert numbers to string
    if (typeof value !== 'string') {
      value = JSON.stringify(value)
    }
    
    if (value) {
      put.add(family, qualifier, value)
    }
  })
  
  return put
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
  
  this.log = new Logger({
    scope: 'hbase-client',
    level: options.logLevel,
    file: options.logFile
  })
  
  const factory = {
    create: function() {
      return new hbase({
        zookeeperHosts: options.hosts,
        zookeeperRoot: options.root,
        zookeeperReconnectTimeout: (options.timeout || 30000) * 1.1,
        rpcTimeout: (options.timeout || 30000) * 1.1,
        callTimeout: (options.timeout || 30000) * 1.1,
        tcpNoDelay: true,
        tcpKeepAlive: true
      })
    },
    destroy: client => {
      console.log('destroy')
    }
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
    client.on('timeout', reject.bind(this, 'HBase connection timeout'))
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
  const get = new hbase.Get(options.rowkey)

  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(client => {
      client.get(table, get, function(err, resp) { 
        self.release(client)
        
        const row = {}

        if (err) {
          return reject(err)
        }

        if (!resp) {
          return resolve(undefined)
        }

        resolve({
          row: resp.row,
          columns: resp.cols
        })
      })    
    })
    .catch(reject)
  }).then(handleResponse)    
  
  function handleResponse(row) {
    if (row && options.columns) {
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
  const gets = options.rowkeys.map(rowkey => {
    return new hbase.Get(rowkey)
  })
  
  if (!gets.length) {
    return Promise.resolve([])
  }
  
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

  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(client => {
      client.mget(table, gets, function(err, resp) {  
        self.release(client)
        
        const rows = []

        if (err) {
          return reject(err)
        }

        resp.forEach(row => {
          if (row.row) {
            rows.push({
              row: row.row,
              columns: row.cols
            })
          }
        })

        resolve(rows)
      })  
    })
    .catch(reject)
  }).then(handleResponse)
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

  Object.keys(options.rows).forEach(key => {
    rows.push(makePut({
      rowkey: key,
      columns: options.rows[key]
    }))
  })
  
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
      .then(client => {
        client.mput(table, rows, function(err, resp) {  
          self.release(client)
          
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
  const put = makePut({
    rowkey: options.rowkey, 
    columns: options.columns
  })

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
      .then(client => {
        client.put(table, put, function(err, resp) {  
          self.release(client)
          
          if (err) {
            return reject(err)
          }

          resolve()
        })
      }) 
      .catch(reject)
    })
  })
}

HbaseClient.prototype.getScan = function(options) {
  const self = this
  const prefix = options.prefix || self._prefix
  const table = prefix + options.table
  const limit = Number(options.limit)
  const scanOpts = {
    reversed: options.descending === true,
  }
  
  let d = Date.now()
  let swap

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
  
  if (options.marker) {
    scanOpts.startRow = options.marker.toString()
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
  
  return new Promise((resolve, reject) => {
    self.acquire(reject)
    .then(client => {
      
      function done(err, data) {
        scan.close()
        self.release(client)
        
        if (err) {
          reject(err)
        } else {
          resolve(data)
        }     
      }
      
      const scan = client.getScanner(table, 
                                          scanOpts.startRow, 
                                          scanOpts.stopRow)
      if (scanOpts.reversed) {
        scan.setReversed()

      }

      if (options.filters) {
        addFilters(scan, options.filters)
      }

      const rows = []
      getNext()

      function getNext(getMarker) {
        scan.next((err, row) => {           
          if (err) {
            return done(err)
          }

          if (!row.row || getMarker) {
            done(null, {
              rows: rows,
              marker: row.row ? row.row.toString('utf8') : undefined
            })
            return
          }        

          rows.push({
            row: row.row,
            columns: row.cols
          })


          if (rows.length === limit
             && options.excludeMarker) {
            done(null, {
              rows: rows
            })  

          } else if (rows.length === limit) {
            getNext(true)  

          } else {
            getNext()
          }
        })
      }
    })
    .catch(reject)
  })
  .then(handleResponse)
}

/**
 * deleteRow
 * delete a single row
 */

HbaseClient.prototype.deleteRow = function(options) {
  
  if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }

  return this._delete({
    prefix: options.prefix,
    table: options.table,
    rowkey: options.rowkey
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

  if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }
  
  
  return this._delete({
    prefix: options.prefix,
    table: options.table,
    rowkey: options.rowkey,
    columns: options.columns
  })
}

/**
 * deleteColumn
 */

HbaseClient.prototype.deleteColumn = function(options) {
  
  if (!options.rowkey) {
    return Promise.reject('missing required parameter: rowkey')
  }
  
  return this._delete({
    prefix: options.prefix,
    table: options.table,
    rowkey: options.rowkey,
    columns: [options.column]
  })
  
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
    self.client.delete(options.table, del, function(err, resp) {               
      if (err) {
        return reject(err)
      }

      resolve()
    })      
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
  let key

  if (ignore) {
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

    return self._delete({
      prefix: options.prefix,
      table: options.table,
      rowkey: rowkey,
      columns: removed
    })   
  }
} 

module.exports = HbaseClient
