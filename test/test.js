const mock = require('./mock.json')
const assert = require('assert')
const Hbase = require('../src/index.js')

const HbaseRest = require('hbase')

const hbase = new Hbase({
  hosts: ['hbase'],
  root: '/hbase',
  prefix: 'prefix',
  logLevel: 3,
  max_sockets: 10,
  min_sockets: 10,
  timeout: 5000
})

describe('hbase client', function() {

  before(function(done) {
    this.timeout(5000)
    
    const rest = HbaseRest({ host: 'hbase', port: 8080 })
    
    rest.table('prefixtest').delete((err) => {
      if (err && err.code !== 404) {
        assert.ifError(err)
      }
      
      rest.table('prefixtest').create({
        ColumnSchema: [
          { name: 'f'},
          { name: 'd'}
        ]
      }, (err, resp) => {
        assert.ifError(err)
        done()       
      })
    })
  })

  
  it('should handle client error', function() {    
    const hb = new Hbase({
      hosts: ['hbase'],
      root: '/hbase',
      prefix: 'prefix',
      logLevel: 3,
      min_sockets: 1,
      timeout: 1
    })
    
    return hb.getRow({
      table: mock.row.table,
      rowkey: mock.row.rowkey
    }) 
    .then(assert)
    .catch(err => {
      assert.strictEqual(err.toString(), 'Failed to connect to zookeeper. zkHosts: ' + 
                         '[hbase] zkRoot: \'/hbase\'')
    })
  })
  
  it('should handle resource timeout error', function() {    
    const hb = new Hbase({
      hosts: ['hbase'],
      root: '/hbase',
      prefix: 'prefix',
      logLevel: 3,
      min_sockets: 10,
      max_sockets: 10,
      timeout: 100
    })
    
    let i = 30
    const list = []
    while (i--) {
      list.push(hb.getScan({
        table: 'test',
        startRow: 'A',
        stopRow: 'Z',
        descending: true
      }))
    }

    return Promise.all(list)
    .then(assert)
    .catch(err => {
      assert.strictEqual(err.toString(), 'TimeoutError: ResourceRequest timed out')
    })
  })  

  it('should save a single row', function() {
    return hbase.putRow(mock.row)
    .then(() => {
      return hbase.getRow({
        table: mock.row.table,
        rowkey: mock.row.rowkey
      })      
    }).then(row => {
      assert.strictEqual(row.rowkey, mock.row.rowkey)
      assert.deepEqual(row.columns, mock.row.columns)
    })    
  })

  it('should save a single row with column families', function() {
    return hbase.putRow(mock.rowWithColumnFamilies)
    .then(() => {
      return hbase.getRow({
        table: mock.rowWithColumnFamilies.table,
        rowkey: mock.rowWithColumnFamilies.rowkey,
        includeFamilies: true
      })      
    }).then(row => {
      assert.strictEqual(row.rowkey, mock.rowWithColumnFamilies.rowkey)
      assert.deepEqual(row.columns, mock.rowWithColumnFamilies.columns)
    })
  })

  it('should save multiple rows', function() {
    const keys = Object.keys(mock.rows.rows)
    let attempts = 0
    
    return hbase.putRows(mock.rows)
    .then(count => {
      assert.strictEqual(count, keys.length)
      return getRows()
    })
  
    function getRows() {
      return hbase.getRows({
        table: mock.rows.table,
        rowkeys: keys
      })
      .then(rows => {
        assert(attempts++ < 100, 'too many attempts')
        
        if (rows[0].rowkey !== 'ROW|3') {
          return getRows()   
        }
        
        assert.strictEqual(rows.length, keys.length)
        assert.strictEqual(rows[0].columns.column0, mock.rows.rows['ROW|3'].column0.toString())
        assert.strictEqual(rows[0].columns.column1, mock.rows.rows['ROW|3'].column1.toString())
        assert.strictEqual(rows[0].columns.column2, mock.rows.rows['ROW|3'].column2)
        assert.strictEqual(rows[0].columns['columnJSON'], 
                           JSON.stringify(mock.rows.rows['ROW|3'].columnJSON))
      })      
    }
  })

  it('should get a row by key', function() {
    return hbase.getRow({
      table: mock.row.table,
      rowkey: mock.row.rowkey
    })
    .then(row => {
      assert.strictEqual(row.rowkey, mock.row.rowkey)
      assert.deepEqual(row.columns, mock.row.columns)
    })
  })

  it('should get a row by key with specific columns', function() {
    return hbase.getRow({
      table: mock.row.table,
      rowkey: mock.row.rowkey,
      columns: ['d:baz']
    })
    .then(row => {
      assert.strictEqual(row.rowkey, mock.row.rowkey)
      assert.deepEqual(row.columns, {
        baz: mock.row.columns.baz
      })
    })
  })

  it('should get a row by key with column families', function() {
    return hbase.getRow({
      table: mock.rowWithColumnFamilies.table,
      rowkey: mock.rowWithColumnFamilies.rowkey,
      includeFamilies: true
    })
    .then(row => {
      assert.strictEqual(row.rowkey, mock.rowWithColumnFamilies.rowkey)
      assert.deepEqual(row.columns, mock.rowWithColumnFamilies.columns)
    })
  })
  
  
  it('should get multiple rows by key', function() {
    const rowkeys = [
      'ROW|1',
      'ROW|2',
      'ROW|3',
      'ROW|4'
    ]
    
    return hbase.getRows({
      table: mock.row.table,
      rowkeys: rowkeys
    })
    .then(rows => {
      assert.strictEqual(rows.length, 4)
      rowkeys.forEach((key,i) => {
        assert.strictEqual(rows[i].rowkey, key)
      })
    })
  })

  it('should get multiple rows by key with column families', function() {
    return hbase.getRows({
      table: mock.row.table,
      rowkeys: [
        'ROW|1',
        'ROW|2',
        'ROW|3',
        'ROW|4'
      ],
      includeFamilies: true
    })
    .then(rows => {
      assert.strictEqual(rows.length, 4)
      assert.strictEqual(rows[3].columns['d:column5'], '5')
    })
  })

  it('should get multiple rows by key with specific columns', function() {
    return hbase.getRows({
      table: mock.row.table,
      rowkeys: [
        'ROW|1',
        'ROW|2',
        'ROW|3',
        'ROW|4'
      ],
      columns: ['d:foo']
    })
    .then(rows => {
      assert.strictEqual(rows.length, 2)
    })
  })


  it('should get rows by scan', function() {
    return hbase.getScan({
      table: 'test'
    }).then(resp => {
      assert.strictEqual(resp.rows.length, 6)
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|1')
    })
  })

  it('should get rows by scan with limit', function() {
    return hbase.getScan({
      table: 'test',
      limit: 2
    }).then(resp => {
      assert.strictEqual(resp.rows.length, 2)
      assert.strictEqual(resp.marker, 'ROW|3')
    })
  })
  
  it('should get rows by scan with start row', function() {
    return hbase.getScan({
      table: 'test',
      startRow: 'ROW|2'
    }).then(resp => {
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|2')
      assert.strictEqual(resp.rows.length, 5)
      assert.strictEqual(resp.marker, undefined)
    })
  })  
  
  it('should get rows by scan with stop row', function() {
    return hbase.getScan({
      table: 'test',
      stopRow: 'ROW|4'
    }).then(resp => {
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|1')
      assert.strictEqual(resp.rows.length, 3)
      assert.strictEqual(resp.marker, undefined)
    })
  })  
  
  it('should get rows by scan with start row and stop row', function() {
    return hbase.getScan({
      table: 'test',
      startRow: 'ROW|3',
      stopRow: 'ROW|5'
    }).then(resp => {
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|3')
      assert.strictEqual(resp.rows.length, 2)
      assert.strictEqual(resp.marker, undefined)
    })
  })  
  
  it('should get rows by scan with start row and stop row (switch)', function() {
    return hbase.getScan({
      table: 'test',
      startRow: 'ROW|5',
      stopRow: 'ROW|3'
    }).then(resp => {
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|3')
      assert.strictEqual(resp.rows.length, 2)
      assert.strictEqual(resp.marker, undefined)
    })
  })    

  it('should get rows by scan with marker', function() {
    return hbase.getScan({
      table: 'test',
      limit: 1,
      marker: 'ROW|3'
    }).then(resp => {
      assert.strictEqual(resp.rows.length, 1)
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|3')
      assert.strictEqual(resp.marker, 'ROW|4')
    })
  })
  
  it('should get rows by scan with start row and stop row and marker', function() {
    return hbase.getScan({
      table: 'test',
      startRow: 'ROW|2',
      stopRow: 'ROW|6',
      marker: 'ROW|4'
    }).then(resp => {
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|4')
      assert.strictEqual(resp.rows.length, 2)
      assert.strictEqual(resp.marker, undefined)
    })
  })    

  it('should get rows by scan (reversed)', function() {
    return hbase.getScan({
      table: 'test',
      descending: true
    }).then(resp => {
      assert.strictEqual(resp.rows.length, 6)
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|6')
    })
  })
  
  it('should get rows by scan (reversed) with limit', function() {
    return hbase.getScan({
      table: 'test',
      descending: true,
      limit: 2
    }).then(resp => {
      assert.strictEqual(resp.rows.length, 2)
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|6')
      assert.strictEqual(resp.marker, 'ROW|4')
      
      return hbase.getScan({
        table: 'test',
        descending: true,
        limit: 2,
        marker: 'ROW|4'
      }).then(resp => {
        assert.strictEqual(resp.rows.length, 2)
        assert.strictEqual(resp.rows[0].rowkey, 'ROW|4')
        assert.strictEqual(resp.marker, 'ROW|2')
      })      
    })
  })  
  
  it('should get rows by scan with startRow (reversed)', function() {
    return hbase.getScan({
      table: 'test',
      descending: true,
      startRow: 'ROW|1',
      stopRow: 'ROW|6'
    }).then(resp => {
      assert.strictEqual(resp.rows.length, 5)
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|6')
    })
  })  
  
  it('should get rows by scan with startRow (reversed) with limit', function() {
    return hbase.getScan({
      table: 'test',
      descending: true,
      startRow: 'ROW|1',
      stopRow: 'ROW|5',
      limit: 1,
    }).then(resp => {
      assert.strictEqual(resp.rows.length, 1)
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|5')
      assert.strictEqual(resp.marker, 'ROW|4')
      
      return hbase.getScan({
        table: 'test',
        descending: true,
        startRow: 'ROW|1',
        stopRow: 'ROW|5',
        marker: 'ROW|4',
        limit: 1,
      }).then(resp => {
        assert.strictEqual(resp.rows.length, 1)
        assert.strictEqual(resp.rows[0].rowkey, 'ROW|4')
        assert.strictEqual(resp.marker, 'ROW|3')    
      })
  
        return hbase.getScan({
          table: 'test',
          descending: true,
          startRow: 'ROW|1',
          stopRow: 'ROW|5',
          marker: 'ROW|3',
          limit: 1,
        }).then(resp => {
          assert.strictEqual(resp.rows.length, 1)
          assert.strictEqual(resp.rows[0].rowkey, 'ROW|3')
          assert.strictEqual(resp.marker, 'ROW|2')    
        })      
    })
  }) 
  
  it('should get rows by scan with start row and stop row (switch, reverse)', function() {
    return hbase.getScan({
      table: 'test',
      startRow: 'ROW|3',
      stopRow: 'ROW|5',
      descending: true
    }).then(resp => {
      assert.strictEqual(resp.rows[0].rowkey, 'ROW|5')
      assert.strictEqual(resp.rows.length, 2)
      assert.strictEqual(resp.marker, undefined)
    })
  })   
  
  it('should get rows by scan with filters', function() {
    return hbase.getScan({
      table: 'test',
      filters: [mock.filter1]
    }).then(resp => {      
      const column = mock.filter1.qualifier
      const value = mock.filter1.value
      
      assert.strictEqual(resp.rows.length, 2)
      assert.strictEqual(resp.rows[0].columns[column], value)
      assert.strictEqual(resp.rows[1].columns[column], value)
    })
  })
  
  it.skip('should get rows by scan with family filter', function() {
    return hbase.getScan({
      table: 'test',
      filters: [mock.filter2]
    })
  }) 
  
  it('should get rows by scan with FirstKeyOnlyFilter and KeyOnlyFilter', function() {
    return hbase.getScan({
      table: 'test',
      filters: [
        {type: 'FirstKeyOnlyFilter'},
        {type: 'KeyOnlyFilter'}
      ]
    }).then(resp => {  
      assert.strictEqual(resp.rows.length, 6)
      resp.rows.forEach(row => {
        const keys = Object.keys(row.columns) 
        assert.strictEqual(keys.length, 1)
        assert.strictEqual(row.columns[keys[0]], '')
      })
    })
  }) 
  
  it('should get rows by scan with DependentColumnFilter', function() {
    return hbase.getScan({
      table: 'test',
      filters: [{
        type: 'DependentColumnFilter',
        family: 'd',
        qualifier: 'column5'
      }]
    }).then(resp => {  
      assert.strictEqual(resp.rows.length, 3)
      resp.rows.forEach(row => {
        assert.strictEqual(Boolean(row.columns.column5), true)
      })
    })
  })   
  
  it('should ignore an empty filter array', function() {
    return hbase.getScan({
      table: 'test',
      filters: []
    }).then(resp => {   
      assert.strictEqual(resp.rows.length, 6)
    })
  })    

  it('should do a bunch of scans, puts, and gets', function() {
    this.timeout(7000)
    let i = 300
    const list = []
    while (i--) {

      list.push(hbase.putRow(mock.row))
      list.push(hbase.getRow({
        table: mock.row.table,
        rowkey: mock.row.rowkey
      }))
      
      list.push(hbase.getRows({
        table: mock.rows.table,
        rowkeys: Object.keys(mock.rows.rows)
      }))

      list.push(hbase.getScan({
        table: 'test',
        startRow: 'A',
        stopRow: 'Z',
        descending: true
      }))
    }

    return Promise.all(list)
  })

  it('should delete a column', function() {
    return hbase.deleteColumn({
      table: 'test',
      rowkey: 'ROW|1',
      column: 'd:foo'
    }).then(() => {
      return hbase.getRow({
        table: 'test',
        rowkey: 'ROW|1'
      })
      .then(row => {
        assert.strictEqual(row.columns.foo, undefined)  
      })
    })
  })

  it('should delete columns', function() {
  
    
    return hbase.deleteColumns({
      table: 'test',
      rowkey: mock.rowWithColumnFamilies.rowkey,
      columns: Object.keys(mock.rowWithColumnFamilies.columns)
    })
    .then(() => {
      return hbase.getRow({
        table: 'test',
        rowkey: mock.rowWithColumnFamilies.rowkey
      })
      .then(row => {
        assert.strictEqual(row, undefined)  
      })      
    })
  })

  it('should delete a row', function() {
    return hbase.deleteRow({
      table: 'test',
      rowkey: 'ROW|1'
    })
  })

  it('should delete rows', function() {
    return hbase.deleteRows({
      table: 'test',
      rowkeys: ['ROW|2', 'ROW|3']
    })
  })

  it('should save a single row while removing empty columns', function() {
    mock.row.removeEmptyColumns = true
    mock.row.columns.foo = ''
 
    return hbase.putRow(mock.row)
    .then(() => {
      return hbase.getRow({
        table: mock.row.table,
        rowkey: mock.row.rowkey
      })
      .then(row => {
        assert.strictEqual(row.columns.foo, undefined)
        assert.strictEqual(row.columns.baz, 'foo')
      })
    })   
  }) 
  
  it('should save multiple rows while removing empty columns', function() {
    mock.rows.removeEmptyColumns = true
    mock.rows.rows['ROW|3'].column1 = ''
    mock.rows.rows['ROW|4'].column6 = ''
 
    return hbase.putRows(mock.rows)
    .then(count => {
      assert.strictEqual(count, Object.keys(mock.rows.rows).length)
      return hbase.getRows({
        table: mock.rows.table,
        rowkeys: ['ROW|3', 'ROW|4']
      })
      .then(rows => {
        assert.strictEqual(rows[0].columns.column1, undefined)
        assert.strictEqual(rows[0].columns.column2, 'two')
        assert.strictEqual(rows[1].columns.column6, undefined)
        assert.strictEqual(rows[1].columns.column5, '5')
      })
    })   
  }) 

  it('getRows should not error with an empty rowset', function() {
    return hbase.getRows({
      table: mock.rows.table,
      rowkeys: []
    })
    .then(rows => {
      assert.strictEqual(rows.length, 0)
    })
  }) 
  
  it('putRows should not error with an empty rowset', function() {
    return hbase.putRows({
      table: mock.rows.table,
      rows: {}
    })
    .then(count => {
      assert.strictEqual(count, 0)
    })
  })  
  
  it('putRows should not error with an prefix only', function() {
    return hbase.putRows({
      prefix: 'prefixtest',
      table: '',
      rows: {}
    })
    .then(count => {
      assert.strictEqual(count, 0)
    })
  })    
})
