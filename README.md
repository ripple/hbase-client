## Ripple Hbase Client

#### Usage:
```
const Hbase = require('ripple-hbase-client')
const hbase = new Hbase({
  host: 'localhost',
  port: 9090,
  prefix: 'prefix',
  logLevel: 2
})

hbase.putRow({
  table: 'table',
  rowkey: 'rowkey',
  columns: {
    column1: '1'
    'f:column2': 2
  }
})
.then(console.log)

hbase.getRow({
  table: 'table',
  rowkey: 'rowkey'
})
.then(console.log)

```
