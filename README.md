# elasticsearch-writable-stream

A writable stream for doing operations in Elasticsearch with support
for bulk actions. Supports virtually all indexing operations including
index, update, update_by_query, and delete.

This module used to be known as [elasticsearch-bulk-index-stream](https://www.npmjs.com/package/elasticsearch-bulk-index-stream),
but was renamed because the package has added support for non-bulk actions.

[![build status](https://travis-ci.org/voldern/elasticsearch-writable-stream.svg)](https://travis-ci.org/voldern/elasticsearch-writable-stream)
[![modules status](https://david-dm.org/voldern/elasticsearch-writable-stream.svg)](https://david-dm.org/voldern/elasticsearch-writable-stream)

[![npm badge](https://nodei.co/npm/elasticsearch-writable-stream.png?downloads=true)](https://nodei.co/npm/elasticsearch-writable-stream)

# Usage

## Format

The records written to the stream has to have the following format:
```javascript
{
  index: 'name-of-index',
  type: 'recordType',
  id: 'recordId',
  parent: 'parentRecordType', // optional
  action: 'update', // optional (default: 'index')
  body: {
    name: 'Foo Bar'
  }
}
```

## Buffering

The `highWaterMark` option set on the stream defines how many items
will be buffered before doing a bulk operation. The stream will also
write all buffered items if its is closed, before emitting the
`finish` event.

The `update_by_query` action bypasses the buffer and gets executed at
once since its not supported by the bulk API.

## Flushing

Its also possible to send in the option `flushTimeout` to indicate
that the items currently in the buffer should be flushed after the
given amount of milliseconds if the `highWaterMark` haven't been
reached.

## Logging

A [bunyan](https://www.npmjs.com/package/bunyan),
[winston](https://www.npmjs.com/package/winston) or similar logger
instance that have methods like `debug`, `error` and `info` may be
sent in as `options.logger` to the constructor.

# Example

```javascript
var ElasticsearchWritableStream = require('elasticsearch-writable-stream');

var stream = new ElasticsearchWritableStream(elasticsearchClient, {
  highWaterMark: 256,
  flushTimeout: 500
});

someInputStream
  .pipe(stream)
  .on('error', function(error) {
    // Handle error
  })
  .on('finish', function() {
    // Clean up Elasticsearch client?
  })
```

# API

See [api.md](api.md).

# See

- [elasticsearch-streams](https://www.npmjs.com/package/elasticsearch-streams)

Elasticsearch readable and writable streams. The main difference
between the bulk writer in `elasticsearch-streams` and this library is
that this library requires the `index` and `type` of the data being
written to exist in the record instead of being set in a callback when
the records written.

`elasticsearch-streams` also implements its own event named `close` to
indicate that all the data has been written to Elasticsearch. This
will break modules like [pump](https://www.npmjs.com/package/pump)
that depend on the `finish` event.

# License

MIT
