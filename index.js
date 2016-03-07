'use strict';

var util = require('util'),
    assert = require('assert'),
    FlushWritable = require('flushwritable'),
    _ = require('lodash');

util.inherits(ElasticsearchBulkIndexWritable, FlushWritable);

/**
 * Transform records into a format required by Elasticsearch bulk API
 *
 * @private
 * @param {array} records
 * @return {array}
 */
function transformRecords(records) {
    return records.reduce(function(bulkOperations, record) {
        assert(record.index, 'index is required');
        assert(record.type, 'type is required');
        assert(record.body, 'body is required');

        bulkOperations.push({
            index: {
                _index: record.index,
                _type: record.type,
                _id: record.id
            }
        });

        bulkOperations.push(record.body);

        return bulkOperations;
    }, []);
}

/**
 * A simple wrapper around Elasticsearch for bulk writing items
 *
 * @param {Elasticsearch.Client} client Elasticsearch client
 * @param {Object} options Options
 * @param {number} [options.highWaterMark=16] Number of items to buffer before writing.
 * Also the size of the underlying stream buffer.
 * @param {number} [options.flushTimeout=null] Number of ms to flush records after, if highWaterMark hasn't been reached
 * @param {Object} [options.logger] Instance of a logger like bunyan or winston
 */
function ElasticsearchBulkIndexWritable(client, options) {
    assert(client, 'client is required');

    options = options || {};
    options.objectMode = true;

    FlushWritable.call(this, options);

    this.client = client;
    this.logger = options.logger || null;

    this.highWaterMark = options.highWaterMark || 16;
    this.flushTimeout = options.flushTimeout || null;
    this.writtenRecords = 0;
    this.queue = [];
}

/**
 * Bulk write records to Elasticsearch
 *
 * @private
 * @param {array} records
 * @param {Function} callback
 */
ElasticsearchBulkIndexWritable.prototype.bulkWrite = function bulkWrite(records, callback) {
    this.client.bulk({ body: records }, function bulkCallback(err, data) {
        if (err) {
            err.records = records;

            return callback(err);
        }

        if (data.errors === true) {
            var errors = data.items.map(function(item) {
                return _.pluck(item, 'error')[0];
            });

            errors = _.uniq(_.filter(errors, _.isString));

            if (this.logger) {
                errors.forEach(this.logger.error.bind(this.logger));
            }

            var error = new Error(errors);
            error.records = records;

            return callback(error);
        }

        callback();
    }.bind(this));
};

/**
 * Flush method needed by the underlying stream implementation
 *
 * @private
 * @param {Function} callback
 * @return {undefined}
 */
ElasticsearchBulkIndexWritable.prototype._flush = function _flush(callback) {
    if (this.queue.length === 0) {
        return callback();
    }

    try {
        var records = transformRecords(this.queue);
    } catch (error) {
        return callback(error);
    }

    if (this.logger) {
        this.logger.debug('Writing %d records to Elasticsearch', this.queue.length);
    }

    this.bulkWrite(records, function(err) {
        if (err) {
            return callback(err);
        }

        if (this.logger) {
            this.logger.info('Wrote %d records to Elasticsearch', this.queue.length);
        }

        this.writtenRecords += this.queue.length;
        this.queue = [];

        callback();
    }.bind(this));
};

/**
 * Write method needed by the underlying stream implementation
 *
 * @private
 * @param {Object} record
 * @param {string} enc
 * @param {Function} callback
 * @returns {undefined}
 */
ElasticsearchBulkIndexWritable.prototype._write = function _write(record, enc, callback) {
    if (this.logger) {
        this.logger.debug('Adding to Elasticsearch queue', { record: record });
    }
    var self = this;
    this.queue.push(record);

    if (this.queue.length >= this.highWaterMark) {
        return this._flush(callback);
    } else if (self.flushTimeout) {
        clearTimeout(this.timeoutId);
        self.timeoutId = setTimeout(function() {
            self._flush(function(err) {
                if (err) {
                    self.emit('error', err);
                }
            });
        }, self.flushTimeout);
    }

    callback();
};

module.exports = ElasticsearchBulkIndexWritable;
