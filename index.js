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
        assert(record.id, 'id is required');
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
 * @param {Number} [options.highWaterMark] Number of items to buffer before writing.
 * Also the size of the underlying stream buffer.
 * @param {Object} [options.logger] Instance of a logger like bunyan or winston
 */
function ElasticsearchBulkIndexWritable(client, options) {
    assert(client, 'client is required');

    options = options || {};
    options.objectMode = true;

    FlushWritable.call(this, options);

    this.client = client;
    this.logger = options.logger || null;

    this.highWaterMark = options.highWaterMark || 64;
    this.writtenRecords = 0;
    this.queue = [];
}

/**
 * Write items in queue to Elasticsearch
 *
 * @private
 * @param {Function} callback
 * @return {undefined}
 */
ElasticsearchBulkIndexWritable.prototype._flush = function _flush(callback) {
    try {
        var records = transformRecords(this.queue);
    } catch (error) {
        return callback(error);
    }

    if (this.logger) {
        this.logger.debug('Writing %d records to Elasticsearch', this.queue.length);
    }

    this.client.bulk({ body: records }, function bulkCallback(err, data) {
        if (err) {
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

            return callback(new Error(errors));
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

    this.queue.push(record);

    if (this.queue.length >= this.highWaterMark) {
        return this._flush(callback);
    }

    callback();
};

module.exports = ElasticsearchBulkIndexWritable;
