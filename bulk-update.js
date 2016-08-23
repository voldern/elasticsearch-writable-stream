'use strict';

var util = require('util'),
    assert = require('assert'),
    Writable = require('stream').Writable,
    _ = require('lodash');

util.inherits(ElasticsearchPartialUpdateWritable, Writable);

/**
 * Transform records into a format required by Elasticsearch bulk API
 *
 * @private
 * @param {array} records
 * @return {array}
 */
function transformUpdate(record) {
    assert(record.index, 'index is required');
    assert(record.type, 'type is required');
    assert(record.body, 'body is required');
    assert(record.body.script, 'body.script is required');
    assert(record.body.query, 'body.query is required');

    return {
        index: record.index,
        type: record.type,
        body: {
            script: record.body.script,
            query: record.body.query
        }
    };
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
function ElasticsearchPartialUpdateWritable(client, options) {
    assert(client, 'client is required');

    options = options || {};
    options.objectMode = true;

    Writable.call(this, options);

    this.client = client;
    this.logger = options.logger || null;

    this.highWaterMark = options.highWaterMark || 16;
    this.writtenRecords = 0;
}

/**
 * Bulk write records to Elasticsearch
 *
 * @private
 * @param {array} records
 * @param {Function} callback
 */
ElasticsearchPartialUpdateWritable.prototype.partialUpdate = function partialUpdate(data, callback) {
    try {
        var update = transformUpdate(data);
    } catch (error) {
        return callback(error);
    }

    this.client.updateByQuery(update, function bulkCallback(err, data) {
        if (err) {
            err.update = update;
            return callback(err);
        }

        if (data.failures.length !== 0) {
            if (this.logger) {
                data.failures.forEach(this.logger.error.bind(this.logger));
            }

            var error = new Error('One or more failures occurred during bulk update.');
            error.update = update;

            return callback(error);
        }

        callback(null, data);
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
ElasticsearchPartialUpdateWritable.prototype._write = function _write(update, enc, callback) {
    this.partialUpdate(update, function(err, data) {
        if (err) {
            return callback(err);
        }

        if (this.logger) {
            this.logger.info('Wrote %d records to Elasticsearch', data.updated);
        }

        this.writtenRecords += data.updated;

        callback();
    }.bind(this));
};

module.exports = ElasticsearchPartialUpdateWritable;
