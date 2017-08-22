'use strict';

var _ = require('lodash'),
    chai = require('chai'),
    sinon = require('sinon'),
    sinonChai = require('sinon-chai'),
    ElasticsearchWritable = require('../');

chai.use(sinonChai);

var expect = chai.expect;

var recordFixture = require('./fixture/record.json');
var recordDeleteFixture = require('./fixture/record-delete.json');
var recordParentFixture = require('./fixture/record-parent.json');
var recordUpdateFixture = require('./fixture/record-update.json');
var recordUpdateByQueryFixture = require('./fixture/record-update-by-query.json');
var successResponseFixture = require('./fixture/success-response.json');
var successDeleteResponseFixture = require('./fixture/success-delete-response.json');
var successParentResponseFixture = require('./fixture/success-parent-response.json');
var successUpdateResponseFixture = require('./fixture/success-update-response.json');
var successUpdateByQueryResponseFixture = require('./fixture/success-update-by-query-response.json');
var errorResponseFixture = require('./fixture/error-response.json');
var errorObjectResponseFixture = require('./fixture/error-object-response');

function getMissingFieldTest(fieldName, testFixture) {
    return function(done) {
        this.stream.on('error', function(error) {
            expect(error).to.be.instanceOf(Error);
            expect(error.message).to.eq(fieldName + ' is required');

            done();
        });

        var fixture = _.cloneDeep(testFixture || recordFixture);
        _.unset(fixture, fieldName);

        this.stream.end(fixture);
    };
}

describe('ElasticsearchWritable', function() {
    beforeEach(function() {
        this.sinon = sinon.sandbox.create();
    });

    afterEach(function() {
        this.sinon.restore();
    });

    describe('constructor', function() {
        it('should require client', function() {
            expect(function() {
                new ElasticsearchWritable();
            }).to.Throw(Error, 'client is required');
        });

        it('should default highWaterMark to 16', function() {
            var stream = new ElasticsearchWritable({});

            expect(stream.highWaterMark).to.eq(16);
        });
    });

    describe('queue', function() {
        beforeEach(function() {
            this.stream = new ElasticsearchWritable({}, { highWaterMark: 10 });
        });

        it('should queue up number of items equal to highWaterMark', function(done) {
            this.sinon.stub(this.stream, '_flush').yields();

            for (var i = 0; i < 8; i++) {
                this.stream.write(recordFixture);
            }

            this.stream.write(recordFixture, function() {
                expect(this.stream._flush).to.not.have.been.called;

                this.stream.write(recordFixture, function() {
                    expect(this.stream._flush).to.have.been.calledOnce;

                    done();
                }.bind(this));
            }.bind(this));
        });

        it('should flush queue if stream is closed', function(done) {
            this.sinon.stub(this.stream, '_flush').yields();

            this.stream.end(recordFixture, function() {
                expect(this.stream._flush).to.have.been.calledOnce;

                done();
            }.bind(this));
        });
    });

    describe('flushing', function() {
        beforeEach(function() {
            this.client = {
                bulk: this.sinon.stub()
            };

            this.stream = new ElasticsearchWritable(this.client, {
                highWaterMark: 6
            });
        });

        it('should write records to elasticsearch', function(done) {
            this.client.bulk.yields(null, successResponseFixture);

            this.stream.end(recordFixture, function() {
                expect(this.client.bulk).to.have.been.called;

                done();
            }.bind(this));
        });

        it('should do nothing if there is nothing in the queue when the stream is closed', function(done) {
            this.client.bulk.yields(null, successResponseFixture);

            this.stream.on('finish', function() {
                expect(this.client.bulk).to.have.been.calledOnce;

                done();
            }.bind(this));

            for (var i = 0; i < 6; i++) {
                this.stream.write(recordFixture);
            }

            this.stream.end();
        });

        it('should trigger error on elasticsearch error', function(done) {
            this.client.bulk.yields(new Error('Fail'));

            this.stream.on('error', function(error) {
                expect(error.message).to.eq('Fail');

                done();
            });

            this.stream.end(recordFixture);
        });

        it('should trigger error on bulk errors', function(done) {
            this.client.bulk.yields(null, errorResponseFixture);

            this.stream.on('error', function(error) {
                expect(error).to.be.instanceOf(Error);
                expect(error.message).to.deep.eq('InternalServerError,Forbidden');

                done();
            });

            this.stream.write(recordFixture);
            this.stream.end(recordFixture);
        });

        it('should support error objects', function(done) {
            this.client.bulk.yields(null, errorObjectResponseFixture);

            this.stream.on('error', function(error) {
                expect(error).to.be.instanceOf(Error);
                expect(error.message).to.deep.eq('InternalServerError[Error reason],Forbidden[Forbidden reason]');

                done();
            });

            this.stream.write(recordFixture);
            this.stream.end(recordFixture);
        });

        it('should throw error on index missing in record', getMissingFieldTest('index'));

        it('should throw error on type missing in record', getMissingFieldTest('type'));

        it('should throw error on body missing in record', getMissingFieldTest('body'));
    });

    describe('flush timeout', function() {
        beforeEach(function() {
            this.client = {
                bulk: this.sinon.stub()
            };

            this.stream = new ElasticsearchWritable(this.client, {
                highWaterMark: 10,
                flushTimeout: 1000
            });

            this.client.bulk.yields(null, successResponseFixture);
            this.clock = sinon.useFakeTimers();
        });

        it('should flush queue if there is something in the queue after timeout', function() {
            for (var i = 0; i < 10; i++) {
                this.stream.write(recordFixture);
            }

            expect(this.client.bulk).to.have.callCount(1);

            this.stream.write(recordFixture);
            this.clock.tick(1001);

            expect(this.client.bulk).to.have.callCount(2);
        });

        it('should clear flush timer when stream has ended', function(done) {
            this.sinon.spy(this.stream, '_flush');

            this.stream.write(recordFixture);

            this.stream.end(function() {
                expect(this.stream._flush).to.have.callCount(1);

                this.clock.tick(2001);

                expect(this.stream._flush).to.have.callCount(1);

                done();
            }.bind(this));
        });
    });

    describe('parent type', function() {
        beforeEach(function() {
            this.client = {
                bulk: this.sinon.stub()
            };

            this.stream = new ElasticsearchWritable(this.client, {
                highWaterMark: 1,
                flushTimeout: 10
            });

            this.client.bulk.yields(null, successParentResponseFixture);
            this.clock = sinon.useFakeTimers();
        });

        it('should include parent type in record if present', function() {
            this.stream.write(recordParentFixture);

            var expectedArgument = {
                body: [
                    {
                        index: {
                            _index: 'indexName',
                            _type: 'recordType',
                            _id: 'recordId',
                            _parent: 'parentRecordType'
                        }
                    },
                    {
                        foo: 'bar'
                    }
                ]
            };

            expect(this.client.bulk).to.have.callCount(1);
            expect(this.client.bulk).to.have.been.calledWith(expectedArgument);
        });
    });

    describe('custom action', function() {
        beforeEach(function() {
            this.client = {
                bulk: this.sinon.stub(),
                updateByQuery: this.sinon.stub()
            };

            this.stream = new ElasticsearchWritable(this.client, {
                highWaterMark: 1,
                flushTimeout: 10
            });

            this.clock = sinon.useFakeTimers();
        });

        it('should allow you to override the action', function() {
            this.client.bulk.yields(null, successUpdateResponseFixture);
            this.stream.write(recordUpdateFixture);

            var expectedArgument = {
                body: [
                    {
                        update: {
                            _index: 'indexName',
                            _type: 'recordType',
                            _id: 'recordId'
                        }
                    },
                    {
                        foo: 'bar'
                    }
                ]
            };

            expect(this.client.bulk).to.have.callCount(1);
            expect(this.client.bulk).to.have.been.calledWith(expectedArgument);
        });

        it('should not include body if action is delete', function() {
            this.client.bulk.yields(null, successDeleteResponseFixture);
            this.stream.write(recordDeleteFixture);

            var expectedArgument = {
                body: [
                    {
                        delete: {
                            _index: 'indexName',
                            _type: 'recordType',
                            _id: 'recordId'
                        }
                    }
                ]
            };

            expect(this.client.bulk).to.have.callCount(1);
            expect(this.client.bulk).to.have.been.calledWith(expectedArgument);
        });

        it('should support update_by_query', function() {
            this.client.updateByQuery.yields(null, successUpdateByQueryResponseFixture);
            this.stream.write(recordUpdateByQueryFixture);

            var expectedArgument = {
                index: 'indexName',
                type: 'recordType',
                body: {
                    script: {
                        inline: 'ctx._source.counter++'
                    },
                    query: {
                        term: {
                            foo: 'bar'
                        }
                    }
                }
            };

            expect(this.client.updateByQuery).to.have.callCount(1);
            expect(this.client.updateByQuery).to.have.been.calledWith(expectedArgument);
        });

        it('should throw error on body.script missing in update_by_query record',
            getMissingFieldTest('body.script', recordUpdateByQueryFixture));

        it('should throw error on body.query missing in update_by_query record',
            getMissingFieldTest('body.query', recordUpdateByQueryFixture));
    });
});
