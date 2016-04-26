'use strict';

var chai = require('chai'),
    sinon = require('sinon'),
    sinonChai = require('sinon-chai'),
    clone = require('clone'),
    ElasticsearchBulkIndexWritable = require('../');

chai.use(sinonChai);

var expect = chai.expect;

var recordFixture = require('./fixture/record.json');
var successResponseFixture = require('./fixture/success-response.json');
var errorResponseFixture = require('./fixture/error-response.json');

describe('ElastisearchBulkIndexWritable', function() {
    beforeEach(function() {
        this.sinon = sinon.sandbox.create();
    });

    afterEach(function() {
        this.sinon.restore();
    });

    describe('constructor', function() {
        it('should require client', function() {
            expect(function() {
                new ElasticsearchBulkIndexWritable();
            }).to.Throw(Error, 'client is required');
        });

        it('should default highWaterMark to 16', function() {
            var stream = new ElasticsearchBulkIndexWritable({});

            expect(stream.highWaterMark).to.eq(16);
        });
    });

    describe('queue', function() {
        beforeEach(function() {
            this.stream = new ElasticsearchBulkIndexWritable({}, { highWaterMark: 10 });
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
        function getMissingFieldTest(fieldName) {
            return function(done) {
                this.stream.on('error', function(error) {
                    expect(error).to.be.instanceOf(Error);
                    expect(error.message).to.eq(fieldName + ' is required');

                    done();
                });

                var fixture = clone(recordFixture);
                delete fixture[fieldName];

                this.stream.end(fixture);
            };
        }

        beforeEach(function() {
            this.client = {
                bulk: this.sinon.stub()
            };

            this.stream = new ElasticsearchBulkIndexWritable(this.client, {
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

        it('should throw error on index missing in record', getMissingFieldTest('index'));

        it('should throw error on type missing in record', getMissingFieldTest('type'));

        it('should throw error on body missing in record', getMissingFieldTest('body'));
    });

    describe('flush timeout', function() {
        beforeEach(function() {
            this.client = {
                bulk: this.sinon.stub()
            };

            this.stream = new ElasticsearchBulkIndexWritable(this.client, {
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
});
