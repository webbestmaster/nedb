// @ts-nocheck

import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import {describe, test, beforeEach} from '@jest/globals';
import _ from 'underscore';
import async from 'async';
import * as model from './../src/model';
import {Datastore} from '../src/datastore';
import {Persistence} from '../src/persistence';

const testDb = 'workspace/test.db';

// Test that even if a callback throws an exception, the next DB operations will still be executed
// We prevent Mocha from catching the exception we throw on purpose by remembering all current handlers, remove them and register them back after test ends
function testThrowInCallback(d, done) {
    var currentUncaughtExceptionHandlers = process.listeners('uncaughtException');

    process.removeAllListeners('uncaughtException');

    process.on('uncaughtException', function (err) {
        // Do nothing with the error which is only there to test we stay on track
    });

    d.find({}, function (err) {
        process.nextTick(function () {
            d.insert({bar: 1}, function (err) {
                process.removeAllListeners('uncaughtException');
                for (var i = 0; i < currentUncaughtExceptionHandlers.length; i += 1) {
                    process.on('uncaughtException', currentUncaughtExceptionHandlers[i]);
                }

                done();
            });
        });

        throw new Error('Some error');
    });
}

// Test that if the callback is falsy, the next DB operations will still be executed
function testFalsyCallback(d, done) {
    d.insert({a: 1}, null);
    process.nextTick(function () {
        d.update({a: 1}, {a: 2}, {}, null);
        process.nextTick(function () {
            d.update({a: 2}, {a: 1}, null);
            process.nextTick(function () {
                d.remove({a: 2}, {}, null);
                process.nextTick(function () {
                    d.remove({a: 2}, null);
                    process.nextTick(function () {
                        d.find({}, done);
                    });
                });
            });
        });
    });
}

// Test that operations are executed in the right order
// We prevent Mocha from catching the exception we throw on purpose by remembering all current handlers, remove them and register them back after test ends
function testRightOrder(d, done) {
    var currentUncaughtExceptionHandlers = process.listeners('uncaughtException');

    process.removeAllListeners('uncaughtException');

    process.on('uncaughtException', function (err) {
        // Do nothing with the error which is only there to test we stay on track
    });

    d.find({}, function (err, docs) {
        assert.equal(docs.length, 0);

        d.insert({a: 1}, function () {
            d.update({a: 1}, {a: 2}, {}, function () {
                d.find({}, function (err, docs) {
                    assert.equal(docs[0].a, 2);

                    process.nextTick(function () {
                        d.update({a: 2}, {a: 3}, {}, function () {
                            d.find({}, function (err, docs) {
                                assert.equal(docs[0].a, 3);

                                process.removeAllListeners('uncaughtException');
                                for (var i = 0; i < currentUncaughtExceptionHandlers.length; i += 1) {
                                    process.on('uncaughtException', currentUncaughtExceptionHandlers[i]);
                                }

                                done();
                            });
                        });
                    });

                    throw new Error('Some error');
                });
            });
        });
    });
}

// Note:  The following test does not have any assertion because it
// is meant to address the deprecation warning:
// (node) warning: Recursive process.nextTick detected. This will break in the next version of node. Please use setImmediate for recursive deferral.
// see
var testEventLoopStarvation = function (d, done) {
    var times = 1001;
    var i = 0;
    while (i < times) {
        i++;
        d.find({'bogus': 'search'}, function (err, docs) {});
    }
    done();
};

// Test that operations are executed in the right order even with no callback
function testExecutorWorksWithoutCallback(d, done) {
    d.insert({a: 1});
    d.insert({a: 2}, false);
    d.find({}, function (err, docs) {
        assert.equal(docs.length, 2);
        done();
    });
}

describe('Executor', function () {
    describe('With persistent database', function () {
        var d;

        beforeEach(function (done) {
            d = new Datastore({filename: testDb});
            assert.equal(d.filename, testDb);
            assert.equal(d.inMemoryOnly, false);

            async.waterfall(
                [
                    function (cb) {
                        Persistence.ensureDirectoryExists(path.dirname(testDb), function () {
                            fs.exists(testDb, function (exists) {
                                if (exists) {
                                    fs.unlink(testDb, cb);
                                } else {
                                    return cb();
                                }
                            });
                        });
                    },
                    function (cb) {
                        d.loadDatabase(function (err) {
                            assert.equal(err, null);
                            assert.equal(d.getAllData().length, 0);
                            return cb();
                        });
                    },
                ],
                done
            );
        });

        test.skip('A throw in a callback doesnt prevent execution of next operations', function (done) {
            testThrowInCallback(d, done);
        });

        test('A falsy callback doesnt prevent execution of next operations', function (done) {
            testFalsyCallback(d, done);
        });

        test.skip('Operations are executed in the right order', function (done) {
            testRightOrder(d, done);
        });

        test('Does not starve event loop and raise warning when more than 1000 callbacks are in queue', function (done) {
            testEventLoopStarvation(d, done);
        });

        test('Works in the right order even with no supplied callback', function (done) {
            testExecutorWorksWithoutCallback(d, done);
        });
    }); // ==== End of 'With persistent database' ====

    describe('With non persistent database', function () {
        var d;

        beforeEach(function (done) {
            d = new Datastore({inMemoryOnly: true});
            assert.equal(d.inMemoryOnly, true);

            d.loadDatabase(function (err) {
                assert.equal(err, null);
                assert.equal(d.getAllData().length, 0);
                return done();
            });
        });

        test.skip('A throw in a callback doesnt prevent execution of next operations', function (done) {
            testThrowInCallback(d, done);
        });

        test('A falsy callback doesnt prevent execution of next operations', function (done) {
            testFalsyCallback(d, done);
        });

        test.skip('Operations are executed in the right order', function (done) {
            testRightOrder(d, done);
        });

        test('Works in the right order even with no supplied callback', function (done) {
            testExecutorWorksWithoutCallback(d, done);
        });
    }); // ==== End of 'With non persistent database' ====
});
