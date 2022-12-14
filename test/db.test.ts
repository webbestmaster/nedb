// @ts-nocheck

import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import {describe, test, beforeEach} from '@jest/globals';
import _ from 'underscore';
import async from 'async';
import model from './../lib/model';
import Datastore from '../lib/datastore';
import Persistence from '../lib/persistence';

const reloadTimeUpperBound = 60; // In ms, an upper bound for the reload time used to check createdAt and updatedAt
const testDb = 'workspace/test.db';

describe('Database', function () {
    var d; //

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

    test('Constructor compatibility with v0.6-', function () {
        var dbef = new Datastore('somefile');
        assert.equal(dbef.filename, 'somefile');
        assert.equal(dbef.inMemoryOnly, false);

        var dbef = new Datastore('');
        assert.equal(dbef.filename, null);
        assert.equal(dbef.inMemoryOnly, true);

        var dbef = new Datastore();
        assert.equal(dbef.filename, null);
        assert.equal(dbef.inMemoryOnly, true);
    });

    describe('Autoloading', function () {
        test('Can autoload a database and query it right away', function (done) {
            var fileStr =
                    model.serialize({_id: '1', a: 5, planet: 'Earth'}) +
                    '\n' +
                    model.serialize({_id: '2', a: 5, planet: 'Mars'}) +
                    '\n',
                autoDb = 'workspace/auto.db',
                db;

            fs.writeFileSync(autoDb, fileStr, 'utf8');
            db = new Datastore({filename: autoDb, autoload: true});

            db.find({}, function (err, docs) {
                assert.equal(err, null);
                assert.equal(docs.length, 2);
                done();
            });
        });

        test('Throws if autoload fails', function (done) {
            var fileStr =
                    model.serialize({_id: '1', a: 5, planet: 'Earth'}) +
                    '\n' +
                    model.serialize({_id: '2', a: 5, planet: 'Mars'}) +
                    '\n' +
                    '{"$$indexCreated":{"fieldName":"a","unique":true}}',
                autoDb = 'workspace/auto.db',
                db;

            fs.writeFileSync(autoDb, fileStr, 'utf8');

            // Check the loadDatabase generated an error
            function onload(err) {
                assert.equal(err.errorType, 'uniqueViolated');
                done();
            }

            db = new Datastore({filename: autoDb, autoload: true, onload: onload});

            db.find({}, function (err, docs) {
                done(new Error('Find should not be executed since autoload failed'));
            });
        });
    });

    describe('Insert', function () {
        test('Able to insert a document in the database, setting an _id if none provided, and retrieve it even after a reload', function (done) {
            d.find({}, function (err, docs) {
                assert.equal(docs.length, 0);

                d.insert({somedata: 'ok'}, function (err) {
                    // The data was correctly updated
                    d.find({}, function (err, docs) {
                        assert.equal(err, null);
                        assert.equal(docs.length, 1);
                        assert.equal(Object.keys(docs[0]).length, 2);
                        assert.equal(docs[0].somedata, 'ok');
                        assert.notEqual(docs[0]._id, undefined);

                        // After a reload the data has been correctly persisted
                        d.loadDatabase(function (err) {
                            d.find({}, function (err, docs) {
                                assert.equal(err, null);
                                assert.equal(docs.length, 1);
                                assert.equal(Object.keys(docs[0]).length, 2);
                                assert.equal(docs[0].somedata, 'ok');
                                assert.notEqual(docs[0]._id, undefined);

                                done();
                            });
                        });
                    });
                });
            });
        });

        test('Can insert multiple documents in the database', function (done) {
            d.find({}, function (err, docs) {
                assert.equal(docs.length, 0);

                d.insert({somedata: 'ok'}, function (err) {
                    d.insert({somedata: 'another'}, function (err) {
                        d.insert({somedata: 'again'}, function (err) {
                            d.find({}, function (err, docs) {
                                assert.equal(docs.length, 3);
                                assert.equal(_.pluck(docs, 'somedata').includes('ok'), true);
                                assert.equal(_.pluck(docs, 'somedata').includes('another'), true);
                                assert.equal(_.pluck(docs, 'somedata').includes('again'), true);
                                done();
                            });
                        });
                    });
                });
            });
        });

        test('Can insert and get back from DB complex objects with all primitive and secondary types', function (done) {
            var da = new Date(),
                obj = {a: ['ee', 'ff', 42], date: da, subobj: {a: 'b', b: 'c'}};
            d.insert(obj, function (err) {
                d.findOne({}, function (err, res) {
                    assert.equal(err, null);
                    assert.equal(res.a.length, 3);
                    assert.equal(res.a[0], 'ee');
                    assert.equal(res.a[1], 'ff');
                    assert.equal(res.a[2], 42);
                    assert.equal(res.date.getTime(), da.getTime());
                    assert.equal(res.subobj.a, 'b');
                    assert.equal(res.subobj.b, 'c');

                    done();
                });
            });
        });

        test('If an object returned from the DB is modified and refetched, the original value should be found', function (done) {
            d.insert({a: 'something'}, function () {
                d.findOne({}, function (err, doc) {
                    assert.equal(doc.a, 'something');
                    doc.a = 'another thing';
                    assert.equal(doc.a, 'another thing');

                    // Re-fetching with findOne should yield the persisted value
                    d.findOne({}, function (err, doc) {
                        assert.equal(doc.a, 'something');
                        doc.a = 'another thing';
                        assert.equal(doc.a, 'another thing');

                        // Re-fetching with find should yield the persisted value
                        d.find({}, function (err, docs) {
                            assert.equal(docs[0].a, 'something');

                            done();
                        });
                    });
                });
            });
        });

        test('Cannot insert a doc that has a field beginning with a $ sign', function (done) {
            d.insert({$something: 'atest'}, function (err) {
                // assert.isDefined(err);
                assert.equal(err instanceof Error, true);
                done();
            });
        });

        test('If an _id is already given when we insert a document, use that instead of generating a random one', function (done) {
            d.insert({_id: 'test', stuff: true}, function (err, newDoc) {
                if (err) {
                    return done(err);
                }

                assert.equal(newDoc.stuff, true);
                assert.equal(newDoc._id, 'test');

                d.insert({_id: 'test', otherstuff: 42}, function (err) {
                    assert.equal(err.errorType, 'uniqueViolated');

                    done();
                });
            });
        });

        test('Modifying the insertedDoc after an insert doesnt change the copy saved in the database', function (done) {
            d.insert({a: 2, hello: 'world'}, function (err, newDoc) {
                newDoc.hello = 'changed';

                d.findOne({a: 2}, function (err, doc) {
                    assert.equal(doc.hello, 'world');
                    done();
                });
            });
        });

        test('Can insert an array of documents at once', function (done) {
            var docs = [
                {a: 5, b: 'hello'},
                {a: 42, b: 'world'},
            ];

            d.insert(docs, function (err) {
                d.find({}, function (err, docs) {
                    var data;

                    assert.equal(docs.length, 2);
                    assert.equal(
                        _.find(docs, function (doc) {
                            return doc.a === 5;
                        }).b,
                        'hello'
                    );
                    assert.equal(
                        _.find(docs, function (doc) {
                            return doc.a === 42;
                        }).b,
                        'world'
                    );

                    // The data has been persisted correctly
                    data = _.filter(fs.readFileSync(testDb, 'utf8').split('\n'), function (line) {
                        return line.length > 0;
                    });
                    assert.equal(data.length, 2);
                    assert.equal(model.deserialize(data[0]).a, 5);
                    assert.equal(model.deserialize(data[0]).b, 'hello');
                    assert.equal(model.deserialize(data[1]).a, 42);
                    assert.equal(model.deserialize(data[1]).b, 'world');

                    done();
                });
            });
        });

        test('If a bulk insert violates a constraint, all changes are rolled back', function (done) {
            var docs = [{a: 5, b: 'hello'}, {a: 42, b: 'world'}, {a: 5, b: 'bloup'}, {a: 7}];

            d.ensureIndex({fieldName: 'a', unique: true}, function () {
                // Important to specify callback here to make sure filesystem synced
                d.insert(docs, function (err) {
                    assert.equal(err.errorType, 'uniqueViolated');

                    d.find({}, function (err, docs) {
                        // Datafile only contains index definition
                        var datafileContents = model.deserialize(fs.readFileSync(testDb, 'utf8'));
                        assert.deepEqual(datafileContents, {$$indexCreated: {fieldName: 'a', unique: true}});

                        assert.equal(docs.length, 0);

                        done();
                    });
                });
            });
        });

        test('If timestampData option is set, a createdAt field is added and persisted', function (done) {
            var newDoc = {hello: 'world'},
                beginning = Date.now();
            d = new Datastore({filename: testDb, timestampData: true, autoload: true});
            d.find({}, function (err, docs) {
                assert.equal(err, null);
                assert.equal(docs.length, 0);

                d.insert(newDoc, function (err, insertedDoc) {
                    // No side effect on given input
                    assert.deepEqual(newDoc, {hello: 'world'});
                    // Insert doc has two new fields, _id and createdAt
                    assert.equal(insertedDoc.hello, 'world');
                    assert.notEqual(insertedDoc.createdAt, undefined);
                    assert.notEqual(insertedDoc.updatedAt, undefined);
                    assert.equal(insertedDoc.createdAt, insertedDoc.updatedAt);
                    assert.notEqual(insertedDoc._id, undefined);
                    assert.equal(Object.keys(insertedDoc).length, 4);
                    assert.equal(Math.abs(insertedDoc.createdAt.getTime() - beginning) < reloadTimeUpperBound, true); // No more than 30ms should have elapsed (worst case, if there is a flush)

                    // Modifying results of insert doesn't change the cache
                    insertedDoc.bloup = 'another';
                    assert.equal(Object.keys(insertedDoc).length, 5);

                    d.find({}, function (err, docs) {
                        assert.equal(docs.length, 1);
                        assert.deepEqual(newDoc, {hello: 'world'});
                        assert.deepEqual(
                            {
                                hello: 'world',
                                _id: insertedDoc._id,
                                createdAt: insertedDoc.createdAt,
                                updatedAt: insertedDoc.updatedAt,
                            },
                            docs[0]
                        );

                        // All data correctly persisted on disk
                        d.loadDatabase(function () {
                            d.find({}, function (err, docs) {
                                assert.equal(docs.length, 1);
                                assert.deepEqual(newDoc, {hello: 'world'});
                                assert.deepEqual(
                                    {
                                        hello: 'world',
                                        _id: insertedDoc._id,
                                        createdAt: insertedDoc.createdAt,
                                        updatedAt: insertedDoc.updatedAt,
                                    },
                                    docs[0]
                                );

                                done();
                            });
                        });
                    });
                });
            });
        });

        test("If timestampData option not set, don't create a createdAt and a updatedAt field", function (done) {
            d.insert({hello: 'world'}, function (err, insertedDoc) {
                assert.equal(Object.keys(insertedDoc).length, 2);
                assert.equal(insertedDoc.createdAt, undefined);
                assert.equal(insertedDoc.updatedAt, undefined);

                d.find({}, function (err, docs) {
                    assert.equal(docs.length, 1);
                    assert.deepEqual(docs[0], insertedDoc);

                    done();
                });
            });
        });

        test("If timestampData is set but createdAt is specified by user, don't change it", function (done) {
            var newDoc = {hello: 'world', createdAt: new Date(234)},
                beginning = Date.now();
            d = new Datastore({filename: testDb, timestampData: true, autoload: true});
            d.insert(newDoc, function (err, insertedDoc) {
                assert.equal(Object.keys(insertedDoc).length, 4);
                assert.equal(insertedDoc.createdAt.getTime(), 234); // Not modified
                assert.equal(insertedDoc.updatedAt.getTime() - beginning < reloadTimeUpperBound, true); // Created

                d.find({}, function (err, docs) {
                    assert.deepEqual(insertedDoc, docs[0]);

                    d.loadDatabase(function () {
                        d.find({}, function (err, docs) {
                            assert.deepEqual(insertedDoc, docs[0]);

                            done();
                        });
                    });
                });
            });
        });

        test("If timestampData is set but updatedAt is specified by user, don't change it", function (done) {
            var newDoc = {hello: 'world', updatedAt: new Date(234)},
                beginning = Date.now();
            d = new Datastore({filename: testDb, timestampData: true, autoload: true});
            d.insert(newDoc, function (err, insertedDoc) {
                assert.equal(Object.keys(insertedDoc).length, 4);
                assert.equal(insertedDoc.updatedAt.getTime(), 234); // Not modified
                assert.equal(insertedDoc.createdAt.getTime() - beginning < reloadTimeUpperBound, true); // Created

                d.find({}, function (err, docs) {
                    assert.deepEqual(insertedDoc, docs[0]);

                    d.loadDatabase(function () {
                        d.find({}, function (err, docs) {
                            assert.deepEqual(insertedDoc, docs[0]);

                            done();
                        });
                    });
                });
            });
        });

        test('Can insert a doc with id 0', function (done) {
            d.insert({_id: 0, hello: 'world'}, function (err, doc) {
                assert.equal(doc._id, 0);
                assert.equal(doc.hello, 'world');
                done();
            });
        });

        /**
         * Complicated behavior here. Basically we need to test that when a user function throws an exception, it is not caught
         * in NeDB and the callback called again, transforming a user error into a NeDB error.
         *
         * So we need a way to check that the callback is called only once and the exception thrown is indeed the client exception
         * Mocha's exception handling mechanism interferes with this since it already registers a listener on uncaughtException
         * which we need to use since findOne is not called in the same turn of the event loop (so no try/catch)
         * So we remove all current listeners, put our own which when called will register the former listeners (incl. Mocha's) again.
         *
         * Note: maybe using an in-memory only NeDB would give us an easier solution
         */
        test.only('If the callback throws an uncaught exception, do not catch it inside findOne, this is userspace concern', function (done) {
            var tryCount = 0,
                currentUncaughtExceptionHandlers = process.listeners('uncaughtException'),
                i;

            console.log(currentUncaughtExceptionHandlers.length)

            process.removeAllListeners('uncaughtException');

            process.on('uncaughtException', function MINE(ex) {

                console.error('///////// uncaughtException')

                process.removeAllListeners('uncaughtException');

                for (i = 0; i < currentUncaughtExceptionHandlers.length; i += 1) {
                    process.on('uncaughtException', currentUncaughtExceptionHandlers[i]);
                }

                // assert.equal(ex.message, 'SOME EXCEPTION');
                done();
            });

            d.insert({a: 5}, function () {
                d.findOne({a: 5}, function (err, doc) {
                    if (tryCount === 0) {
                        tryCount += 1;

                        console.log('///////// uncaughtException \\\\\\\\\\\\\\')

                        throw new Error('SOME EXCEPTION');
                    } else {
                        done(new Error('Callback was called twice'));
                    }
                });
            });
        });
    }); // ==== End of 'Insert' ==== //

    describe('#getCandidates', function () {
        test('Can use an index to get docs with a basic match', function (done) {
            d.ensureIndex({fieldName: 'tf'}, function (err) {
                d.insert({tf: 4}, function (err, _doc1) {
                    d.insert({tf: 6}, function () {
                        d.insert({tf: 4, an: 'other'}, function (err, _doc2) {
                            d.insert({tf: 9}, function () {
                                d.getCandidates({r: 6, tf: 4}, function (err, data) {
                                    var doc1 = _.find(data, function (d) {
                                            return d._id === _doc1._id;
                                        }),
                                        doc2 = _.find(data, function (d) {
                                            return d._id === _doc2._id;
                                        });
                                    assert.equal(data.length, 2);
                                    assert.deepEqual(doc1, {_id: doc1._id, tf: 4});
                                    assert.deepEqual(doc2, {_id: doc2._id, tf: 4, an: 'other'});

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Can use an index to get docs with a $in match', function (done) {
            d.ensureIndex({fieldName: 'tf'}, function (err) {
                d.insert({tf: 4}, function (err) {
                    d.insert({tf: 6}, function (err, _doc1) {
                        d.insert({tf: 4, an: 'other'}, function (err) {
                            d.insert({tf: 9}, function (err, _doc2) {
                                d.getCandidates({r: 6, tf: {$in: [6, 9, 5]}}, function (err, data) {
                                    var doc1 = _.find(data, function (d) {
                                            return d._id === _doc1._id;
                                        }),
                                        doc2 = _.find(data, function (d) {
                                            return d._id === _doc2._id;
                                        });
                                    assert.equal(data.length, 2);
                                    assert.deepEqual(doc1, {_id: doc1._id, tf: 6});
                                    assert.deepEqual(doc2, {_id: doc2._id, tf: 9});

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        test('If no index can be used, return the whole database', function (done) {
            d.ensureIndex({fieldName: 'tf'}, function (err) {
                d.insert({tf: 4}, function (err, _doc1) {
                    d.insert({tf: 6}, function (err, _doc2) {
                        d.insert({tf: 4, an: 'other'}, function (err, _doc3) {
                            d.insert({tf: 9}, function (err, _doc4) {
                                d.getCandidates({r: 6, notf: {$in: [6, 9, 5]}}, function (err, data) {
                                    var doc1 = _.find(data, function (d) {
                                            return d._id === _doc1._id;
                                        }),
                                        doc2 = _.find(data, function (d) {
                                            return d._id === _doc2._id;
                                        }),
                                        doc3 = _.find(data, function (d) {
                                            return d._id === _doc3._id;
                                        }),
                                        doc4 = _.find(data, function (d) {
                                            return d._id === _doc4._id;
                                        });
                                    assert.equal(data.length, 4);
                                    assert.deepEqual(doc1, {_id: doc1._id, tf: 4});
                                    assert.deepEqual(doc2, {_id: doc2._id, tf: 6});
                                    assert.deepEqual(doc3, {_id: doc3._id, tf: 4, an: 'other'});
                                    assert.deepEqual(doc4, {_id: doc4._id, tf: 9});

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Can use indexes for comparison matches', function (done) {
            d.ensureIndex({fieldName: 'tf'}, function (err) {
                d.insert({tf: 4}, function (err, _doc1) {
                    d.insert({tf: 6}, function (err, _doc2) {
                        d.insert({tf: 4, an: 'other'}, function (err, _doc3) {
                            d.insert({tf: 9}, function (err, _doc4) {
                                d.getCandidates({r: 6, tf: {$lte: 9, $gte: 6}}, function (err, data) {
                                    var doc2 = _.find(data, function (d) {
                                            return d._id === _doc2._id;
                                        }),
                                        doc4 = _.find(data, function (d) {
                                            return d._id === _doc4._id;
                                        });
                                    assert.equal(data.length, 2);
                                    assert.deepEqual(doc2, {_id: doc2._id, tf: 6});
                                    assert.deepEqual(doc4, {_id: doc4._id, tf: 9});

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Can set a TTL index that expires documents', function (done) {
            d.ensureIndex({fieldName: 'exp', expireAfterSeconds: 0.2}, function () {
                d.insert({hello: 'world', exp: new Date()}, function () {
                    setTimeout(function () {
                        d.findOne({}, function (err, doc) {
                            assert.equal(err, null);
                            assert.equal(doc.hello, 'world');

                            setTimeout(function () {
                                d.findOne({}, function (err, doc) {
                                    assert.equal(err, null);
                                    assert.equal(doc, null);

                                    d.on('compaction.done', function () {
                                        // After compaction, no more mention of the document, correctly removed
                                        var datafileContents = fs.readFileSync(testDb, 'utf8');
                                        assert.equal(datafileContents.split('\n').length, 2);
                                        assert.equal(datafileContents.match(/world/), null);

                                        // New datastore on same datafile is empty
                                        var d2 = new Datastore({filename: testDb, autoload: true});
                                        d2.findOne({}, function (err, doc) {
                                            assert.equal(err, null);
                                            assert.equal(doc, null);

                                            done();
                                        });
                                    });

                                    d.persistence.compactDatafile();
                                });
                            }, 101);
                        });
                    }, 100);
                });
            });
        });

        test('TTL indexes can expire multiple documents and only what needs to be expired', function (done) {
            d.ensureIndex({fieldName: 'exp', expireAfterSeconds: 0.2}, function () {
                d.insert({hello: 'world1', exp: new Date()}, function () {
                    d.insert({hello: 'world2', exp: new Date()}, function () {
                        d.insert({hello: 'world3', exp: new Date(new Date().getTime() + 100)}, function () {
                            setTimeout(function () {
                                d.find({}, function (err, docs) {
                                    assert.equal(err, null);
                                    assert.equal(docs.length, 3);

                                    setTimeout(function () {
                                        d.find({}, function (err, docs) {
                                            assert.equal(err, null);
                                            assert.equal(docs.length, 1);
                                            assert.equal(docs[0].hello, 'world3');

                                            setTimeout(function () {
                                                d.find({}, function (err, docs) {
                                                    assert.equal(err, null);
                                                    assert.equal(docs.length, 0);

                                                    done();
                                                });
                                            }, 101);
                                        });
                                    }, 101);
                                });
                            }, 100);
                        });
                    });
                });
            });
        });

        test('Document where indexed field is absent or not a date are ignored', function (done) {
            d.ensureIndex({fieldName: 'exp', expireAfterSeconds: 0.2}, function () {
                d.insert({hello: 'world1', exp: new Date()}, function () {
                    d.insert({hello: 'world2', exp: 'not a date'}, function () {
                        d.insert({hello: 'world3'}, function () {
                            setTimeout(function () {
                                d.find({}, function (err, docs) {
                                    assert.equal(err, null);
                                    assert.equal(docs.length, 3);

                                    setTimeout(function () {
                                        d.find({}, function (err, docs) {
                                            assert.equal(err, null);
                                            assert.equal(docs.length, 2);

                                            assert.notEqual(docs[0].hello, 'world1');
                                            assert.notEqual(docs[1].hello, 'world1');

                                            done();
                                        });
                                    }, 101);
                                });
                            }, 100);
                        });
                    });
                });
            });
        });
    }); // ==== End of '#getCandidates' ==== //

    describe('Find', function () {
        test('Can find all documents if an empty query is used', function (done) {
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err) {
                            d.insert({somedata: 'another', plus: 'additional data'}, function (err) {
                                d.insert({somedata: 'again'}, function (err) {
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        // Test with empty object
                        d.find({}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 3);
                            assert.equal(_.pluck(docs, 'somedata').includes('ok'), true);
                            assert.equal(_.pluck(docs, 'somedata').includes('another'), true);
                            assert.equal(
                                _.find(docs, function (d) {
                                    return d.somedata === 'another';
                                }).plus,
                                'additional data'
                            );
                            assert.equal(_.pluck(docs, 'somedata').includes('again'), true);
                            return cb();
                        });
                    },
                ],
                done
            );
        });

        test('Can find all documents matching a basic query', function (done) {
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err) {
                            d.insert({somedata: 'again', plus: 'additional data'}, function (err) {
                                d.insert({somedata: 'again'}, function (err) {
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        // Test with query that will return docs
                        d.find({somedata: 'again'}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 2);
                            assert.equal(_.pluck(docs, 'somedata').includes('ok'), false);
                            return cb();
                        });
                    },
                    function (cb) {
                        // Test with query that doesn't match anything
                        d.find({somedata: 'nope'}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 0);
                            return cb();
                        });
                    },
                ],
                done
            );
        });

        test('Can find one document matching a basic query and return null if none is found', function (done) {
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err) {
                            d.insert({somedata: 'again', plus: 'additional data'}, function (err) {
                                d.insert({somedata: 'again'}, function (err) {
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        // Test with query that will return docs
                        d.findOne({somedata: 'ok'}, function (err, doc) {
                            assert.equal(err, null);
                            assert.equal(Object.keys(doc).length, 2);
                            assert.equal(doc.somedata, 'ok');
                            assert.notEqual(doc._id, undefined);
                            return cb();
                        });
                    },
                    function (cb) {
                        // Test with query that doesn't match anything
                        d.findOne({somedata: 'nope'}, function (err, doc) {
                            assert.equal(err, null);
                            assert.equal(doc, null);
                            return cb();
                        });
                    },
                ],
                done
            );
        });

        test('Can find dates and objects (non JS-native types)', function (done) {
            var date1 = new Date(1234543),
                date2 = new Date(9999);
            d.insert({now: date1, sth: {name: 'nedb'}}, function () {
                d.findOne({now: date1}, function (err, doc) {
                    assert.equal(err, null);
                    assert.equal(doc.sth.name, 'nedb');

                    d.findOne({now: date2}, function (err, doc) {
                        assert.equal(err, null);
                        assert.equal(doc, null);

                        d.findOne({sth: {name: 'nedb'}}, function (err, doc) {
                            assert.equal(err, null);
                            assert.equal(doc.sth.name, 'nedb');

                            d.findOne({sth: {name: 'other'}}, function (err, doc) {
                                assert.equal(err, null);
                                assert.equal(doc, null);

                                done();
                            });
                        });
                    });
                });
            });
        });

        test('Can use dot-notation to query subfields', function (done) {
            d.insert({greeting: {english: 'hello'}}, function () {
                d.findOne({'greeting.english': 'hello'}, function (err, doc) {
                    assert.equal(err, null);
                    assert.equal(doc.greeting.english, 'hello');

                    d.findOne({'greeting.english': 'hellooo'}, function (err, doc) {
                        assert.equal(err, null);
                        assert.equal(doc, null);

                        d.findOne({'greeting.englis': 'hello'}, function (err, doc) {
                            assert.equal(err, null);
                            assert.equal(doc, null);

                            done();
                        });
                    });
                });
            });
        });

        test('Array fields match if any element matches', function (done) {
            d.insert({fruits: ['pear', 'apple', 'banana']}, function (err, doc1) {
                d.insert({fruits: ['coconut', 'orange', 'pear']}, function (err, doc2) {
                    d.insert({fruits: ['banana']}, function (err, doc3) {
                        d.find({fruits: 'pear'}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 2);
                            assert.equal(_.pluck(docs, '_id').includes(doc1._id), true);
                            assert.equal(_.pluck(docs, '_id').includes(doc2._id), true);

                            d.find({fruits: 'banana'}, function (err, docs) {
                                assert.equal(err, null);
                                assert.equal(docs.length, 2);
                                assert.equal(_.pluck(docs, '_id').includes(doc1._id), true);
                                assert.equal(_.pluck(docs, '_id').includes(doc3._id), true);

                                d.find({fruits: 'doesntexist'}, function (err, docs) {
                                    assert.equal(err, null);
                                    assert.equal(docs.length, 0);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Returns an error if the query is not well formed', function (done) {
            d.insert({hello: 'world'}, function () {
                d.find({$or: {hello: 'world'}}, function (err, docs) {
                    assert.equal(err instanceof Error, true);
                    assert.equal(docs, undefined);

                    d.findOne({$or: {hello: 'world'}}, function (err, doc) {
                        assert.equal(err instanceof Error, true);
                        assert.equal(doc, undefined);

                        done();
                    });
                });
            });
        });

        test('Changing the documents returned by find or findOne do not change the database state', function (done) {
            d.insert({a: 2, hello: 'world'}, function () {
                d.findOne({a: 2}, function (err, doc) {
                    doc.hello = 'changed';

                    d.findOne({a: 2}, function (err, doc) {
                        assert.equal(doc.hello, 'world');

                        d.find({a: 2}, function (err, docs) {
                            docs[0].hello = 'changed';

                            d.findOne({a: 2}, function (err, doc) {
                                assert.equal(doc.hello, 'world');

                                done();
                            });
                        });
                    });
                });
            });
        });

        test('Can use sort, skip and limit if the callback is not passed to find but to exec', function (done) {
            d.insert({a: 2, hello: 'world'}, function () {
                d.insert({a: 24, hello: 'earth'}, function () {
                    d.insert({a: 13, hello: 'blueplanet'}, function () {
                        d.insert({a: 15, hello: 'home'}, function () {
                            d.find({})
                                .sort({a: 1})
                                .limit(2)
                                .exec(function (err, docs) {
                                    assert.equal(err, null);
                                    assert.equal(docs.length, 2);
                                    assert.equal(docs[0].hello, 'world');
                                    assert.equal(docs[1].hello, 'blueplanet');
                                    done();
                                });
                        });
                    });
                });
            });
        });

        test('Can use sort and skip if the callback is not passed to findOne but to exec', function (done) {
            d.insert({a: 2, hello: 'world'}, function () {
                d.insert({a: 24, hello: 'earth'}, function () {
                    d.insert({a: 13, hello: 'blueplanet'}, function () {
                        d.insert({a: 15, hello: 'home'}, function () {
                            // No skip no query
                            d.findOne({})
                                .sort({a: 1})
                                .exec(function (err, doc) {
                                    assert.equal(err, null);
                                    assert.equal(doc.hello, 'world');

                                    // A query
                                    d.findOne({a: {$gt: 14}})
                                        .sort({a: 1})
                                        .exec(function (err, doc) {
                                            assert.equal(err, null);
                                            assert.equal(doc.hello, 'home');

                                            // And a skip
                                            d.findOne({a: {$gt: 14}})
                                                .sort({a: 1})
                                                .skip(1)
                                                .exec(function (err, doc) {
                                                    assert.equal(err, null);
                                                    assert.equal(doc.hello, 'earth');

                                                    // No result
                                                    d.findOne({a: {$gt: 14}})
                                                        .sort({a: 1})
                                                        .skip(2)
                                                        .exec(function (err, doc) {
                                                            assert.equal(err, null);
                                                            assert.equal(doc, null);

                                                            done();
                                                        });
                                                });
                                        });
                                });
                        });
                    });
                });
            });
        });

        test('Can use projections in find, normal or cursor way', function (done) {
            d.insert({a: 2, hello: 'world'}, function (err, doc0) {
                d.insert({a: 24, hello: 'earth'}, function (err, doc1) {
                    d.find({a: 2}, {a: 0, _id: 0}, function (err, docs) {
                        assert.equal(err, null);
                        assert.equal(docs.length, 1);
                        assert.deepEqual(docs[0], {hello: 'world'});

                        d.find({a: 2}, {a: 0, _id: 0}).exec(function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 1);
                            assert.deepEqual(docs[0], {hello: 'world'});

                            // Can't use both modes at once if not _id
                            d.find({a: 2}, {a: 0, hello: 1}, function (err, docs) {
                                assert.notEqual(err, null);
                                assert.equal(docs, undefined);

                                d.find({a: 2}, {a: 0, hello: 1}).exec(function (err, docs) {
                                    assert.notEqual(err, null);
                                    assert.equal(docs, undefined);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Can use projections in findOne, normal or cursor way', function (done) {
            d.insert({a: 2, hello: 'world'}, function (err, doc0) {
                d.insert({a: 24, hello: 'earth'}, function (err, doc1) {
                    d.findOne({a: 2}, {a: 0, _id: 0}, function (err, doc) {
                        assert.equal(err, null);
                        assert.deepEqual(doc, {hello: 'world'});

                        d.findOne({a: 2}, {a: 0, _id: 0}).exec(function (err, doc) {
                            assert.equal(err, null);
                            assert.deepEqual(doc, {hello: 'world'});

                            // Can't use both modes at once if not _id
                            d.findOne({a: 2}, {a: 0, hello: 1}, function (err, doc) {
                                assert.notEqual(err, null);
                                assert.equal(doc, undefined);

                                d.findOne({a: 2}, {a: 0, hello: 1}).exec(function (err, doc) {
                                    assert.notEqual(err, null);
                                    assert.equal(doc, undefined);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });
    }); // ==== End of 'Find' ==== //

    describe('Count', function () {
        test('Count all documents if an empty query is used', function (done) {
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err) {
                            d.insert({somedata: 'another', plus: 'additional data'}, function (err) {
                                d.insert({somedata: 'again'}, function (err) {
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        // Test with empty object
                        d.count({}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs, 3);
                            return cb();
                        });
                    },
                ],
                done
            );
        });

        test('Count all documents matching a basic query', function (done) {
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err) {
                            d.insert({somedata: 'again', plus: 'additional data'}, function (err) {
                                d.insert({somedata: 'again'}, function (err) {
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        // Test with query that will return docs
                        d.count({somedata: 'again'}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs, 2);
                            return cb();
                        });
                    },
                    function (cb) {
                        // Test with query that doesn't match anything
                        d.count({somedata: 'nope'}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs, 0);
                            return cb();
                        });
                    },
                ],
                done
            );
        });

        test('Array fields match if any element matches', function (done) {
            d.insert({fruits: ['pear', 'apple', 'banana']}, function (err, doc1) {
                d.insert({fruits: ['coconut', 'orange', 'pear']}, function (err, doc2) {
                    d.insert({fruits: ['banana']}, function (err, doc3) {
                        d.count({fruits: 'pear'}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs, 2);

                            d.count({fruits: 'banana'}, function (err, docs) {
                                assert.equal(err, null);
                                assert.equal(docs, 2);

                                d.count({fruits: 'doesntexist'}, function (err, docs) {
                                    assert.equal(err, null);
                                    assert.equal(docs, 0);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Returns an error if the query is not well formed', function (done) {
            d.insert({hello: 'world'}, function () {
                d.count({$or: {hello: 'world'}}, function (err, docs) {
                    assert.equal(err instanceof Error, true);
                    assert.equal(docs, undefined);

                    done();
                });
            });
        });
    });

    describe('Update', function () {
        test("If the query doesn't match anything, database is not modified", function (done) {
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err) {
                            d.insert({somedata: 'again', plus: 'additional data'}, function (err) {
                                d.insert({somedata: 'another'}, function (err) {
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        // Test with query that doesn't match anything
                        d.update({somedata: 'nope'}, {newDoc: 'yes'}, {multi: true}, function (err, n) {
                            assert.equal(err, null);
                            assert.equal(n, 0);

                            d.find({}, function (err, docs) {
                                var doc1 = _.find(docs, function (d) {
                                        return d.somedata === 'ok';
                                    }),
                                    doc2 = _.find(docs, function (d) {
                                        return d.somedata === 'again';
                                    }),
                                    doc3 = _.find(docs, function (d) {
                                        return d.somedata === 'another';
                                    });
                                assert.equal(docs.length, 3);
                                assert.equal(
                                    _.find(docs, function (d) {
                                        return d.newDoc === 'yes';
                                    }), undefined
                                );

                                assert.deepEqual(doc1, {_id: doc1._id, somedata: 'ok'});
                                assert.deepEqual(doc2, {_id: doc2._id, somedata: 'again', plus: 'additional data'});
                                assert.deepEqual(doc3, {_id: doc3._id, somedata: 'another'});

                                return cb();
                            });
                        });
                    },
                ],
                done
            );
        });

        test('If timestampData option is set, update the updatedAt field', function (done) {
            var beginning = Date.now();
            d = new Datastore({filename: testDb, autoload: true, timestampData: true});
            d.insert({hello: 'world'}, function (err, insertedDoc) {
                assert.equal(insertedDoc.updatedAt.getTime() - beginning < reloadTimeUpperBound, true);
                assert.equal(insertedDoc.createdAt.getTime() - beginning < reloadTimeUpperBound, true);
                assert.equal(Object.keys(insertedDoc).length, 4);

                // Wait 100ms before performing the update
                setTimeout(function () {
                    var step1 = Date.now();
                    d.update({_id: insertedDoc._id}, {$set: {hello: 'mars'}}, {}, function () {
                        d.find({_id: insertedDoc._id}, function (err, docs) {
                            assert.equal(docs.length, 1);
                            assert.equal(Object.keys(docs[0]).length, 4);
                            assert.equal(docs[0]._id, insertedDoc._id);
                            assert.equal(docs[0].createdAt, insertedDoc.createdAt);
                            assert.equal(docs[0].hello, 'mars');
                            assert.equal(docs[0].updatedAt.getTime() - beginning> 99, true); // updatedAt modified
                            assert.equal(docs[0].updatedAt.getTime() - step1 < reloadTimeUpperBound, true); // updatedAt modified

                            done();
                        });
                    });
                }, 100);
            });
        });

        test('Can update multiple documents matching the query', function (done) {
            var id1, id2, id3;

            // Test DB state after update and reload
            function testPostUpdateState(cb) {
                d.find({}, function (err, docs) {
                    var doc1 = _.find(docs, function (d) {
                            return d._id === id1;
                        }),
                        doc2 = _.find(docs, function (d) {
                            return d._id === id2;
                        }),
                        doc3 = _.find(docs, function (d) {
                            return d._id === id3;
                        });
                    assert.equal(docs.length, 3);

                    assert.equal(Object.keys(doc1).length, 2);
                    assert.equal(doc1.somedata, 'ok');
                    assert.equal(doc1._id, id1);

                    assert.equal(Object.keys(doc2).length, 2);
                    assert.equal(doc2.newDoc, 'yes');
                    assert.equal(doc2._id, id2);

                    assert.equal(Object.keys(doc3).length, 2);
                    assert.equal(doc3.newDoc, 'yes');
                    assert.equal(doc3._id, id3);

                    return cb();
                });
            }

            // Actually launch the tests
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err, doc1) {
                            id1 = doc1._id;
                            d.insert({somedata: 'again', plus: 'additional data'}, function (err, doc2) {
                                id2 = doc2._id;
                                d.insert({somedata: 'again'}, function (err, doc3) {
                                    id3 = doc3._id;
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        d.update({somedata: 'again'}, {newDoc: 'yes'}, {multi: true}, function (err, n) {
                            assert.equal(err, null);
                            assert.equal(n, 2);
                            return cb();
                        });
                    },
                    async.apply(testPostUpdateState),
                    function (cb) {
                        d.loadDatabase(function (err) {
                            cb(err);
                        });
                    },
                    async.apply(testPostUpdateState),
                ],
                done
            );
        });

        test('Can update only one document matching the query', function (done) {
            var id1, id2, id3;

            // Test DB state after update and reload
            function testPostUpdateState(cb) {
                d.find({}, function (err, docs) {
                    var doc1 = _.find(docs, function (d) {
                            return d._id === id1;
                        }),
                        doc2 = _.find(docs, function (d) {
                            return d._id === id2;
                        }),
                        doc3 = _.find(docs, function (d) {
                            return d._id === id3;
                        });
                    assert.equal(docs.length, 3);

                    assert.deepEqual(doc1, {somedata: 'ok', _id: doc1._id});

                    // doc2 or doc3 was modified. Since we sort on _id and it is random
                    // it can be either of two situations
                    try {
                        assert.deepEqual(doc2, {newDoc: 'yes', _id: doc2._id});
                        assert.deepEqual(doc3, {somedata: 'again', _id: doc3._id});
                    } catch (e) {
                        assert.deepEqual(doc2, {somedata: 'again', plus: 'additional data', _id: doc2._id});
                        assert.deepEqual(doc3, {newDoc: 'yes', _id: doc3._id});
                    }

                    return cb();
                });
            }

            // Actually launch the test
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err, doc1) {
                            id1 = doc1._id;
                            d.insert({somedata: 'again', plus: 'additional data'}, function (err, doc2) {
                                id2 = doc2._id;
                                d.insert({somedata: 'again'}, function (err, doc3) {
                                    id3 = doc3._id;
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        // Test with query that doesn't match anything
                        d.update({somedata: 'again'}, {newDoc: 'yes'}, {multi: false}, function (err, n) {
                            assert.equal(err, null);
                            assert.equal(n, 1);
                            return cb();
                        });
                    },
                    async.apply(testPostUpdateState),
                    function (cb) {
                        d.loadDatabase(function (err) {
                            return cb(err);
                        });
                    },
                    async.apply(testPostUpdateState), // The persisted state has been updated
                ],
                done
            );
        });

        describe('Upserts', function () {
            test('Can perform upserts if needed', function (done) {
                d.update({impossible: 'db is empty anyway'}, {newDoc: true}, {}, function (err, nr, upsert) {
                    assert.equal(err, null);
                    assert.equal(nr, 0);
                    assert.equal(upsert, undefined);

                    d.find({}, function (err, docs) {
                        assert.equal(docs.length, 0); // Default option for upsert is false

                        d.update(
                            {impossible: 'db is empty anyway'},
                            {something: 'created ok'},
                            {upsert: true},
                            function (err, nr, newDoc) {
                                assert.equal(err, null);
                                assert.equal(nr, 1);
                                assert.equal(newDoc.something, 'created ok');
                                assert.notEqual(newDoc._id, undefined);

                                d.find({}, function (err, docs) {
                                    assert.equal(docs.length, 1); // Default option for upsert is false
                                    assert.equal(docs[0].something, 'created ok');

                                    // Modifying the returned upserted document doesn't modify the database
                                    newDoc.newField = true;
                                    d.find({}, function (err, docs) {
                                        assert.equal(docs[0].something, 'created ok');
                                        assert.equal(docs[0].newField, undefined);

                                        done();
                                    });
                                });
                            }
                        );
                    });
                });
            });

            test('If the update query is a normal object with no modifiers, it is the doc that will be upserted', function (done) {
                d.update({$or: [{a: 4}, {a: 5}]}, {hello: 'world', bloup: 'blap'}, {upsert: true}, function (err) {
                    d.find({}, function (err, docs) {
                        assert.equal(err, null);
                        assert.equal(docs.length, 1);
                        var doc = docs[0];
                        assert.equal(Object.keys(doc).length, 3);
                        assert.equal(doc.hello, 'world');
                        assert.equal(doc.bloup, 'blap');
                        done();
                    });
                });
            });

            test('If the update query contains modifiers, it is applied to the object resulting from removing all operators from the find query 1', function (done) {
                d.update(
                    {$or: [{a: 4}, {a: 5}]},
                    {$set: {hello: 'world'}, $inc: {bloup: 3}},
                    {upsert: true},
                    function (err) {
                        d.find({hello: 'world'}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 1);
                            var doc = docs[0];
                            assert.equal(Object.keys(doc).length, 3);
                            assert.equal(doc.hello, 'world');
                            assert.equal(doc.bloup, 3);
                            done();
                        });
                    }
                );
            });

            test('If the update query contains modifiers, it is applied to the object resulting from removing all operators from the find query 2', function (done) {
                d.update(
                    {$or: [{a: 4}, {a: 5}], cac: 'rrr'},
                    {$set: {hello: 'world'}, $inc: {bloup: 3}},
                    {upsert: true},
                    function (err) {
                        d.find({hello: 'world'}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 1);
                            var doc = docs[0];
                            assert.equal(Object.keys(doc).length, 4);
                            assert.equal(doc.cac, 'rrr');
                            assert.equal(doc.hello, 'world');
                            assert.equal(doc.bloup, 3);
                            done();
                        });
                    }
                );
            });

            test('Performing upsert with badly formatted fields yields a standard error not an exception', function (done) {
                d.update({_id: '1234'}, {$set: {$$badfield: 5}}, {upsert: true}, function (err, doc) {
                    assert.equal(err instanceof Error, true);
                    done();
                });
            });
        }); // ==== End of 'Upserts' ==== //

        test('Cannot perform update if the update query is not either registered-modifiers-only or copy-only, or contain badly formatted fields', function (done) {
            d.insert({something: 'yup'}, function () {
                d.update({}, {boom: {$badfield: 5}}, {multi: false}, function (err) {
                    assert.equal(err instanceof Error, true);

                    d.update({}, {boom: {'bad.field': 5}}, {multi: false}, function (err) {
                        assert.equal(err instanceof Error, true);

                        d.update({}, {$inc: {test: 5}, mixed: 'rrr'}, {multi: false}, function (err) {
                            assert.equal(err instanceof Error, true);

                            d.update({}, {$inexistent: {test: 5}}, {multi: false}, function (err) {
                                assert.equal(err instanceof Error, true);

                                done();
                            });
                        });
                    });
                });
            });
        });

        test('Can update documents using multiple modifiers', function (done) {
            var id;

            d.insert({something: 'yup', other: 40}, function (err, newDoc) {
                id = newDoc._id;

                d.update({}, {$set: {something: 'changed'}, $inc: {other: 10}}, {multi: false}, function (err, nr) {
                    assert.equal(err, null);
                    assert.equal(nr, 1);

                    d.findOne({_id: id}, function (err, doc) {
                        assert.equal(Object.keys(doc).length, 3);
                        assert.equal(doc._id, id);
                        assert.equal(doc.something, 'changed');
                        assert.equal(doc.other, 50);

                        done();
                    });
                });
            });
        });

        test('Can upsert a document even with modifiers', function (done) {
            d.update({bloup: 'blap'}, {$set: {hello: 'world'}}, {upsert: true}, function (err, nr, newDoc) {
                assert.equal(err, null);
                assert.equal(nr, 1);
                assert.equal(newDoc.bloup, 'blap');
                assert.equal(newDoc.hello, 'world');
                assert.notEqual(newDoc._id, undefined);

                d.find({}, function (err, docs) {
                    assert.equal(docs.length, 1);
                    assert.equal(Object.keys(docs[0]).length, 3);
                    assert.equal(docs[0].hello, 'world');
                    assert.equal(docs[0].bloup, 'blap');
                    assert.notEqual(docs[0]._id, undefined);

                    done();
                });
            });
        });

        test('When using modifiers, the only way to update subdocs is with the dot-notation', function (done) {
            d.insert({bloup: {blip: 'blap', other: true}}, function () {
                // Correct methos
                d.update({}, {$set: {'bloup.blip': 'hello'}}, {}, function () {
                    d.findOne({}, function (err, doc) {
                        assert.equal(doc.bloup.blip, 'hello');
                        assert.equal(doc.bloup.other, true);

                        // Wrong
                        d.update({}, {$set: {bloup: {blip: 'ola'}}}, {}, function () {
                            d.findOne({}, function (err, doc) {
                                assert.equal(doc.bloup.blip, 'ola');
                                assert.equal(doc.bloup.other, undefined); // This information was lost

                                done();
                            });
                        });
                    });
                });
            });
        });

        test('Returns an error if the query is not well formed', function (done) {
            d.insert({hello: 'world'}, function () {
                d.update({$or: {hello: 'world'}}, {a: 1}, {}, function (err, nr, upsert) {
                    assert.equal(err instanceof Error, true);
                    assert.equal(nr, undefined);
                    assert.equal(upsert, undefined);

                    done();
                });
            });
        });

        test('If an error is thrown by a modifier, the database state is not changed', function (done) {
            d.insert({hello: 'world'}, function (err, newDoc) {
                d.update({}, {$inc: {hello: 4}}, {}, function (err, nr) {
                    assert.equal(err instanceof Error, true);
                    assert.equal(nr, undefined);

                    d.find({}, function (err, docs) {
                        assert.deepEqual(docs, [{_id: newDoc._id, hello: 'world'}]);

                        done();
                    });
                });
            });
        });

        test('Cant change the _id of a document', function (done) {
            d.insert({a: 2}, function (err, newDoc) {
                d.update({a: 2}, {a: 2, _id: 'nope'}, {}, function (err) {
                    assert.equal(err instanceof Error, true);

                    d.find({}, function (err, docs) {
                        assert.equal(docs.length, 1);
                        assert.equal(Object.keys(docs[0]).length, 2);
                        assert.equal(docs[0].a, 2);
                        assert.equal(docs[0]._id, newDoc._id);

                        d.update({a: 2}, {$set: {_id: 'nope'}}, {}, function (err) {
                            assert.equal(err instanceof Error, true);

                            d.find({}, function (err, docs) {
                                assert.equal(docs.length, 1);
                                assert.equal(Object.keys(docs[0]).length, 2);
                                assert.equal(docs[0].a, 2);
                                assert.equal(docs[0]._id, newDoc._id);

                                done();
                            });
                        });
                    });
                });
            });
        });

        test('Non-multi updates are persistent', function (done) {
            d.insert({a: 1, hello: 'world'}, function (err, doc1) {
                d.insert({a: 2, hello: 'earth'}, function (err, doc2) {
                    d.update({a: 2}, {$set: {hello: 'changed'}}, {}, function (err) {
                        assert.equal(err, null);

                        d.find({}, function (err, docs) {
                            docs.sort(function (a, b) {
                                return a.a - b.a;
                            });
                            assert.equal(docs.length, 2);
                            assert.equal(_.isEqual(docs[0], {_id: doc1._id, a: 1, hello: 'world'}), true);
                            assert.equal(_.isEqual(docs[1], {_id: doc2._id, a: 2, hello: 'changed'}), true);

                            // Even after a reload the database state hasn't changed
                            d.loadDatabase(function (err) {
                                assert.equal(err, null);

                                d.find({}, function (err, docs) {
                                    docs.sort(function (a, b) {
                                        return a.a - b.a;
                                    });
                                    assert.equal(docs.length, 2);
                                    assert.equal(_.isEqual(docs[0], {_id: doc1._id, a: 1, hello: 'world'}), true);
                                    assert.equal(_.isEqual(docs[1], {_id: doc2._id, a: 2, hello: 'changed'}), true);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Multi updates are persistent', function (done) {
            d.insert({a: 1, hello: 'world'}, function (err, doc1) {
                d.insert({a: 2, hello: 'earth'}, function (err, doc2) {
                    d.insert({a: 5, hello: 'pluton'}, function (err, doc3) {
                        d.update({a: {$in: [1, 2]}}, {$set: {hello: 'changed'}}, {multi: true}, function (err) {
                            assert.equal(err, null);

                            d.find({}, function (err, docs) {
                                docs.sort(function (a, b) {
                                    return a.a - b.a;
                                });
                                assert.equal(docs.length, 3);
                                assert.equal(_.isEqual(docs[0], {_id: doc1._id, a: 1, hello: 'changed'}), true);
                                assert.equal(_.isEqual(docs[1], {_id: doc2._id, a: 2, hello: 'changed'}), true);
                                assert.equal(_.isEqual(docs[2], {_id: doc3._id, a: 5, hello: 'pluton'}), true);

                                // Even after a reload the database state hasn't changed
                                d.loadDatabase(function (err) {
                                    assert.equal(err, null);

                                    d.find({}, function (err, docs) {
                                        docs.sort(function (a, b) {
                                            return a.a - b.a;
                                        });
                                        assert.equal(docs.length, 3);
                                        assert.equal(_.isEqual(docs[0], {_id: doc1._id, a: 1, hello: 'changed'}), true);
                                        assert.equal(_.isEqual(docs[1], {_id: doc2._id, a: 2, hello: 'changed'}), true);
                                        assert.equal(_.isEqual(docs[2], {_id: doc3._id, a: 5, hello: 'pluton'}), true);

                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Can update without the options arg (will use defaults then)', function (done) {
            d.insert({a: 1, hello: 'world'}, function (err, doc1) {
                d.insert({a: 2, hello: 'earth'}, function (err, doc2) {
                    d.insert({a: 5, hello: 'pluton'}, function (err, doc3) {
                        d.update({a: 2}, {$inc: {a: 10}}, function (err, nr) {
                            assert.equal(err, null);
                            assert.equal(nr, 1);
                            d.find({}, function (err, docs) {
                                var d1 = _.find(docs, function (doc) {
                                        return doc._id === doc1._id;
                                    }),
                                    d2 = _.find(docs, function (doc) {
                                        return doc._id === doc2._id;
                                    }),
                                    d3 = _.find(docs, function (doc) {
                                        return doc._id === doc3._id;
                                    });
                                assert.equal(d1.a, 1);
                                assert.equal(d2.a, 12);
                                assert.equal(d3.a, 5);

                                done();
                            });
                        });
                    });
                });
            });
        });

        test('If a multi update fails on one document, previous updates should be rolled back', function (done) {
            d.ensureIndex({fieldName: 'a'});
            d.insert({a: 4}, function (err, doc1) {
                d.insert({a: 5}, function (err, doc2) {
                    d.insert({a: 'abc'}, function (err, doc3) {
                        // With this query, candidates are always returned in the order 4, 5, 'abc' so it's always the last one which fails
                        d.update({a: {$in: [4, 5, 'abc']}}, {$inc: {a: 10}}, {multi: true}, function (err) {
                            assert.equal(err instanceof Error, true);

                            // No index modified
                            _.each(d.indexes, function (index) {
                                var docs = index.getAll(),
                                    d1 = _.find(docs, function (doc) {
                                        return doc._id === doc1._id;
                                    }),
                                    d2 = _.find(docs, function (doc) {
                                        return doc._id === doc2._id;
                                    }),
                                    d3 = _.find(docs, function (doc) {
                                        return doc._id === doc3._id;
                                    });
                                // All changes rolled back, including those that didn't trigger an error
                                assert.equal(d1.a, 4);
                                assert.equal(d2.a, 5);
                                assert.equal(d3.a, 'abc');
                            });

                            done();
                        });
                    });
                });
            });
        });

        test('If an index constraint is violated by an update, all changes should be rolled back', function (done) {
            d.ensureIndex({fieldName: 'a', unique: true});
            d.insert({a: 4}, function (err, doc1) {
                d.insert({a: 5}, function (err, doc2) {
                    // With this query, candidates are always returned in the order 4, 5, 'abc' so it's always the last one which fails
                    d.update({a: {$in: [4, 5, 'abc']}}, {$set: {a: 10}}, {multi: true}, function (err) {
                        assert.equal(err instanceof Error, true);

                        // Check that no index was modified
                        _.each(d.indexes, function (index) {
                            var docs = index.getAll(),
                                d1 = _.find(docs, function (doc) {
                                    return doc._id === doc1._id;
                                }),
                                d2 = _.find(docs, function (doc) {
                                    return doc._id === doc2._id;
                                });
                            assert.equal(d1.a, 4);
                            assert.equal(d2.a, 5);
                        });

                        done();
                    });
                });
            });
        });

        test('If options.returnUpdatedDocs is true, return all matched docs', function (done) {
            d.insert([{a: 4}, {a: 5}, {a: 6}], function (err, docs) {
                assert.equal(docs.length, 3);

                d.update(
                    {a: 7},
                    {$set: {u: 1}},
                    {multi: true, returnUpdatedDocs: true},
                    function (err, num, updatedDocs) {
                        assert.equal(num, 0);
                        assert.equal(updatedDocs.length, 0);

                        d.update(
                            {a: 5},
                            {$set: {u: 2}},
                            {multi: true, returnUpdatedDocs: true},
                            function (err, num, updatedDocs) {
                                assert.equal(num, 1);
                                assert.equal(updatedDocs.length, 1);
                                assert.equal(updatedDocs[0].a, 5);
                                assert.equal(updatedDocs[0].u, 2);

                                d.update(
                                    {a: {$in: [4, 6]}},
                                    {$set: {u: 3}},
                                    {multi: true, returnUpdatedDocs: true},
                                    function (err, num, updatedDocs) {
                                        assert.equal(num, 2);
                                        assert.equal(updatedDocs.length, 2);
                                        assert.equal(updatedDocs[0].u, 3);
                                        assert.equal(updatedDocs[1].u, 3);
                                        if (updatedDocs[0].a === 4) {
                                            assert.equal(updatedDocs[0].a, 4);
                                            assert.equal(updatedDocs[1].a, 6);
                                        } else {
                                            assert.equal(updatedDocs[0].a, 6);
                                            assert.equal(updatedDocs[1].a, 4);
                                        }

                                        done();
                                    }
                                );
                            }
                        );
                    }
                );
            });
        });

        test('createdAt property is unchanged and updatedAt correct after an update, even a complete document replacement', function (done) {
            var d2 = new Datastore({inMemoryOnly: true, timestampData: true});
            d2.insert({a: 1});
            d2.findOne({a: 1}, function (err, doc) {
                var createdAt = doc.createdAt.getTime();

                // Modifying update
                setTimeout(function () {
                    d2.update({a: 1}, {$set: {b: 2}}, {});
                    d2.findOne({a: 1}, function (err, doc) {
                        assert.equal(doc.createdAt.getTime(), createdAt);
                        assert.equal(Date.now() - doc.updatedAt.getTime()< 5, true);

                        // Complete replacement
                        setTimeout(function () {
                            d2.update({a: 1}, {c: 3}, {});
                            d2.findOne({c: 3}, function (err, doc) {
                                assert.equal(doc.createdAt.getTime(), createdAt);
                                assert.equal(Date.now() - doc.updatedAt.getTime()< 5, true);

                                done();
                            });
                        }, 20);
                    });
                }, 20);
            });
        });

        describe('Callback signature', function () {
            test('Regular update, multi false', function (done) {
                d.insert({a: 1});
                d.insert({a: 2});

                // returnUpdatedDocs set to false
                d.update({a: 1}, {$set: {b: 20}}, {}, function (err, numAffected, affectedDocuments, upsert) {
                    assert.equal(err, null);
                    assert.equal(numAffected, 1);
                    assert.equal(affectedDocuments, undefined);
                    assert.equal(upsert, undefined);

                    // returnUpdatedDocs set to true
                    d.update(
                        {a: 1},
                        {$set: {b: 21}},
                        {returnUpdatedDocs: true},
                        function (err, numAffected, affectedDocuments, upsert) {
                            assert.equal(err, null);
                            assert.equal(numAffected, 1);
                            assert.equal(affectedDocuments.a, 1);
                            assert.equal(affectedDocuments.b, 21);
                            assert.equal(upsert, undefined);

                            done();
                        }
                    );
                });
            });

            test('Regular update, multi true', function (done) {
                d.insert({a: 1});
                d.insert({a: 2});

                // returnUpdatedDocs set to false
                d.update({}, {$set: {b: 20}}, {multi: true}, function (err, numAffected, affectedDocuments, upsert) {
                    assert.equal(err, null);
                    assert.equal(numAffected, 2);
                    assert.equal(affectedDocuments, undefined);
                    assert.equal(upsert, undefined);

                    // returnUpdatedDocs set to true
                    d.update(
                        {},
                        {$set: {b: 21}},
                        {multi: true, returnUpdatedDocs: true},
                        function (err, numAffected, affectedDocuments, upsert) {
                            assert.equal(err, null);
                            assert.equal(numAffected, 2);
                            assert.equal(affectedDocuments.length, 2);
                            assert.equal(upsert, undefined);

                            done();
                        }
                    );
                });
            });

            test('Upsert', function (done) {
                d.insert({a: 1});
                d.insert({a: 2});

                // Upsert flag not set
                d.update({a: 3}, {$set: {b: 20}}, {}, function (err, numAffected, affectedDocuments, upsert) {
                    assert.equal(err, null);
                    assert.equal(numAffected, 0);
                    assert.equal(affectedDocuments, undefined);
                    assert.equal(upsert, undefined);

                    // Upsert flag set
                    d.update(
                        {a: 3},
                        {$set: {b: 21}},
                        {upsert: true},
                        function (err, numAffected, affectedDocuments, upsert) {
                            assert.equal(err, null);
                            assert.equal(numAffected, 1);
                            assert.equal(affectedDocuments.a, 3);
                            assert.equal(affectedDocuments.b, 21);
                            assert.equal(upsert, true);

                            d.find({}, function (err, docs) {
                                assert.equal(docs.length, 3);
                                done();
                            });
                        }
                    );
                });
            });
        }); // ==== End of 'Update - Callback signature' ==== //
    }); // ==== End of 'Update' ==== //

    describe('Remove', function () {
        test('Can remove multiple documents', function (done) {
            var id1, id2, id3;

            // Test DB status
            function testPostUpdateState(cb) {
                d.find({}, function (err, docs) {
                    assert.equal(docs.length, 1);

                    assert.equal(Object.keys(docs[0]).length, 2);
                    assert.equal(docs[0]._id, id1);
                    assert.equal(docs[0].somedata, 'ok');

                    return cb();
                });
            }

            // Actually launch the test
            async.waterfall(
                [
                    function (cb) {
                        d.insert({somedata: 'ok'}, function (err, doc1) {
                            id1 = doc1._id;
                            d.insert({somedata: 'again', plus: 'additional data'}, function (err, doc2) {
                                id2 = doc2._id;
                                d.insert({somedata: 'again'}, function (err, doc3) {
                                    id3 = doc3._id;
                                    return cb(err);
                                });
                            });
                        });
                    },
                    function (cb) {
                        // Test with query that doesn't match anything
                        d.remove({somedata: 'again'}, {multi: true}, function (err, n) {
                            assert.equal(err, null);
                            assert.equal(n, 2);
                            return cb();
                        });
                    },
                    async.apply(testPostUpdateState),
                    function (cb) {
                        d.loadDatabase(function (err) {
                            return cb(err);
                        });
                    },
                    async.apply(testPostUpdateState),
                ],
                done
            );
        });

        // This tests concurrency issues
        test('Remove can be called multiple times in parallel and everything that needs to be removed will be', function (done) {
            d.insert({planet: 'Earth'}, function () {
                d.insert({planet: 'Mars'}, function () {
                    d.insert({planet: 'Saturn'}, function () {
                        d.find({}, function (err, docs) {
                            assert.equal(docs.length, 3);

                            // Remove two docs simultaneously
                            var toRemove = ['Mars', 'Saturn'];
                            async.each(
                                toRemove,
                                function (planet, cb) {
                                    d.remove({planet: planet}, function (err) {
                                        return cb(err);
                                    });
                                },
                                function (err) {
                                    d.find({}, function (err, docs) {
                                        assert.equal(docs.length, 1);

                                        done();
                                    });
                                }
                            );
                        });
                    });
                });
            });
        });

        test('Returns an error if the query is not well formed', function (done) {
            d.insert({hello: 'world'}, function () {
                d.remove({$or: {hello: 'world'}}, {}, function (err, nr, upsert) {
                    assert.equal(err instanceof Error, true);
                    assert.equal(nr, undefined);
                    assert.equal(upsert, undefined);

                    done();
                });
            });
        });

        test('Non-multi removes are persistent', function (done) {
            d.insert({a: 1, hello: 'world'}, function (err, doc1) {
                d.insert({a: 2, hello: 'earth'}, function (err, doc2) {
                    d.insert({a: 3, hello: 'moto'}, function (err, doc3) {
                        d.remove({a: 2}, {}, function (err) {
                            assert.equal(err, null);

                            d.find({}, function (err, docs) {
                                docs.sort(function (a, b) {
                                    return a.a - b.a;
                                });
                                assert.equal(docs.length, 2);
                                assert.equal(_.isEqual(docs[0], {_id: doc1._id, a: 1, hello: 'world'}), true);
                                assert.equal(_.isEqual(docs[1], {_id: doc3._id, a: 3, hello: 'moto'}), true);

                                // Even after a reload the database state hasn't changed
                                d.loadDatabase(function (err) {
                                    assert.equal(err, null);

                                    d.find({}, function (err, docs) {
                                        docs.sort(function (a, b) {
                                            return a.a - b.a;
                                        });
                                        assert.equal(docs.length, 2);
                                        assert.equal(_.isEqual(docs[0], {_id: doc1._id, a: 1, hello: 'world'}), true);
                                        assert.equal(_.isEqual(docs[1], {_id: doc3._id, a: 3, hello: 'moto'}), true);

                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Multi removes are persistent', function (done) {
            d.insert({a: 1, hello: 'world'}, function (err, doc1) {
                d.insert({a: 2, hello: 'earth'}, function (err, doc2) {
                    d.insert({a: 3, hello: 'moto'}, function (err, doc3) {
                        d.remove({a: {$in: [1, 3]}}, {multi: true}, function (err) {
                            assert.equal(err, null);

                            d.find({}, function (err, docs) {
                                assert.equal(docs.length, 1);
                                assert.equal(_.isEqual(docs[0], {_id: doc2._id, a: 2, hello: 'earth'}), true);

                                // Even after a reload the database state hasn't changed
                                d.loadDatabase(function (err) {
                                    assert.equal(err, null);

                                    d.find({}, function (err, docs) {
                                        assert.equal(docs.length, 1);
                                        assert.equal(_.isEqual(docs[0], {_id: doc2._id, a: 2, hello: 'earth'}), true);

                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        test('Can remove without the options arg (will use defaults then)', function (done) {
            d.insert({a: 1, hello: 'world'}, function (err, doc1) {
                d.insert({a: 2, hello: 'earth'}, function (err, doc2) {
                    d.insert({a: 5, hello: 'pluton'}, function (err, doc3) {
                        d.remove({a: 2}, function (err, nr) {
                            assert.equal(err, null);
                            assert.equal(nr, 1);
                            d.find({}, function (err, docs) {
                                var d1 = _.find(docs, function (doc) {
                                        return doc._id === doc1._id;
                                    }),
                                    d2 = _.find(docs, function (doc) {
                                        return doc._id === doc2._id;
                                    }),
                                    d3 = _.find(docs, function (doc) {
                                        return doc._id === doc3._id;
                                    });
                                assert.equal(d1.a, 1);
                                assert.equal(d2, undefined);
                                assert.equal(d3.a, 5);

                                done();
                            });
                        });
                    });
                });
            });
        });
    }); // ==== End of 'Remove' ==== //

    describe('Using indexes', function () {
        describe('ensureIndex and index initialization in database loading', function () {
            test('ensureIndex can be called right after a loadDatabase and be initialized and filled correctly', function (done) {
                var now = new Date(),
                    rawData =
                        model.serialize({_id: 'aaa', z: '1', a: 2, ages: [1, 5, 12]}) +
                        '\n' +
                        model.serialize({_id: 'bbb', z: '2', hello: 'world'}) +
                        '\n' +
                        model.serialize({_id: 'ccc', z: '3', nested: {today: now}});
                assert.equal(d.getAllData().length, 0);

                fs.writeFile(testDb, rawData, 'utf8', function () {
                    d.loadDatabase(function () {
                        assert.equal(d.getAllData().length, 3);

                        assert.deepEqual(Object.keys(d.indexes), ['_id']);

                        d.ensureIndex({fieldName: 'z'});
                        assert.equal(d.indexes.z.fieldName, 'z');
                        assert.equal(d.indexes.z.unique, false);
                        assert.equal(d.indexes.z.sparse, false);
                        assert.equal(d.indexes.z.tree.getNumberOfKeys(), 3);
                        assert.equal(d.indexes.z.tree.search('1')[0], d.getAllData()[0]);
                        assert.equal(d.indexes.z.tree.search('2')[0], d.getAllData()[1]);
                        assert.equal(d.indexes.z.tree.search('3')[0], d.getAllData()[2]);

                        done();
                    });
                });
            });

            test('ensureIndex can be called twice on the same field, the second call will ahve no effect', function (done) {
                assert.equal(Object.keys(d.indexes).length, 1);
                assert.equal(Object.keys(d.indexes)[0], '_id');

                d.insert({planet: 'Earth'}, function () {
                    d.insert({planet: 'Mars'}, function () {
                        d.find({}, function (err, docs) {
                            assert.equal(docs.length, 2);

                            d.ensureIndex({fieldName: 'planet'}, function (err) {
                                assert.equal(err, null);
                                assert.equal(Object.keys(d.indexes).length, 2);
                                assert.equal(Object.keys(d.indexes)[0], '_id');
                                assert.equal(Object.keys(d.indexes)[1], 'planet');

                                assert.equal(d.indexes.planet.getAll().length, 2);

                                // This second call has no effect, documents don't get inserted twice in the index
                                d.ensureIndex({fieldName: 'planet'}, function (err) {
                                    assert.equal(err, null);
                                    assert.equal(Object.keys(d.indexes).length, 2);
                                    assert.equal(Object.keys(d.indexes)[0], '_id');
                                    assert.equal(Object.keys(d.indexes)[1], 'planet');

                                    assert.equal(d.indexes.planet.getAll().length, 2);

                                    done();
                                });
                            });
                        });
                    });
                });
            });

            test('ensureIndex can be called after the data set was modified and the index still be correct', function (done) {
                var rawData =
                    model.serialize({_id: 'aaa', z: '1', a: 2, ages: [1, 5, 12]}) +
                    '\n' +
                    model.serialize({_id: 'bbb', z: '2', hello: 'world'});
                assert.equal(d.getAllData().length, 0);

                fs.writeFile(testDb, rawData, 'utf8', function () {
                    d.loadDatabase(function () {
                        assert.equal(d.getAllData().length, 2);

                        assert.deepEqual(Object.keys(d.indexes), ['_id']);

                        d.insert({z: '12', yes: 'yes'}, function (err, newDoc1) {
                            d.insert({z: '14', nope: 'nope'}, function (err, newDoc2) {
                                d.remove({z: '2'}, {}, function () {
                                    d.update({z: '1'}, {$set: {'yes': 'yep'}}, {}, function () {
                                        assert.deepEqual(Object.keys(d.indexes), ['_id']);

                                        d.ensureIndex({fieldName: 'z'});
                                        assert.equal(d.indexes.z.fieldName, 'z');
                                        assert.equal(d.indexes.z.unique, false);
                                        assert.equal(d.indexes.z.sparse, false);
                                        assert.equal(d.indexes.z.tree.getNumberOfKeys(), 3);

                                        // The pointers in the _id and z indexes are the same
                                        assert.equal(
                                            d.indexes.z.tree.search('1')[0],
                                            d.indexes._id.getMatching('aaa')[0]
                                        );
                                        assert.equal(
                                            d.indexes.z.tree.search('12')[0],
                                            d.indexes._id.getMatching(newDoc1._id)[0]
                                        );
                                        assert.equal(
                                            d.indexes.z.tree.search('14')[0],
                                            d.indexes._id.getMatching(newDoc2._id)[0]
                                        );

                                        // The data in the z index is correct
                                        d.find({}, function (err, docs) {
                                            var doc0 = _.find(docs, function (doc) {
                                                    return doc._id === 'aaa';
                                                }),
                                                doc1 = _.find(docs, function (doc) {
                                                    return doc._id === newDoc1._id;
                                                }),
                                                doc2 = _.find(docs, function (doc) {
                                                    return doc._id === newDoc2._id;
                                                });
                                            assert.equal(docs.length, 3);

                                            assert.deepEqual(doc0, {
                                                _id: 'aaa',
                                                z: '1',
                                                a: 2,
                                                ages: [1, 5, 12],
                                                yes: 'yep',
                                            });
                                            assert.deepEqual(doc1, {_id: newDoc1._id, z: '12', yes: 'yes'});
                                            assert.deepEqual(doc2, {_id: newDoc2._id, z: '14', nope: 'nope'});

                                            done();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });

            test('ensureIndex can be called before a loadDatabase and still be initialized and filled correctly', function (done) {
                var now = new Date(),
                    rawData =
                        model.serialize({_id: 'aaa', z: '1', a: 2, ages: [1, 5, 12]}) +
                        '\n' +
                        model.serialize({_id: 'bbb', z: '2', hello: 'world'}) +
                        '\n' +
                        model.serialize({_id: 'ccc', z: '3', nested: {today: now}});
                assert.equal(d.getAllData().length, 0);

                d.ensureIndex({fieldName: 'z'});
                assert.equal(d.indexes.z.fieldName, 'z');
                assert.equal(d.indexes.z.unique, false);
                assert.equal(d.indexes.z.sparse, false);
                assert.equal(d.indexes.z.tree.getNumberOfKeys(), 0);

                fs.writeFile(testDb, rawData, 'utf8', function () {
                    d.loadDatabase(function () {
                        var doc1 = _.find(d.getAllData(), function (doc) {
                                return doc.z === '1';
                            }),
                            doc2 = _.find(d.getAllData(), function (doc) {
                                return doc.z === '2';
                            }),
                            doc3 = _.find(d.getAllData(), function (doc) {
                                return doc.z === '3';
                            });
                        assert.equal(d.getAllData().length, 3);

                        assert.equal(d.indexes.z.tree.getNumberOfKeys(), 3);
                        assert.equal(d.indexes.z.tree.search('1')[0], doc1);
                        assert.equal(d.indexes.z.tree.search('2')[0], doc2);
                        assert.equal(d.indexes.z.tree.search('3')[0], doc3);

                        done();
                    });
                });
            });

            test('Can initialize multiple indexes on a database load', function (done) {
                var now = new Date(),
                    rawData =
                        model.serialize({_id: 'aaa', z: '1', a: 2, ages: [1, 5, 12]}) +
                        '\n' +
                        model.serialize({_id: 'bbb', z: '2', a: 'world'}) +
                        '\n' +
                        model.serialize({_id: 'ccc', z: '3', a: {today: now}});
                assert.equal(d.getAllData().length, 0);
                d.ensureIndex({fieldName: 'z'}, function () {
                    d.ensureIndex({fieldName: 'a'}, function () {
                        assert.equal(d.indexes.a.tree.getNumberOfKeys(), 0);
                        assert.equal(d.indexes.z.tree.getNumberOfKeys(), 0);

                        fs.writeFile(testDb, rawData, 'utf8', function () {
                            d.loadDatabase(function (err) {
                                var doc1 = _.find(d.getAllData(), function (doc) {
                                        return doc.z === '1';
                                    }),
                                    doc2 = _.find(d.getAllData(), function (doc) {
                                        return doc.z === '2';
                                    }),
                                    doc3 = _.find(d.getAllData(), function (doc) {
                                        return doc.z === '3';
                                    });
                                assert.equal(err, null);
                                assert.equal(d.getAllData().length, 3);

                                assert.equal(d.indexes.z.tree.getNumberOfKeys(), 3);
                                assert.equal(d.indexes.z.tree.search('1')[0], doc1);
                                assert.equal(d.indexes.z.tree.search('2')[0], doc2);
                                assert.equal(d.indexes.z.tree.search('3')[0], doc3);

                                assert.equal(d.indexes.a.tree.getNumberOfKeys(), 3);
                                assert.equal(d.indexes.a.tree.search(2)[0], doc1);
                                assert.equal(d.indexes.a.tree.search('world')[0], doc2);
                                assert.equal(d.indexes.a.tree.search({today: now})[0], doc3);

                                done();
                            });
                        });
                    });
                });
            });

            test('If a unique constraint is not respected, database loading will not work and no data will be inserted', function (done) {
                var now = new Date(),
                    rawData =
                        model.serialize({_id: 'aaa', z: '1', a: 2, ages: [1, 5, 12]}) +
                        '\n' +
                        model.serialize({_id: 'bbb', z: '2', a: 'world'}) +
                        '\n' +
                        model.serialize({_id: 'ccc', z: '1', a: {today: now}});
                assert.equal(d.getAllData().length, 0);

                d.ensureIndex({fieldName: 'z', unique: true});
                assert.equal(d.indexes.z.tree.getNumberOfKeys(), 0);

                fs.writeFile(testDb, rawData, 'utf8', function () {
                    // WARNING: in original it was almost synchronously
                    setTimeout(() => {
                        d.loadDatabase(function (err) {
                            assert.equal(err.errorType, 'uniqueViolated');
                            assert.equal(err.key, '1');
                            assert.equal(d.getAllData().length, 0);
                            assert.equal(d.indexes.z.tree.getNumberOfKeys(), 0);

                            done();
                        });
                    }, 10);
                });
            });

            test('If a unique constraint is not respected, ensureIndex will return an error and not create an index', function (done) {
                d.insert({a: 1, b: 4}, function () {
                    d.insert({a: 2, b: 45}, function () {
                        d.insert({a: 1, b: 3}, function () {
                            d.ensureIndex({fieldName: 'b'}, function (err) {
                                assert.equal(err, null);

                                d.ensureIndex({fieldName: 'a', unique: true}, function (err) {
                                    assert.equal(err.errorType, 'uniqueViolated');
                                    assert.deepEqual(Object.keys(d.indexes), ['_id', 'b']);

                                    done();
                                });
                            });
                        });
                    });
                });
            });

            test('Can remove an index', function (done) {
                d.ensureIndex({fieldName: 'e'}, function (err) {
                    assert.equal(err, null);

                    assert.equal(Object.keys(d.indexes).length, 2);
                    assert.notEqual(d.indexes.e, null);

                    d.removeIndex('e', function (err) {
                        assert.equal(err, null);
                        assert.equal(Object.keys(d.indexes).length, 1);
                        assert.equal(d.indexes.e, undefined);

                        done();
                    });
                });
            });
        }); // ==== End of 'ensureIndex and index initialization in database loading' ==== //

        describe('Indexing newly inserted documents', function () {
            test('Newly inserted documents are indexed', function (done) {
                d.ensureIndex({fieldName: 'z'});
                assert.equal(d.indexes.z.tree.getNumberOfKeys(), 0);

                d.insert({a: 2, z: 'yes'}, function (err, newDoc) {
                    assert.equal(d.indexes.z.tree.getNumberOfKeys(), 1);
                    assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);

                    d.insert({a: 5, z: 'nope'}, function (err, newDoc) {
                        assert.equal(d.indexes.z.tree.getNumberOfKeys(), 2);
                        assert.deepEqual(d.indexes.z.getMatching('nope'), [newDoc]);

                        done();
                    });
                });
            });

            test('If multiple indexes are defined, the document is inserted in all of them', function (done) {
                d.ensureIndex({fieldName: 'z'});
                d.ensureIndex({fieldName: 'ya'});
                assert.equal(d.indexes.z.tree.getNumberOfKeys(), 0);

                d.insert({a: 2, z: 'yes', ya: 'indeed'}, function (err, newDoc) {
                    assert.equal(d.indexes.z.tree.getNumberOfKeys(), 1);
                    assert.equal(d.indexes.ya.tree.getNumberOfKeys(), 1);
                    assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);
                    assert.deepEqual(d.indexes.ya.getMatching('indeed'), [newDoc]);

                    d.insert({a: 5, z: 'nope', ya: 'sure'}, function (err, newDoc2) {
                        assert.equal(d.indexes.z.tree.getNumberOfKeys(), 2);
                        assert.equal(d.indexes.ya.tree.getNumberOfKeys(), 2);
                        assert.deepEqual(d.indexes.z.getMatching('nope'), [newDoc2]);
                        assert.deepEqual(d.indexes.ya.getMatching('sure'), [newDoc2]);

                        done();
                    });
                });
            });

            test('Can insert two docs at the same key for a non unique index', function (done) {
                d.ensureIndex({fieldName: 'z'});
                assert.equal(d.indexes.z.tree.getNumberOfKeys(), 0);

                d.insert({a: 2, z: 'yes'}, function (err, newDoc) {
                    assert.equal(d.indexes.z.tree.getNumberOfKeys(), 1);
                    assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);

                    d.insert({a: 5, z: 'yes'}, function (err, newDoc2) {
                        assert.equal(d.indexes.z.tree.getNumberOfKeys(), 1);
                        assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc, newDoc2]);

                        done();
                    });
                });
            });

            test('If the index has a unique constraint, an error is thrown if it is violated and the data is not modified', function (done) {
                d.ensureIndex({fieldName: 'z', unique: true});
                assert.equal(d.indexes.z.tree.getNumberOfKeys(), 0);

                d.insert({a: 2, z: 'yes'}, function (err, newDoc) {
                    assert.equal(d.indexes.z.tree.getNumberOfKeys(), 1);
                    assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);

                    d.insert({a: 5, z: 'yes'}, function (err) {
                        assert.equal(err.errorType, 'uniqueViolated');
                        assert.equal(err.key, 'yes');

                        // Index didn't change
                        assert.equal(d.indexes.z.tree.getNumberOfKeys(), 1);
                        assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);

                        // Data didn't change
                        assert.deepEqual(d.getAllData(), [newDoc]);
                        d.loadDatabase(function () {
                            assert.equal(d.getAllData().length, 1);
                            assert.deepEqual(d.getAllData()[0], newDoc);

                            done();
                        });
                    });
                });
            });

            test('If an index has a unique constraint, other indexes cannot be modified when it raises an error', function (done) {
                d.ensureIndex({fieldName: 'nonu1'});
                d.ensureIndex({fieldName: 'uni', unique: true});
                d.ensureIndex({fieldName: 'nonu2'});

                d.insert({nonu1: 'yes', nonu2: 'yes2', uni: 'willfail'}, function (err, newDoc) {
                    assert.equal(err, null);
                    assert.equal(d.indexes.nonu1.tree.getNumberOfKeys(), 1);
                    assert.equal(d.indexes.uni.tree.getNumberOfKeys(), 1);
                    assert.equal(d.indexes.nonu2.tree.getNumberOfKeys(), 1);

                    d.insert({nonu1: 'no', nonu2: 'no2', uni: 'willfail'}, function (err) {
                        assert.equal(err.errorType, 'uniqueViolated');

                        // No index was modified
                        assert.equal(d.indexes.nonu1.tree.getNumberOfKeys(), 1);
                        assert.equal(d.indexes.uni.tree.getNumberOfKeys(), 1);
                        assert.equal(d.indexes.nonu2.tree.getNumberOfKeys(), 1);

                        assert.deepEqual(d.indexes.nonu1.getMatching('yes'), [newDoc]);
                        assert.deepEqual(d.indexes.uni.getMatching('willfail'), [newDoc]);
                        assert.deepEqual(d.indexes.nonu2.getMatching('yes2'), [newDoc]);

                        done();
                    });
                });
            });

            test('Unique indexes prevent you from inserting two docs where the field is undefined except if theyre sparse', function (done) {
                d.ensureIndex({fieldName: 'zzz', unique: true});
                assert.equal(d.indexes.zzz.tree.getNumberOfKeys(), 0);

                d.insert({a: 2, z: 'yes'}, function (err, newDoc) {
                    assert.equal(d.indexes.zzz.tree.getNumberOfKeys(), 1);
                    assert.deepEqual(d.indexes.zzz.getMatching(undefined), [newDoc]);

                    d.insert({a: 5, z: 'other'}, function (err) {
                        assert.equal(err.errorType, 'uniqueViolated');
                        assert.equal(err.key, undefined);

                        d.ensureIndex({fieldName: 'yyy', unique: true, sparse: true});

                        d.insert({a: 5, z: 'other', zzz: 'set'}, function (err) {
                            assert.equal(err, null);
                            assert.equal(d.indexes.yyy.getAll().length, 0); // Nothing indexed
                            assert.equal(d.indexes.zzz.getAll().length, 2);

                            done();
                        });
                    });
                });
            });

            test('Insertion still works as before with indexing', function (done) {
                d.ensureIndex({fieldName: 'a'});
                d.ensureIndex({fieldName: 'b'});

                d.insert({a: 1, b: 'hello'}, function (err, doc1) {
                    d.insert({a: 2, b: 'si'}, function (err, doc2) {
                        d.find({}, function (err, docs) {
                            assert.deepEqual(
                                doc1,
                                _.find(docs, function (d) {
                                    return d._id === doc1._id;
                                })
                            );
                            assert.deepEqual(
                                doc2,
                                _.find(docs, function (d) {
                                    return d._id === doc2._id;
                                })
                            );

                            done();
                        });
                    });
                });
            });

            test('All indexes point to the same data as the main index on _id', function (done) {
                d.ensureIndex({fieldName: 'a'});

                d.insert({a: 1, b: 'hello'}, function (err, doc1) {
                    d.insert({a: 2, b: 'si'}, function (err, doc2) {
                        d.find({}, function (err, docs) {
                            assert.equal(docs.length, 2);
                            assert.equal(d.getAllData().length, 2);

                            assert.equal(d.indexes._id.getMatching(doc1._id).length, 1);
                            assert.equal(d.indexes.a.getMatching(1).length, 1);
                            assert.equal(d.indexes._id.getMatching(doc1._id)[0], d.indexes.a.getMatching(1)[0]);

                            assert.equal(d.indexes._id.getMatching(doc2._id).length, 1);
                            assert.equal(d.indexes.a.getMatching(2).length, 1);
                            assert.equal(d.indexes._id.getMatching(doc2._id)[0], d.indexes.a.getMatching(2)[0]);

                            done();
                        });
                    });
                });
            });

            test('If a unique constraint is violated, no index is changed, including the main one', function (done) {
                d.ensureIndex({fieldName: 'a', unique: true});

                d.insert({a: 1, b: 'hello'}, function (err, doc1) {
                    d.insert({a: 1, b: 'si'}, function (err) {
                        assert.equal(err instanceof Error, true);

                        d.find({}, function (err, docs) {
                            assert.equal(docs.length, 1);
                            assert.equal(d.getAllData().length, 1);

                            assert.equal(d.indexes._id.getMatching(doc1._id).length, 1);
                            assert.equal(d.indexes.a.getMatching(1).length, 1);
                            assert.equal(d.indexes._id.getMatching(doc1._id)[0], d.indexes.a.getMatching(1)[0]);

                            assert.equal(d.indexes.a.getMatching(2).length, 0);

                            done();
                        });
                    });
                });
            });
        }); // ==== End of 'Indexing newly inserted documents' ==== //

        describe('Updating indexes upon document update', function () {
            test('Updating docs still works as before with indexing', function (done) {
                d.ensureIndex({fieldName: 'a'});

                d.insert({a: 1, b: 'hello'}, function (err, _doc1) {
                    d.insert({a: 2, b: 'si'}, function (err, _doc2) {
                        d.update({a: 1}, {$set: {a: 456, b: 'no'}}, {}, function (err, nr) {
                            var data = d.getAllData(),
                                doc1 = _.find(data, function (doc) {
                                    return doc._id === _doc1._id;
                                }),
                                doc2 = _.find(data, function (doc) {
                                    return doc._id === _doc2._id;
                                });
                            assert.equal(err, null);
                            assert.equal(nr, 1);

                            assert.equal(data.length, 2);
                            assert.deepEqual(doc1, {a: 456, b: 'no', _id: _doc1._id});
                            assert.deepEqual(doc2, {a: 2, b: 'si', _id: _doc2._id});

                            d.update({}, {$inc: {a: 10}, $set: {b: 'same'}}, {multi: true}, function (err, nr) {
                                var data = d.getAllData(),
                                    doc1 = _.find(data, function (doc) {
                                        return doc._id === _doc1._id;
                                    }),
                                    doc2 = _.find(data, function (doc) {
                                        return doc._id === _doc2._id;
                                    });
                                assert.equal(err, null);
                                assert.equal(nr, 2);

                                assert.equal(data.length, 2);
                                assert.deepEqual(doc1, {a: 466, b: 'same', _id: _doc1._id});
                                assert.deepEqual(doc2, {a: 12, b: 'same', _id: _doc2._id});

                                done();
                            });
                        });
                    });
                });
            });

            test('Indexes get updated when a document (or multiple documents) is updated', function (done) {
                d.ensureIndex({fieldName: 'a'});
                d.ensureIndex({fieldName: 'b'});

                d.insert({a: 1, b: 'hello'}, function (err, doc1) {
                    d.insert({a: 2, b: 'si'}, function (err, doc2) {
                        // Simple update
                        d.update({a: 1}, {$set: {a: 456, b: 'no'}}, {}, function (err, nr) {
                            assert.equal(err, null);
                            assert.equal(nr, 1);

                            assert.equal(d.indexes.a.tree.getNumberOfKeys(), 2);
                            assert.equal(d.indexes.a.getMatching(456)[0]._id, doc1._id);
                            assert.equal(d.indexes.a.getMatching(2)[0]._id, doc2._id);

                            assert.equal(d.indexes.b.tree.getNumberOfKeys(), 2);
                            assert.equal(d.indexes.b.getMatching('no')[0]._id, doc1._id);
                            assert.equal(d.indexes.b.getMatching('si')[0]._id, doc2._id);

                            // The same pointers are shared between all indexes
                            assert.equal(d.indexes.a.tree.getNumberOfKeys(), 2);
                            assert.equal(d.indexes.b.tree.getNumberOfKeys(), 2);
                            assert.equal(d.indexes._id.tree.getNumberOfKeys(), 2);
                            assert.equal(d.indexes.a.getMatching(456)[0], d.indexes._id.getMatching(doc1._id)[0]);
                            assert.equal(d.indexes.b.getMatching('no')[0], d.indexes._id.getMatching(doc1._id)[0]);
                            assert.equal(d.indexes.a.getMatching(2)[0], d.indexes._id.getMatching(doc2._id)[0]);
                            assert.equal(d.indexes.b.getMatching('si')[0], d.indexes._id.getMatching(doc2._id)[0]);

                            // Multi update
                            d.update({}, {$inc: {a: 10}, $set: {b: 'same'}}, {multi: true}, function (err, nr) {
                                assert.equal(err, null);
                                assert.equal(nr, 2);

                                assert.equal(d.indexes.a.tree.getNumberOfKeys(), 2);
                                assert.equal(d.indexes.a.getMatching(466)[0]._id, doc1._id);
                                assert.equal(d.indexes.a.getMatching(12)[0]._id, doc2._id);

                                assert.equal(d.indexes.b.tree.getNumberOfKeys(), 1);
                                assert.equal(d.indexes.b.getMatching('same').length, 2);
                                assert.equal(_.pluck(d.indexes.b.getMatching('same'), '_id').includes(doc1._id), true);
                                assert.equal(_.pluck(d.indexes.b.getMatching('same'), '_id').includes(doc2._id), true);

                                // The same pointers are shared between all indexes
                                assert.equal(d.indexes.a.tree.getNumberOfKeys(), 2);
                                assert.equal(d.indexes.b.tree.getNumberOfKeys(), 1);
                                assert.equal(d.indexes.b.getAll().length, 2);
                                assert.equal(d.indexes._id.tree.getNumberOfKeys(), 2);
                                assert.equal(d.indexes.a.getMatching(466)[0], d.indexes._id.getMatching(doc1._id)[0]);
                                assert.equal(d.indexes.a.getMatching(12)[0], d.indexes._id.getMatching(doc2._id)[0]);
                                // Can't test the pointers in b as their order is randomized, but it is the same as with a

                                done();
                            });
                        });
                    });
                });
            });

            test('If a simple update violates a contraint, all changes are rolled back and an error is thrown', function (done) {
                d.ensureIndex({fieldName: 'a', unique: true});
                d.ensureIndex({fieldName: 'b', unique: true});
                d.ensureIndex({fieldName: 'c', unique: true});

                d.insert({a: 1, b: 10, c: 100}, function (err, _doc1) {
                    d.insert({a: 2, b: 20, c: 200}, function (err, _doc2) {
                        d.insert({a: 3, b: 30, c: 300}, function (err, _doc3) {
                            // Will conflict with doc3
                            d.update({a: 2}, {$inc: {a: 10, c: 1000}, $set: {b: 30}}, {}, function (err) {
                                var data = d.getAllData(),
                                    doc1 = _.find(data, function (doc) {
                                        return doc._id === _doc1._id;
                                    }),
                                    doc2 = _.find(data, function (doc) {
                                        return doc._id === _doc2._id;
                                    }),
                                    doc3 = _.find(data, function (doc) {
                                        return doc._id === _doc3._id;
                                    });
                                assert.equal(err.errorType, 'uniqueViolated');

                                // Data left unchanged
                                assert.equal(data.length, 3);
                                assert.deepEqual(doc1, {a: 1, b: 10, c: 100, _id: _doc1._id});
                                assert.deepEqual(doc2, {a: 2, b: 20, c: 200, _id: _doc2._id});
                                assert.deepEqual(doc3, {a: 3, b: 30, c: 300, _id: _doc3._id});

                                // All indexes left unchanged and pointing to the same docs
                                assert.equal(d.indexes.a.tree.getNumberOfKeys(), 3);
                                assert.equal(d.indexes.a.getMatching(1)[0], doc1);
                                assert.equal(d.indexes.a.getMatching(2)[0], doc2);
                                assert.equal(d.indexes.a.getMatching(3)[0], doc3);

                                assert.equal(d.indexes.b.tree.getNumberOfKeys(), 3);
                                assert.equal(d.indexes.b.getMatching(10)[0], doc1);
                                assert.equal(d.indexes.b.getMatching(20)[0], doc2);
                                assert.equal(d.indexes.b.getMatching(30)[0], doc3);

                                assert.equal(d.indexes.c.tree.getNumberOfKeys(), 3);
                                assert.equal(d.indexes.c.getMatching(100)[0], doc1);
                                assert.equal(d.indexes.c.getMatching(200)[0], doc2);
                                assert.equal(d.indexes.c.getMatching(300)[0], doc3);

                                done();
                            });
                        });
                    });
                });
            });

            test('If a multi update violates a contraint, all changes are rolled back and an error is thrown', function (done) {
                d.ensureIndex({fieldName: 'a', unique: true});
                d.ensureIndex({fieldName: 'b', unique: true});
                d.ensureIndex({fieldName: 'c', unique: true});

                d.insert({a: 1, b: 10, c: 100}, function (err, _doc1) {
                    d.insert({a: 2, b: 20, c: 200}, function (err, _doc2) {
                        d.insert({a: 3, b: 30, c: 300}, function (err, _doc3) {
                            // Will conflict with doc3
                            d.update(
                                {a: {$in: [1, 2]}},
                                {$inc: {a: 10, c: 1000}, $set: {b: 30}},
                                {multi: true},
                                function (err) {
                                    var data = d.getAllData(),
                                        doc1 = _.find(data, function (doc) {
                                            return doc._id === _doc1._id;
                                        }),
                                        doc2 = _.find(data, function (doc) {
                                            return doc._id === _doc2._id;
                                        }),
                                        doc3 = _.find(data, function (doc) {
                                            return doc._id === _doc3._id;
                                        });
                                    assert.equal(err.errorType, 'uniqueViolated');

                                    // Data left unchanged
                                    assert.equal(data.length, 3);
                                    assert.deepEqual(doc1, {a: 1, b: 10, c: 100, _id: _doc1._id});
                                    assert.deepEqual(doc2, {a: 2, b: 20, c: 200, _id: _doc2._id});
                                    assert.deepEqual(doc3, {a: 3, b: 30, c: 300, _id: _doc3._id});

                                    // All indexes left unchanged and pointing to the same docs
                                    assert.equal(d.indexes.a.tree.getNumberOfKeys(), 3);
                                    assert.equal(d.indexes.a.getMatching(1)[0], doc1);
                                    assert.equal(d.indexes.a.getMatching(2)[0], doc2);
                                    assert.equal(d.indexes.a.getMatching(3)[0], doc3);

                                    assert.equal(d.indexes.b.tree.getNumberOfKeys(), 3);
                                    assert.equal(d.indexes.b.getMatching(10)[0], doc1);
                                    assert.equal(d.indexes.b.getMatching(20)[0], doc2);
                                    assert.equal(d.indexes.b.getMatching(30)[0], doc3);

                                    assert.equal(d.indexes.c.tree.getNumberOfKeys(), 3);
                                    assert.equal(d.indexes.c.getMatching(100)[0], doc1);
                                    assert.equal(d.indexes.c.getMatching(200)[0], doc2);
                                    assert.equal(d.indexes.c.getMatching(300)[0], doc3);

                                    done();
                                }
                            );
                        });
                    });
                });
            });
        }); // ==== End of 'Updating indexes upon document update' ==== //

        describe('Updating indexes upon document remove', function () {
            test('Removing docs still works as before with indexing', function (done) {
                d.ensureIndex({fieldName: 'a'});

                d.insert({a: 1, b: 'hello'}, function (err, _doc1) {
                    d.insert({a: 2, b: 'si'}, function (err, _doc2) {
                        d.insert({a: 3, b: 'coin'}, function (err, _doc3) {
                            d.remove({a: 1}, {}, function (err, nr) {
                                var data = d.getAllData(),
                                    doc2 = _.find(data, function (doc) {
                                        return doc._id === _doc2._id;
                                    }),
                                    doc3 = _.find(data, function (doc) {
                                        return doc._id === _doc3._id;
                                    });
                                assert.equal(err, null);
                                assert.equal(nr, 1);

                                assert.equal(data.length, 2);
                                assert.deepEqual(doc2, {a: 2, b: 'si', _id: _doc2._id});
                                assert.deepEqual(doc3, {a: 3, b: 'coin', _id: _doc3._id});

                                d.remove({a: {$in: [2, 3]}}, {multi: true}, function (err, nr) {
                                    var data = d.getAllData();
                                    assert.equal(err, null);
                                    assert.equal(nr, 2);
                                    assert.equal(data.length, 0);

                                    done();
                                });
                            });
                        });
                    });
                });
            });

            test('Indexes get updated when a document (or multiple documents) is removed', function (done) {
                d.ensureIndex({fieldName: 'a'});
                d.ensureIndex({fieldName: 'b'});

                d.insert({a: 1, b: 'hello'}, function (err, doc1) {
                    d.insert({a: 2, b: 'si'}, function (err, doc2) {
                        d.insert({a: 3, b: 'coin'}, function (err, doc3) {
                            // Simple remove
                            d.remove({a: 1}, {}, function (err, nr) {
                                assert.equal(err, null);
                                assert.equal(nr, 1);

                                assert.equal(d.indexes.a.tree.getNumberOfKeys(), 2);
                                assert.equal(d.indexes.a.getMatching(2)[0]._id, doc2._id);
                                assert.equal(d.indexes.a.getMatching(3)[0]._id, doc3._id);

                                assert.equal(d.indexes.b.tree.getNumberOfKeys(), 2);
                                assert.equal(d.indexes.b.getMatching('si')[0]._id, doc2._id);
                                assert.equal(d.indexes.b.getMatching('coin')[0]._id, doc3._id);

                                // The same pointers are shared between all indexes
                                assert.equal(d.indexes.a.tree.getNumberOfKeys(), 2);
                                assert.equal(d.indexes.b.tree.getNumberOfKeys(), 2);
                                assert.equal(d.indexes._id.tree.getNumberOfKeys(), 2);
                                assert.equal(d.indexes.a.getMatching(2)[0], d.indexes._id.getMatching(doc2._id)[0]);
                                assert.equal(d.indexes.b.getMatching('si')[0], d.indexes._id.getMatching(doc2._id)[0]);
                                assert.equal(d.indexes.a.getMatching(3)[0], d.indexes._id.getMatching(doc3._id)[0]);
                                assert.equal(
                                    d.indexes.b.getMatching('coin')[0],
                                    d.indexes._id.getMatching(doc3._id)[0]
                                );

                                // Multi remove
                                d.remove({}, {multi: true}, function (err, nr) {
                                    assert.equal(err, null);
                                    assert.equal(nr, 2);

                                    assert.equal(d.indexes.a.tree.getNumberOfKeys(), 0);
                                    assert.equal(d.indexes.b.tree.getNumberOfKeys(), 0);
                                    assert.equal(d.indexes._id.tree.getNumberOfKeys(), 0);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        }); // ==== End of 'Updating indexes upon document remove' ==== //

        describe('Persisting indexes', function () {
            test('Indexes are persisted to a separate file and recreated upon reload', function (done) {
                var persDb = 'workspace/persistIndexes.db',
                    db;

                if (fs.existsSync(persDb)) {
                    fs.writeFileSync(persDb, '', 'utf8');
                }
                db = new Datastore({filename: persDb, autoload: true});

                assert.equal(Object.keys(db.indexes).length, 1);
                assert.equal(Object.keys(db.indexes)[0], '_id');

                db.insert({planet: 'Earth'}, function (err) {
                    assert.equal(err, null);
                    db.insert({planet: 'Mars'}, function (err) {
                        assert.equal(err, null);

                        db.ensureIndex({fieldName: 'planet'}, function (err) {
                            assert.equal(Object.keys(db.indexes).length, 2);
                            assert.equal(Object.keys(db.indexes)[0], '_id');
                            assert.equal(Object.keys(db.indexes)[1], 'planet');
                            assert.equal(db.indexes._id.getAll().length, 2);
                            assert.equal(db.indexes.planet.getAll().length, 2);
                            assert.equal(db.indexes.planet.fieldName, 'planet');

                            // After a reload the indexes are recreated
                            db = new Datastore({filename: persDb});
                            db.loadDatabase(function (err) {
                                assert.equal(err, null);
                                assert.equal(Object.keys(db.indexes).length, 2);
                                assert.equal(Object.keys(db.indexes)[0], '_id');
                                assert.equal(Object.keys(db.indexes)[1], 'planet');
                                assert.equal(db.indexes._id.getAll().length, 2);
                                assert.equal(db.indexes.planet.getAll().length, 2);
                                assert.equal(db.indexes.planet.fieldName, 'planet');

                                // After another reload the indexes are still there (i.e. they are preserved during autocompaction)
                                db = new Datastore({filename: persDb});
                                db.loadDatabase(function (err) {
                                    assert.equal(err, null);
                                    assert.equal(Object.keys(db.indexes).length, 2);
                                    assert.equal(Object.keys(db.indexes)[0], '_id');
                                    assert.equal(Object.keys(db.indexes)[1], 'planet');
                                    assert.equal(db.indexes._id.getAll().length, 2);
                                    assert.equal(db.indexes.planet.getAll().length, 2);
                                    assert.equal(db.indexes.planet.fieldName, 'planet');

                                    done();
                                });
                            });
                        });
                    });
                });
            });

            test('Indexes are persisted with their options and recreated even if some db operation happen between loads', function (done) {
                var persDb = 'workspace/persistIndexes.db',
                    db;

                if (fs.existsSync(persDb)) {
                    fs.writeFileSync(persDb, '', 'utf8');
                }
                db = new Datastore({filename: persDb, autoload: true});

                assert.equal(Object.keys(db.indexes).length, 1);
                assert.equal(Object.keys(db.indexes)[0], '_id');

                db.insert({planet: 'Earth'}, function (err) {
                    assert.equal(err, null);
                    db.insert({planet: 'Mars'}, function (err) {
                        assert.equal(err, null);

                        db.ensureIndex({fieldName: 'planet', unique: true, sparse: false}, function (err) {
                            assert.equal(Object.keys(db.indexes).length, 2);
                            assert.equal(Object.keys(db.indexes)[0], '_id');
                            assert.equal(Object.keys(db.indexes)[1], 'planet');
                            assert.equal(db.indexes._id.getAll().length, 2);
                            assert.equal(db.indexes.planet.getAll().length, 2);
                            assert.equal(db.indexes.planet.unique, true);
                            assert.equal(db.indexes.planet.sparse, false);

                            db.insert({planet: 'Jupiter'}, function (err) {
                                assert.equal(err, null);

                                // After a reload the indexes are recreated
                                db = new Datastore({filename: persDb});
                                db.loadDatabase(function (err) {
                                    assert.equal(err, null);
                                    assert.equal(Object.keys(db.indexes).length, 2);
                                    assert.equal(Object.keys(db.indexes)[0], '_id');
                                    assert.equal(Object.keys(db.indexes)[1], 'planet');
                                    assert.equal(db.indexes._id.getAll().length, 3);
                                    assert.equal(db.indexes.planet.getAll().length, 3);
                                    assert.equal(db.indexes.planet.unique, true);
                                    assert.equal(db.indexes.planet.sparse, false);

                                    db.ensureIndex({fieldName: 'bloup', unique: false, sparse: true}, function (err) {
                                        assert.equal(err, null);
                                        assert.equal(Object.keys(db.indexes).length, 3);
                                        assert.equal(Object.keys(db.indexes)[0], '_id');
                                        assert.equal(Object.keys(db.indexes)[1], 'planet');
                                        assert.equal(Object.keys(db.indexes)[2], 'bloup');
                                        assert.equal(db.indexes._id.getAll().length, 3);
                                        assert.equal(db.indexes.planet.getAll().length, 3);
                                        assert.equal(db.indexes.bloup.getAll().length, 0);
                                        assert.equal(db.indexes.planet.unique, true);
                                        assert.equal(db.indexes.planet.sparse, false);
                                        assert.equal(db.indexes.bloup.unique, false);
                                        assert.equal(db.indexes.bloup.sparse, true);

                                        // After another reload the indexes are still there (i.e. they are preserved during autocompaction)
                                        db = new Datastore({filename: persDb});
                                        db.loadDatabase(function (err) {
                                            assert.equal(err, null);
                                            assert.equal(Object.keys(db.indexes).length, 3);
                                            assert.equal(Object.keys(db.indexes)[0], '_id');
                                            assert.equal(Object.keys(db.indexes)[1], 'planet');
                                            assert.equal(Object.keys(db.indexes)[2], 'bloup');
                                            assert.equal(db.indexes._id.getAll().length, 3);
                                            assert.equal(db.indexes.planet.getAll().length, 3);
                                            assert.equal(db.indexes.bloup.getAll().length, 0);
                                            assert.equal(db.indexes.planet.unique, true);
                                            assert.equal(db.indexes.planet.sparse, false);
                                            assert.equal(db.indexes.bloup.unique, false);
                                            assert.equal(db.indexes.bloup.sparse, true);

                                            done();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });

            test('Indexes can also be removed and the remove persisted', function (done) {
                var persDb = 'workspace/persistIndexes.db',
                    db;

                if (fs.existsSync(persDb)) {
                    fs.writeFileSync(persDb, '', 'utf8');
                }
                db = new Datastore({filename: persDb, autoload: true});

                assert.equal(Object.keys(db.indexes).length, 1);
                assert.equal(Object.keys(db.indexes)[0], '_id');

                db.insert({planet: 'Earth'}, function (err) {
                    assert.equal(err, null);
                    db.insert({planet: 'Mars'}, function (err) {
                        assert.equal(err, null);

                        db.ensureIndex({fieldName: 'planet'}, function (err) {
                            assert.equal(err, null);
                            db.ensureIndex({fieldName: 'another'}, function (err) {
                                assert.equal(err, null);
                                assert.equal(Object.keys(db.indexes).length, 3);
                                assert.equal(Object.keys(db.indexes)[0], '_id');
                                assert.equal(Object.keys(db.indexes)[1], 'planet');
                                assert.equal(Object.keys(db.indexes)[2], 'another');
                                assert.equal(db.indexes._id.getAll().length, 2);
                                assert.equal(db.indexes.planet.getAll().length, 2);
                                assert.equal(db.indexes.planet.fieldName, 'planet');

                                // After a reload the indexes are recreated
                                db = new Datastore({filename: persDb});
                                db.loadDatabase(function (err) {
                                    assert.equal(err, null);
                                    assert.equal(Object.keys(db.indexes).length, 3);
                                    assert.equal(Object.keys(db.indexes)[0], '_id');
                                    assert.equal(Object.keys(db.indexes)[1], 'planet');
                                    assert.equal(Object.keys(db.indexes)[2], 'another');
                                    assert.equal(db.indexes._id.getAll().length, 2);
                                    assert.equal(db.indexes.planet.getAll().length, 2);
                                    assert.equal(db.indexes.planet.fieldName, 'planet');

                                    // Index is removed
                                    db.removeIndex('planet', function (err) {
                                        assert.equal(err, null);
                                        assert.equal(Object.keys(db.indexes).length, 2);
                                        assert.equal(Object.keys(db.indexes)[0], '_id');
                                        assert.equal(Object.keys(db.indexes)[1], 'another');
                                        assert.equal(db.indexes._id.getAll().length, 2);

                                        // After a reload indexes are preserved
                                        db = new Datastore({filename: persDb});
                                        db.loadDatabase(function (err) {
                                            assert.equal(err, null);
                                            assert.equal(Object.keys(db.indexes).length, 2);
                                            assert.equal(Object.keys(db.indexes)[0], '_id');
                                            assert.equal(Object.keys(db.indexes)[1], 'another');
                                            assert.equal(db.indexes._id.getAll().length, 2);

                                            // After another reload the indexes are still there (i.e. they are preserved during autocompaction)
                                            db = new Datastore({filename: persDb});
                                            db.loadDatabase(function (err) {
                                                assert.equal(err, null);
                                                assert.equal(Object.keys(db.indexes).length, 2);
                                                assert.equal(Object.keys(db.indexes)[0], '_id');
                                                assert.equal(Object.keys(db.indexes)[1], 'another');
                                                assert.equal(db.indexes._id.getAll().length, 2);

                                                done();
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        }); // ==== End of 'Persisting indexes' ====

        test('Results of getMatching should never contain duplicates', function (done) {
            d.ensureIndex({fieldName: 'bad'});
            d.insert({bad: ['a', 'b']}, function () {
                d.getCandidates({bad: {$in: ['a', 'b']}}, function (err, res) {
                    assert.equal(res.length, 1);
                    done();
                });
            });
        });
    }); // ==== End of 'Using indexes' ==== //
});
