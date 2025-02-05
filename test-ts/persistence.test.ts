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
import customUtils from '../src/customUtils';
import Cursor from '../src/cursor';
import {storage} from '../src/storage';
import child_process from 'child_process';
import os from 'os';
const testDb = 'workspace/test.db';

describe('Persistence', function () {
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

    test('Every line represents a document', function () {
        var now = new Date(),
            rawData =
                model.serialize({_id: '1', a: 2, ages: [1, 5, 12]}) +
                '\n' +
                model.serialize({_id: '2', hello: 'world'}) +
                '\n' +
                model.serialize({_id: '3', nested: {today: now}}),
            treatedData = d.persistence.treatRawData(rawData).data;
        treatedData.sort(function (a, b) {
            return a._id - b._id;
        });
        assert.equal(treatedData.length, 3);
        assert.equal(_.isEqual(treatedData[0], {_id: '1', a: 2, ages: [1, 5, 12]}), true);
        assert.equal(_.isEqual(treatedData[1], {_id: '2', hello: 'world'}), true);
        assert.equal(_.isEqual(treatedData[2], {_id: '3', nested: {today: now}}), true);
    });

    test('Badly formatted lines have no impact on the treated data', function () {
        var now = new Date(),
            rawData =
                model.serialize({_id: '1', a: 2, ages: [1, 5, 12]}) +
                '\n' +
                'garbage\n' +
                model.serialize({_id: '3', nested: {today: now}}),
            treatedData = d.persistence.treatRawData(rawData).data;
        treatedData.sort(function (a, b) {
            return a._id - b._id;
        });
        assert.equal(treatedData.length, 2);
        assert.equal(_.isEqual(treatedData[0], {_id: '1', a: 2, ages: [1, 5, 12]}), true);
        assert.equal(_.isEqual(treatedData[1], {_id: '3', nested: {today: now}}), true);
    });

    test('Well formatted lines that have no _id are not included in the data', function () {
        var now = new Date(),
            rawData =
                model.serialize({_id: '1', a: 2, ages: [1, 5, 12]}) +
                '\n' +
                model.serialize({_id: '2', hello: 'world'}) +
                '\n' +
                model.serialize({nested: {today: now}}),
            treatedData = d.persistence.treatRawData(rawData).data;
        treatedData.sort(function (a, b) {
            return a._id - b._id;
        });
        assert.equal(treatedData.length, 2);
        assert.equal(_.isEqual(treatedData[0], {_id: '1', a: 2, ages: [1, 5, 12]}), true);
        assert.equal(_.isEqual(treatedData[1], {_id: '2', hello: 'world'}), true);
    });

    test('If two lines concern the same doc (= same _id), the last one is the good version', function () {
        var now = new Date(),
            rawData =
                model.serialize({_id: '1', a: 2, ages: [1, 5, 12]}) +
                '\n' +
                model.serialize({_id: '2', hello: 'world'}) +
                '\n' +
                model.serialize({_id: '1', nested: {today: now}}),
            treatedData = d.persistence.treatRawData(rawData).data;
        treatedData.sort(function (a, b) {
            return a._id - b._id;
        });
        assert.equal(treatedData.length, 2);
        assert.equal(_.isEqual(treatedData[0], {_id: '1', nested: {today: now}}), true);
        assert.equal(_.isEqual(treatedData[1], {_id: '2', hello: 'world'}), true);
    });

    test('If a doc contains $$deleted: true, that means we need to remove it from the data', function () {
        var now = new Date(),
            rawData =
                model.serialize({_id: '1', a: 2, ages: [1, 5, 12]}) +
                '\n' +
                model.serialize({_id: '2', hello: 'world'}) +
                '\n' +
                model.serialize({_id: '1', $$deleted: true}) +
                '\n' +
                model.serialize({_id: '3', today: now}),
            treatedData = d.persistence.treatRawData(rawData).data;
        treatedData.sort(function (a, b) {
            return a._id - b._id;
        });
        assert.equal(treatedData.length, 2);
        assert.equal(_.isEqual(treatedData[0], {_id: '2', hello: 'world'}), true);
        assert.equal(_.isEqual(treatedData[1], {_id: '3', today: now}), true);
    });

    test('If a doc contains $$deleted: true, no error is thrown if the doc wasnt in the list before', function () {
        var now = new Date(),
            rawData =
                model.serialize({_id: '1', a: 2, ages: [1, 5, 12]}) +
                '\n' +
                model.serialize({_id: '2', $$deleted: true}) +
                '\n' +
                model.serialize({_id: '3', today: now}),
            treatedData = d.persistence.treatRawData(rawData).data;
        treatedData.sort(function (a, b) {
            return a._id - b._id;
        });
        assert.equal(treatedData.length, 2);
        assert.equal(_.isEqual(treatedData[0], {_id: '1', a: 2, ages: [1, 5, 12]}), true);
        assert.equal(_.isEqual(treatedData[1], {_id: '3', today: now}), true);
    });

    test('If a doc contains $$indexCreated, no error is thrown during treatRawData and we can get the index options', function () {
        var now = new Date(),
            rawData =
                model.serialize({_id: '1', a: 2, ages: [1, 5, 12]}) +
                '\n' +
                model.serialize({$$indexCreated: {fieldName: 'test', unique: true}}) +
                '\n' +
                model.serialize({_id: '3', today: now}),
            treatedData = d.persistence.treatRawData(rawData).data,
            indexes = d.persistence.treatRawData(rawData).indexes;
        assert.equal(Object.keys(indexes).length, 1);
        assert.deepEqual(indexes.test, {fieldName: 'test', unique: true});

        treatedData.sort(function (a, b) {
            return a._id - b._id;
        });
        assert.equal(treatedData.length, 2);
        assert.equal(_.isEqual(treatedData[0], {_id: '1', a: 2, ages: [1, 5, 12]}), true);
        assert.equal(_.isEqual(treatedData[1], {_id: '3', today: now}), true);
    });

    test('Compact database on load', function (done) {
        d.insert({a: 2}, function () {
            d.insert({a: 4}, function () {
                d.remove({a: 2}, {}, function () {
                    // Here, the underlying file is 3 lines long for only one document
                    var data = fs.readFileSync(d.filename, 'utf8').split('\n'),
                        filledCount = 0;

                    data.forEach(function (item) {
                        if (item.length > 0) {
                            filledCount += 1;
                        }
                    });
                    assert.equal(filledCount, 3);

                    d.loadDatabase(function (err) {
                        assert.equal(err, null);

                        // Now, the file has been compacted and is only 1 line long
                        var data = fs.readFileSync(d.filename, 'utf8').split('\n'),
                            filledCount = 0;

                        data.forEach(function (item) {
                            if (item.length > 0) {
                                filledCount += 1;
                            }
                        });
                        assert.equal(filledCount, 1);

                        done();
                    });
                });
            });
        });
    });

    test('Calling loadDatabase after the data was modified doesnt change its contents', function (done) {
        d.loadDatabase(function () {
            d.insert({a: 1}, function (err) {
                assert.equal(err, null);
                d.insert({a: 2}, function (err) {
                    var data = d.getAllData(),
                        doc1 = _.find(data, function (doc) {
                            return doc.a === 1;
                        }),
                        doc2 = _.find(data, function (doc) {
                            return doc.a === 2;
                        });
                    assert.equal(err, null);
                    assert.equal(data.length, 2);
                    assert.equal(doc1.a, 1);
                    assert.equal(doc2.a, 2);

                    d.loadDatabase(function (err) {
                        var data = d.getAllData(),
                            doc1 = _.find(data, function (doc) {
                                return doc.a === 1;
                            }),
                            doc2 = _.find(data, function (doc) {
                                return doc.a === 2;
                            });
                        assert.equal(err, null);
                        assert.equal(data.length, 2);
                        assert.equal(doc1.a, 1);
                        assert.equal(doc2.a, 2);

                        done();
                    });
                });
            });
        });
    });

    test('Calling loadDatabase after the datafile was removed will reset the database', function (done) {
        d.loadDatabase(function () {
            d.insert({a: 1}, function (err) {
                assert.equal(err, null);
                d.insert({a: 2}, function (err) {
                    var data = d.getAllData(),
                        doc1 = _.find(data, function (doc) {
                            return doc.a === 1;
                        }),
                        doc2 = _.find(data, function (doc) {
                            return doc.a === 2;
                        });
                    assert.equal(err, null);
                    assert.equal(data.length, 2);
                    assert.equal(doc1.a, 1);
                    assert.equal(doc2.a, 2);

                    fs.unlink(testDb, function (err) {
                        assert.equal(err, null);
                        d.loadDatabase(function (err) {
                            assert.equal(err, null);
                            assert.equal(d.getAllData().length, 0);

                            done();
                        });
                    });
                });
            });
        });
    });

    test('Calling loadDatabase after the datafile was modified loads the new data', function (done) {
        d.loadDatabase(function () {
            d.insert({a: 1}, function (err) {
                assert.equal(err, null);
                d.insert({a: 2}, function (err) {
                    var data = d.getAllData(),
                        doc1 = _.find(data, function (doc) {
                            return doc.a === 1;
                        }),
                        doc2 = _.find(data, function (doc) {
                            return doc.a === 2;
                        });
                    assert.equal(err, null);
                    assert.equal(data.length, 2);
                    assert.equal(doc1.a, 1);
                    assert.equal(doc2.a, 2);

                    fs.writeFile(testDb, '{"a":3,"_id":"aaa"}', 'utf8', function (err) {
                        assert.equal(err, null);
                        d.loadDatabase(function (err) {
                            var data = d.getAllData(),
                                doc1 = _.find(data, function (doc) {
                                    return doc.a === 1;
                                }),
                                doc2 = _.find(data, function (doc) {
                                    return doc.a === 2;
                                }),
                                doc3 = _.find(data, function (doc) {
                                    return doc.a === 3;
                                });
                            assert.equal(err, null);
                            assert.equal(data.length, 1);
                            assert.equal(doc3.a, 3);
                            assert.equal(doc1, undefined);
                            assert.equal(doc2, undefined);

                            done();
                        });
                    });
                });
            });
        });
    });

    test('When treating raw data, refuse to proceed if too much data is corrupt, to avoid data loss', function (done) {
        var corruptTestFilename = 'workspace/corruptTest.db',
            fakeData =
                '{"_id":"one","hello":"world"}\n' +
                'Some corrupt data\n' +
                '{"_id":"two","hello":"earth"}\n' +
                '{"_id":"three","hello":"you"}\n',
            d;
        fs.writeFileSync(corruptTestFilename, fakeData, 'utf8');

        // Default corruptAlertThreshold
        d = new Datastore({filename: corruptTestFilename});
        d.loadDatabase(function (err) {
            assert.equal(err instanceof Error, true);
            assert.notEqual(err, null);

            fs.writeFileSync(corruptTestFilename, fakeData, 'utf8');
            d = new Datastore({filename: corruptTestFilename, corruptAlertThreshold: 1});
            d.loadDatabase(function (err) {
                assert.equal(err, null);

                fs.writeFileSync(corruptTestFilename, fakeData, 'utf8');
                d = new Datastore({filename: corruptTestFilename, corruptAlertThreshold: 0});
                d.loadDatabase(function (err) {
                    assert.equal(err instanceof Error, true);
                    assert.notEqual(err, null);

                    done();
                });
            });
        });
    });

    test('Can listen to compaction events', function (done) {
        d.on('compaction.done', function () {
            d.removeAllListeners('compaction.done'); // Tidy up for next tests
            done();
        });

        d.persistence.compactDatafile();
    });

    describe('Serialization hooks', function () {
        var theAs = function (s) {
                return 'before_' + s + '_after';
            },
            theBd = function (s) {
                return s.substring(7, s.length - 6);
            };

        test('Declaring only one hook will throw an exception to prevent data loss', function (done) {
            var hookTestFilename = 'workspace/hookTest.db';
            storage.ensureFileDoesntExist(hookTestFilename, function () {
                fs.writeFileSync(hookTestFilename, 'Some content', 'utf8');

                assert.throws(() => {
                    new Datastore({filename: hookTestFilename, autoload: true, afterSerialization: theAs});
                });

                // Data file left untouched
                assert.equal(fs.readFileSync(hookTestFilename, 'utf8'), 'Some content');

                assert.throws(() => {
                    new Datastore({filename: hookTestFilename, autoload: true, beforeDeserialization: theBd});
                });

                // Data file left untouched
                assert.equal(fs.readFileSync(hookTestFilename, 'utf8'), 'Some content');

                done();
            });
        });

        test('Declaring two hooks that are not reverse of one another will cause an exception to prevent data loss', function (done) {
            var hookTestFilename = 'workspace/hookTest.db';

            storage.ensureFileDoesntExist(hookTestFilename, function () {
                fs.writeFileSync(hookTestFilename, 'Some content', 'utf8');

                assert.throws(() => {
                    new Datastore({
                        filename: hookTestFilename,
                        autoload: true,
                        afterSerialization: theAs,
                        beforeDeserialization: function (s) {
                            return s;
                        },
                    });
                });

                // Data file left untouched
                assert.equal(fs.readFileSync(hookTestFilename, 'utf8'), 'Some content');

                done();
            });
        });

        test('A serialization hook can be used to transform data before writing new state to disk', function (done) {
            var hookTestFilename = 'workspace/hookTest.db';
            storage.ensureFileDoesntExist(hookTestFilename, function () {
                var d = new Datastore({
                    filename: hookTestFilename,
                    autoload: true,
                    afterSerialization: theAs,
                    beforeDeserialization: theBd,
                });
                d.insert({hello: 'world'}, function () {
                    var _data = fs.readFileSync(hookTestFilename, 'utf8'),
                        data = _data.split('\n'),
                        doc0 = theBd(data[0]);
                    assert.equal(data.length, 2);

                    assert.equal(data[0].substring(0, 7), 'before_');
                    assert.equal(data[0].substring(data[0].length - 6), '_after');

                    doc0 = model.deserialize(doc0);
                    assert.equal(Object.keys(doc0).length, 2);
                    assert.equal(doc0.hello, 'world');

                    d.insert({p: 'Mars'}, function () {
                        var _data = fs.readFileSync(hookTestFilename, 'utf8'),
                            data = _data.split('\n'),
                            doc0 = theBd(data[0]),
                            doc1 = theBd(data[1]);
                        assert.equal(data.length, 3);

                        assert.equal(data[0].substring(0, 7), 'before_');
                        assert.equal(data[0].substring(data[0].length - 6), '_after');
                        assert.equal(data[1].substring(0, 7), 'before_');
                        assert.equal(data[1].substring(data[1].length - 6), '_after');

                        doc0 = model.deserialize(doc0);
                        assert.equal(Object.keys(doc0).length, 2);
                        assert.equal(doc0.hello, 'world');

                        doc1 = model.deserialize(doc1);
                        assert.equal(Object.keys(doc1).length, 2);
                        assert.equal(doc1.p, 'Mars');

                        d.ensureIndex({fieldName: 'idefix'}, function () {
                            var _data = fs.readFileSync(hookTestFilename, 'utf8'),
                                data = _data.split('\n'),
                                doc0 = theBd(data[0]),
                                doc1 = theBd(data[1]),
                                idx = theBd(data[2]);
                            assert.equal(data.length, 4);

                            assert.equal(data[0].substring(0, 7), 'before_');
                            assert.equal(data[0].substring(data[0].length - 6), '_after');
                            assert.equal(data[1].substring(0, 7), 'before_');
                            assert.equal(data[1].substring(data[1].length - 6), '_after');

                            doc0 = model.deserialize(doc0);
                            assert.equal(Object.keys(doc0).length, 2);
                            assert.equal(doc0.hello, 'world');

                            doc1 = model.deserialize(doc1);
                            assert.equal(Object.keys(doc1).length, 2);
                            assert.equal(doc1.p, 'Mars');

                            idx = model.deserialize(idx);
                            assert.deepEqual(idx, {'$$indexCreated': {fieldName: 'idefix'}});

                            done();
                        });
                    });
                });
            });
        });

        test('Use serialization hook when persisting cached database or compacting', function (done) {
            var hookTestFilename = 'workspace/hookTest.db';
            storage.ensureFileDoesntExist(hookTestFilename, function () {
                var d = new Datastore({
                    filename: hookTestFilename,
                    autoload: true,
                    afterSerialization: theAs,
                    beforeDeserialization: theBd,
                });
                d.insert({hello: 'world'}, function () {
                    d.update({hello: 'world'}, {$set: {hello: 'earth'}}, {}, function () {
                        d.ensureIndex({fieldName: 'idefix'}, function () {
                            var _data = fs.readFileSync(hookTestFilename, 'utf8'),
                                data = _data.split('\n'),
                                doc0 = theBd(data[0]),
                                doc1 = theBd(data[1]),
                                idx = theBd(data[2]),
                                _id;

                            assert.equal(data.length, 4);

                            doc0 = model.deserialize(doc0);
                            assert.equal(Object.keys(doc0).length, 2);
                            assert.equal(doc0.hello, 'world');

                            doc1 = model.deserialize(doc1);
                            assert.equal(Object.keys(doc1).length, 2);
                            assert.equal(doc1.hello, 'earth');

                            assert.equal(doc0._id, doc1._id);
                            _id = doc0._id;

                            idx = model.deserialize(idx);
                            assert.deepEqual(idx, {'$$indexCreated': {fieldName: 'idefix'}});

                            d.persistence.persistCachedDatabase(function () {
                                var _data = fs.readFileSync(hookTestFilename, 'utf8'),
                                    data = _data.split('\n'),
                                    doc0 = theBd(data[0]),
                                    idx = theBd(data[1]);
                                assert.equal(data.length, 3);

                                doc0 = model.deserialize(doc0);
                                assert.equal(Object.keys(doc0).length, 2);
                                assert.equal(doc0.hello, 'earth');

                                assert.equal(doc0._id, _id);

                                idx = model.deserialize(idx);
                                assert.deepEqual(idx, {
                                    '$$indexCreated': {fieldName: 'idefix', unique: false, sparse: false},
                                });

                                done();
                            });
                        });
                    });
                });
            });
        });

        test('Deserialization hook is correctly used when loading data', function (done) {
            var hookTestFilename = 'workspace/hookTest.db';
            storage.ensureFileDoesntExist(hookTestFilename, function () {
                var d = new Datastore({
                    filename: hookTestFilename,
                    autoload: true,
                    afterSerialization: theAs,
                    beforeDeserialization: theBd,
                });
                d.insert({hello: 'world'}, function (err, doc) {
                    var _id = doc._id;
                    d.insert({yo: 'ya'}, function () {
                        d.update({hello: 'world'}, {$set: {hello: 'earth'}}, {}, function () {
                            d.remove({yo: 'ya'}, {}, function () {
                                d.ensureIndex({fieldName: 'idefix'}, function () {
                                    var _data = fs.readFileSync(hookTestFilename, 'utf8'),
                                        data = _data.split('\n');
                                    assert.equal(data.length, 6);

                                    // Everything is deserialized correctly, including deletes and indexes
                                    var d = new Datastore({
                                        filename: hookTestFilename,
                                        afterSerialization: theAs,
                                        beforeDeserialization: theBd,
                                    });
                                    d.loadDatabase(function () {
                                        d.find({}, function (err, docs) {
                                            assert.equal(docs.length, 1);
                                            assert.equal(docs[0].hello, 'earth');
                                            assert.equal(docs[0]._id, _id);

                                            assert.equal(Object.keys(d.indexes).length, 2);
                                            assert.equal(Object.keys(d.indexes).includes('idefix'), true);

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
    }); // ==== End of 'Serialization hooks' ==== //

    describe('Prevent dataloss when persisting data', function () {
        test('Creating a datastore with in memory as true and a bad filename wont cause an error', function () {
            new Datastore({filename: 'workspace/bad.db~', inMemoryOnly: true});
        });

        test('Creating a persistent datastore with a bad filename will cause an error', function () {
            assert.throws(() => {
                new Datastore({filename: 'workspace/bad.db~'});
            });
        });

        test('If no file exists, ensureDatafileIntegrity creates an empty datafile', function (done) {
            var p = new Persistence({db: {inMemoryOnly: false, filename: 'workspace/it.db'}});

            if (fs.existsSync('workspace/it.db')) {
                fs.unlinkSync('workspace/it.db');
            }
            if (fs.existsSync('workspace/it.db~')) {
                fs.unlinkSync('workspace/it.db~');
            }

            assert.equal(fs.existsSync('workspace/it.db'), false);
            assert.equal(fs.existsSync('workspace/it.db~'), false);

            storage.ensureDatafileIntegrity(p.filename, function (err) {
                assert.equal(err, null);

                assert.equal(fs.existsSync('workspace/it.db'), true);
                assert.equal(fs.existsSync('workspace/it.db~'), false);

                assert.equal(fs.readFileSync('workspace/it.db', 'utf8'), '');

                done();
            });
        });

        test('If only datafile exists, ensureDatafileIntegrity will use it', function (done) {
            var p = new Persistence({db: {inMemoryOnly: false, filename: 'workspace/it.db'}});

            if (fs.existsSync('workspace/it.db')) {
                fs.unlinkSync('workspace/it.db');
            }
            if (fs.existsSync('workspace/it.db~')) {
                fs.unlinkSync('workspace/it.db~');
            }

            fs.writeFileSync('workspace/it.db', 'something', 'utf8');

            assert.equal(fs.existsSync('workspace/it.db'), true);
            assert.equal(fs.existsSync('workspace/it.db~'), false);

            storage.ensureDatafileIntegrity(p.filename, function (err) {
                assert.equal(err, null);

                assert.equal(fs.existsSync('workspace/it.db'), true);
                assert.equal(fs.existsSync('workspace/it.db~'), false);

                assert.equal(fs.readFileSync('workspace/it.db', 'utf8'), 'something');

                done();
            });
        });

        test('If temp datafile exists and datafile doesnt, ensureDatafileIntegrity will use it (cannot happen except upon first use)', function (done) {
            var p = new Persistence({db: {inMemoryOnly: false, filename: 'workspace/it.db'}});

            if (fs.existsSync('workspace/it.db')) {
                fs.unlinkSync('workspace/it.db');
            }
            if (fs.existsSync('workspace/it.db~')) {
                fs.unlinkSync('workspace/it.db~~');
            }

            fs.writeFileSync('workspace/it.db~', 'something', 'utf8');

            assert.equal(fs.existsSync('workspace/it.db'), false);
            assert.equal(fs.existsSync('workspace/it.db~'), true);

            storage.ensureDatafileIntegrity(p.filename, function (err) {
                assert.equal(err, null);

                assert.equal(fs.existsSync('workspace/it.db'), true);
                assert.equal(fs.existsSync('workspace/it.db~'), false);

                assert.equal(fs.readFileSync('workspace/it.db', 'utf8'), 'something');

                done();
            });
        });

        // Technically it could also mean the write was successful but the rename wasn't, but there is in any case no guarantee that the data in the temp file is whole so we have to discard the whole file
        test('If both temp and current datafiles exist, ensureDatafileIntegrity will use the datafile, as it means that the write of the temp file failed', function (done) {
            var theDb = new Datastore({filename: 'workspace/it.db'});

            if (fs.existsSync('workspace/it.db')) {
                fs.unlinkSync('workspace/it.db');
            }
            if (fs.existsSync('workspace/it.db~')) {
                fs.unlinkSync('workspace/it.db~');
            }

            fs.writeFileSync('workspace/it.db', '{"_id":"0","hello":"world"}', 'utf8');
            fs.writeFileSync('workspace/it.db~', '{"_id":"0","hello":"other"}', 'utf8');

            assert.equal(fs.existsSync('workspace/it.db'), true);
            assert.equal(fs.existsSync('workspace/it.db~'), true);

            storage.ensureDatafileIntegrity(theDb.persistence.filename, function (err) {
                assert.equal(err, null);

                assert.equal(fs.existsSync('workspace/it.db'), true);
                assert.equal(fs.existsSync('workspace/it.db~'), true);

                assert.equal(fs.readFileSync('workspace/it.db', 'utf8'), '{"_id":"0","hello":"world"}');

                theDb.loadDatabase(function (err) {
                    assert.equal(err, null);
                    theDb.find({}, function (err, docs) {
                        assert.equal(err, null);
                        assert.equal(docs.length, 1);
                        assert.equal(docs[0].hello, 'world');
                        assert.equal(fs.existsSync('workspace/it.db'), true);
                        assert.equal(fs.existsSync('workspace/it.db~'), false);
                        done();
                    });
                });
            });
        });

        test('persistCachedDatabase should update the contents of the datafile and leave a clean state', function (done) {
            d.insert({hello: 'world'}, function () {
                d.find({}, function (err, docs) {
                    assert.equal(docs.length, 1);

                    if (fs.existsSync(testDb)) {
                        fs.unlinkSync(testDb);
                    }
                    if (fs.existsSync(testDb + '~')) {
                        fs.unlinkSync(testDb + '~');
                    }
                    assert.equal(fs.existsSync(testDb), false);

                    fs.writeFileSync(testDb + '~', 'something', 'utf8');
                    assert.equal(fs.existsSync(testDb + '~'), true);

                    d.persistence.persistCachedDatabase(function (err) {
                        var contents = fs.readFileSync(testDb, 'utf8');
                        assert.equal(err, null);
                        assert.equal(fs.existsSync(testDb), true);
                        assert.equal(fs.existsSync(testDb + '~'), false);
                        if (!contents.match(/^{"hello":"world","_id":"[0-9a-zA-Z]{16}"}\n$/)) {
                            throw new Error('Datafile contents not as expected');
                        }
                        done();
                    });
                });
            });
        });

        test('After a persistCachedDatabase, there should be no temp or old filename', function (done) {
            d.insert({hello: 'world'}, function () {
                d.find({}, function (err, docs) {
                    assert.equal(docs.length, 1);

                    if (fs.existsSync(testDb)) {
                        fs.unlinkSync(testDb);
                    }
                    if (fs.existsSync(testDb + '~')) {
                        fs.unlinkSync(testDb + '~');
                    }
                    assert.equal(fs.existsSync(testDb), false);
                    assert.equal(fs.existsSync(testDb + '~'), false);

                    fs.writeFileSync(testDb + '~', 'bloup', 'utf8');
                    assert.equal(fs.existsSync(testDb + '~'), true);

                    d.persistence.persistCachedDatabase(function (err) {
                        var contents = fs.readFileSync(testDb, 'utf8');
                        assert.equal(err, null);
                        assert.equal(fs.existsSync(testDb), true);
                        assert.equal(fs.existsSync(testDb + '~'), false);
                        if (!contents.match(/^{"hello":"world","_id":"[0-9a-zA-Z]{16}"}\n$/)) {
                            throw new Error('Datafile contents not as expected');
                        }
                        done();
                    });
                });
            });
        });

        test('persistCachedDatabase should update the contents of the datafile and leave a clean state even if there is a temp datafile', function (done) {
            d.insert({hello: 'world'}, function () {
                d.find({}, function (err, docs) {
                    assert.equal(docs.length, 1);

                    if (fs.existsSync(testDb)) {
                        fs.unlinkSync(testDb);
                    }
                    fs.writeFileSync(testDb + '~', 'blabla', 'utf8');
                    assert.equal(fs.existsSync(testDb), false);
                    assert.equal(fs.existsSync(testDb + '~'), true);

                    d.persistence.persistCachedDatabase(function (err) {
                        var contents = fs.readFileSync(testDb, 'utf8');
                        assert.equal(err, null);
                        assert.equal(fs.existsSync(testDb), true);
                        assert.equal(fs.existsSync(testDb + '~'), false);
                        if (!contents.match(/^{"hello":"world","_id":"[0-9a-zA-Z]{16}"}\n$/)) {
                            throw new Error('Datafile contents not as expected');
                        }
                        done();
                    });
                });
            });
        });

        test('persistCachedDatabase should update the contents of the datafile and leave a clean state even if there is a temp datafile', function (done) {
            var dbFile = 'workspace/test2.db',
                theDb;

            if (fs.existsSync(dbFile)) {
                fs.unlinkSync(dbFile);
            }
            if (fs.existsSync(dbFile + '~')) {
                fs.unlinkSync(dbFile + '~');
            }

            theDb = new Datastore({filename: dbFile});

            theDb.loadDatabase(function (err) {
                var contents = fs.readFileSync(dbFile, 'utf8');
                assert.equal(err, null);
                assert.equal(fs.existsSync(dbFile), true);
                assert.equal(fs.existsSync(dbFile + '~'), false);
                if (contents != '') {
                    throw new Error('Datafile contents not as expected');
                }
                done();
            });
        });

        test('Persistence works as expected when everything goes fine', function (done) {
            var dbFile = 'workspace/test2.db',
                theDb,
                theDb2,
                doc1,
                doc2;

            async.waterfall(
                [
                    async.apply(storage.ensureFileDoesntExist, dbFile),
                    async.apply(storage.ensureFileDoesntExist, dbFile + '~'),
                    function (cb) {
                        theDb = new Datastore({filename: dbFile});
                        theDb.loadDatabase(cb);
                    },
                    function (cb) {
                        theDb.find({}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 0);
                            return cb();
                        });
                    },
                    function (cb) {
                        theDb.insert({a: 'hello'}, function (err, _doc1) {
                            assert.equal(err, null);
                            doc1 = _doc1;
                            theDb.insert({a: 'world'}, function (err, _doc2) {
                                assert.equal(err, null);
                                doc2 = _doc2;
                                return cb();
                            });
                        });
                    },
                    function (cb) {
                        theDb.find({}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 2);
                            assert.equal(_.find(docs, item => item._id === doc1._id).a, 'hello');
                            assert.equal(_.find(docs, item => item._id === doc2._id).a, 'world');
                            return cb();
                        });
                    },
                    function (cb) {
                        theDb.loadDatabase(cb);
                    },
                    function (cb) {
                        // No change
                        theDb.find({}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 2);
                            assert.equal(_.find(docs, item => item._id === doc1._id).a, 'hello');
                            assert.equal(_.find(docs, item => item._id === doc2._id).a, 'world');
                            return cb();
                        });
                    },
                    function (cb) {
                        assert.equal(fs.existsSync(dbFile), true);
                        assert.equal(fs.existsSync(dbFile + '~'), false);
                        return cb();
                    },
                    function (cb) {
                        theDb2 = new Datastore({filename: dbFile});
                        theDb2.loadDatabase(cb);
                    },
                    function (cb) {
                        // No change in second db
                        theDb2.find({}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 2);
                            assert.equal(_.find(docs, item => item._id === doc1._id).a, 'hello');
                            assert.equal(_.find(docs, item => item._id === doc2._id).a, 'world');
                            return cb();
                        });
                    },
                    function (cb) {
                        assert.equal(fs.existsSync(dbFile), true);
                        assert.equal(fs.existsSync(dbFile + '~'), false);
                        return cb();
                    },
                ],
                done
            );
        });

        // The child process will load the database with the given datafile, but the fs.writeFile function
        // is rewritten to crash the process before it finished (after 5000 bytes), to ensure data was not lost
        test('If system crashes during a loadDatabase, the former version is not lost', function (done) {
            var N = 500,
                toWrite = '',
                i,
                doc_i;

            // Ensuring the state is clean
            if (fs.existsSync('workspace/lac.db')) {
                fs.unlinkSync('workspace/lac.db');
            }
            if (fs.existsSync('workspace/lac.db~')) {
                fs.unlinkSync('workspace/lac.db~');
            }

            // Creating a db file with 150k records (a bit long to load)
            for (i = 0; i < N; i += 1) {
                toWrite += model.serialize({_id: 'anid_' + i, hello: 'world'}) + '\n';
            }
            fs.writeFileSync('workspace/lac.db', toWrite, 'utf8');

            var datafileLength = fs.readFileSync('workspace/lac.db', 'utf8').length;

            // Loading it in a separate process that we will crash before finishing the loadDatabase
            child_process.fork('test_lac/loadAndCrash.test').on('exit', function (code) {
                assert.equal(code, 1); // See test_lac/loadAndCrash.test.js

                assert.equal(fs.existsSync('workspace/lac.db'), true);
                assert.equal(fs.existsSync('workspace/lac.db~'), true);
                assert.equal(fs.readFileSync('workspace/lac.db', 'utf8').length, datafileLength);
                assert.equal(fs.readFileSync('workspace/lac.db~', 'utf8').length, 5000);

                // Reload database without a crash, check that no data was lost and fs state is clean (no temp file)
                var db = new Datastore({filename: 'workspace/lac.db'});
                db.loadDatabase(function (err) {
                    assert.equal(err, null);

                    assert.equal(fs.existsSync('workspace/lac.db'), true);
                    assert.equal(fs.existsSync('workspace/lac.db~'), false);
                    assert.equal(fs.readFileSync('workspace/lac.db', 'utf8').length, datafileLength);

                    db.find({}, function (err, docs) {
                        assert.equal(docs.length, N);
                        for (i = 0; i < N; i += 1) {
                            doc_i = _.find(docs, function (d) {
                                return d._id === 'anid_' + i;
                            });
                            assert.notEqual(doc_i, undefined);
                            assert.deepEqual({hello: 'world', _id: 'anid_' + i}, doc_i);
                        }
                        return done();
                    });
                });
            });
        });

        // Not run on Windows as there is no clean way to set maximum file descriptors. Not an issue as the code itself is tested.
        test('Cannot cause EMFILE errors by opening too many file descriptors', function (done) {
            if (os.platform() === 'win32' || os.platform() === 'win64') {
                return done();
            }
            child_process.execFile('test_lac/openFdsLaunch.sh', function (err, stdout, stderr) {
                if (err) {
                    return done(err);
                }

                // The subprocess will not output anything to stdout unless part of the test fails
                if (stdout.length !== 0) {
                    return done(stdout);
                } else {
                    return done();
                }
            });
        });
    }); // ==== End of 'Prevent dataloss when persisting data' ====

    describe('ensureFileDoesntExist', function () {
        test('Doesnt do anything if file already doesnt exist', function (done) {
            storage.ensureFileDoesntExist('workspace/nonexisting', function (err) {
                assert.equal(err, null);
                assert.equal(fs.existsSync('workspace/nonexisting'), false);
                done();
            });
        });

        test('Deletes file if it exists', function (done) {
            fs.writeFileSync('workspace/existing', 'hello world', 'utf8');
            assert.equal(fs.existsSync('workspace/existing'), true);

            storage.ensureFileDoesntExist('workspace/existing', function (err) {
                assert.equal(err, null);
                assert.equal(fs.existsSync('workspace/existing'), false);
                done();
            });
        });
    }); // ==== End of 'ensureFileDoesntExist' ====
});
