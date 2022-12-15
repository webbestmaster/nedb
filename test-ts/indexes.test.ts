// @ts-nocheck

import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import {describe, test, beforeEach} from '@jest/globals';
import {Index} from '../src/indexes';
import _ from 'underscore';
import async from 'async';
import model from './../src/model';
import Datastore from '../src/datastore';
import Persistence from '../src/persistence';
import customUtils from '../src/customUtils';
import Cursor from '../src/cursor';
import storage from '../src/storage';
import child_process from 'child_process';
import os from 'os';
import util from 'util';

describe('Indexes', function () {
    describe('Insertion', function () {
        test('Can insert pointers to documents in the index correctly when they have the field', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            // The underlying BST now has 3 nodes which contain the docs where it's expected
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('hello'), [{a: 5, tf: 'hello'}]);
            assert.deepEqual(idx.tree.search('world'), [{a: 8, tf: 'world'}]);
            assert.deepEqual(idx.tree.search('bloup'), [{a: 2, tf: 'bloup'}]);

            // The nodes contain pointers to the actual documents
            assert.equal(idx.tree.search('world')[0], doc2);
            idx.tree.search('bloup')[0].a = 42;
            assert.equal(doc3.a, 42);
        });

        test('Inserting twice for the same fieldName in a unique index will result in an error thrown', function () {
            var idx = new Index({fieldName: 'tf', unique: true}),
                doc1 = {a: 5, tf: 'hello'};
            idx.insert(doc1);
            assert.equal(idx.tree.getNumberOfKeys(), 1);
            assert.throws(function () {
                idx.insert(doc1);
            });
        });

        test('Inserting twice for a fieldName the docs dont have with a unique index results in an error thrown', function () {
            var idx = new Index({fieldName: 'nope', unique: true}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 5, tf: 'world'};
            idx.insert(doc1);
            assert.equal(idx.tree.getNumberOfKeys(), 1);
            assert.throws(function () {
                idx.insert(doc2);
            });
        });

        test('Inserting twice for a fieldName the docs dont have with a unique and sparse index will not throw, since the docs will be non indexed', function () {
            var idx = new Index({fieldName: 'nope', unique: true, sparse: true}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 5, tf: 'world'};
            idx.insert(doc1);
            idx.insert(doc2);
            assert.equal(idx.tree.getNumberOfKeys(), 0); // Docs are not indexed
        });

        test('Works with dot notation', function () {
            var idx = new Index({fieldName: 'tf.nested'}),
                doc1 = {a: 5, tf: {nested: 'hello'}},
                doc2 = {a: 8, tf: {nested: 'world', additional: true}},
                doc3 = {a: 2, tf: {nested: 'bloup', age: 42}};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            // The underlying BST now has 3 nodes which contain the docs where it's expected
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('hello'), [doc1]);
            assert.deepEqual(idx.tree.search('world'), [doc2]);
            assert.deepEqual(idx.tree.search('bloup'), [doc3]);

            // The nodes contain pointers to the actual documents
            idx.tree.search('bloup')[0].a = 42;
            assert.equal(doc3.a, 42);
        });

        test('Can insert an array of documents', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'};
            idx.insert([doc1, doc2, doc3]);
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('hello'), [doc1]);
            assert.deepEqual(idx.tree.search('world'), [doc2]);
            assert.deepEqual(idx.tree.search('bloup'), [doc3]);
        });

        test('When inserting an array of elements, if an error is thrown all inserts need to be rolled back', function () {
            var idx = new Index({fieldName: 'tf', unique: true}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc2b = {a: 84, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'};
            try {
                idx.insert([doc1, doc2, doc2b, doc3]);
            } catch (e) {
                assert.equal(e.errorType, 'uniqueViolated');
            }
            assert.equal(idx.tree.getNumberOfKeys(), 0);
            assert.deepEqual(idx.tree.search('hello'), []);
            assert.deepEqual(idx.tree.search('world'), []);
            assert.deepEqual(idx.tree.search('bloup'), []);
        });

        describe('Array fields', function () {
            test('Inserts one entry per array element in the index', function () {
                var obj = {tf: ['aa', 'bb'], really: 'yeah'},
                    obj2 = {tf: 'normal', yes: 'indeed'},
                    idx = new Index({fieldName: 'tf'});
                idx.insert(obj);
                assert.equal(idx.getAll().length, 2);
                assert.equal(idx.getAll()[0], obj);
                assert.equal(idx.getAll()[1], obj);

                idx.insert(obj2);
                assert.equal(idx.getAll().length, 3);
            });

            test('Inserts one entry per array element in the index, type-checked', function () {
                var obj = {tf: ['42', 42, new Date(42), 42], really: 'yeah'},
                    idx = new Index({fieldName: 'tf'});
                idx.insert(obj);
                assert.equal(idx.getAll().length, 3);
                assert.equal(idx.getAll()[0], obj);
                assert.equal(idx.getAll()[1], obj);
                assert.equal(idx.getAll()[2], obj);
            });

            test('Inserts one entry per unique array element in the index, the unique constraint only holds across documents', function () {
                var obj = {tf: ['aa', 'aa'], really: 'yeah'},
                    obj2 = {tf: ['cc', 'yy', 'cc'], yes: 'indeed'},
                    idx = new Index({fieldName: 'tf', unique: true});
                idx.insert(obj);
                assert.equal(idx.getAll().length, 1);
                assert.equal(idx.getAll()[0], obj);

                idx.insert(obj2);
                assert.equal(idx.getAll().length, 3);
            });

            test('The unique constraint holds across documents', function () {
                var obj = {tf: ['aa', 'aa'], really: 'yeah'},
                    obj2 = {tf: ['cc', 'aa', 'cc'], yes: 'indeed'},
                    idx = new Index({fieldName: 'tf', unique: true});
                idx.insert(obj);
                assert.equal(idx.getAll().length, 1);
                assert.equal(idx.getAll()[0], obj);

                assert.throws(function () {
                    idx.insert(obj2);
                });
            });

            test('When removing a document, remove it from the index at all unique array elements', function () {
                var obj = {tf: ['aa', 'aa'], really: 'yeah'},
                    obj2 = {tf: ['cc', 'aa', 'cc'], yes: 'indeed'},
                    idx = new Index({fieldName: 'tf'});
                idx.insert(obj);
                idx.insert(obj2);
                assert.equal(idx.getMatching('aa').length, 2);

                assert.notEqual(idx.getMatching('aa').indexOf(obj), -1);
                assert.notEqual(idx.getMatching('aa').indexOf(obj2), -1);

                assert.equal(idx.getMatching('cc').length, 1);

                idx.remove(obj2);
                assert.equal(idx.getMatching('aa').length, 1);
                assert.notEqual(idx.getMatching('aa').indexOf(obj), -1);
                assert.equal(idx.getMatching('aa').indexOf(obj2), -1);
                assert.equal(idx.getMatching('cc').length, 0);
            });

            test('If a unique constraint is violated when inserting an array key, roll back all inserts before the key', function () {
                var obj = {tf: ['aa', 'bb'], really: 'yeah'},
                    obj2 = {tf: ['cc', 'dd', 'aa', 'ee'], yes: 'indeed'},
                    idx = new Index({fieldName: 'tf', unique: true});
                idx.insert(obj);
                assert.equal(idx.getAll().length, 2);
                assert.equal(idx.getMatching('aa').length, 1);
                assert.equal(idx.getMatching('bb').length, 1);
                assert.equal(idx.getMatching('cc').length, 0);
                assert.equal(idx.getMatching('dd').length, 0);
                assert.equal(idx.getMatching('ee').length, 0);

                assert.throws(function () {
                    idx.insert(obj2);
                });

                assert.equal(idx.getAll().length, 2);
                assert.equal(idx.getMatching('aa').length, 1);
                assert.equal(idx.getMatching('bb').length, 1);
                assert.equal(idx.getMatching('cc').length, 0);
                assert.equal(idx.getMatching('dd').length, 0);
                assert.equal(idx.getMatching('ee').length, 0);
            });
        }); // ==== End of 'Array fields' ==== //
    }); // ==== End of 'Insertion' ==== //

    describe('Removal', function () {
        test('Can remove pointers from the index, even when multiple documents have the same key', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                doc4 = {a: 23, tf: 'world'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            idx.insert(doc4);
            assert.equal(idx.tree.getNumberOfKeys(), 3);

            idx.remove(doc1);
            assert.equal(idx.tree.getNumberOfKeys(), 2);
            assert.equal(idx.tree.search('hello').length, 0);

            idx.remove(doc2);
            assert.equal(idx.tree.getNumberOfKeys(), 2);
            assert.equal(idx.tree.search('world').length, 1);
            assert.equal(idx.tree.search('world')[0], doc4);
        });

        test('If we have a sparse index, removing a non indexed doc has no effect', function () {
            var idx = new Index({fieldName: 'nope', sparse: true}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 5, tf: 'world'};
            idx.insert(doc1);
            idx.insert(doc2);
            assert.equal(idx.tree.getNumberOfKeys(), 0);

            idx.remove(doc1);
            assert.equal(idx.tree.getNumberOfKeys(), 0);
        });

        test('Works with dot notation', function () {
            var idx = new Index({fieldName: 'tf.nested'}),
                doc1 = {a: 5, tf: {nested: 'hello'}},
                doc2 = {a: 8, tf: {nested: 'world', additional: true}},
                doc3 = {a: 2, tf: {nested: 'bloup', age: 42}},
                doc4 = {a: 2, tf: {nested: 'world', fruits: ['apple', 'carrot']}};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            idx.insert(doc4);
            assert.equal(idx.tree.getNumberOfKeys(), 3);

            idx.remove(doc1);
            assert.equal(idx.tree.getNumberOfKeys(), 2);
            assert.equal(idx.tree.search('hello').length, 0);

            idx.remove(doc2);
            assert.equal(idx.tree.getNumberOfKeys(), 2);
            assert.equal(idx.tree.search('world').length, 1);
            assert.equal(idx.tree.search('world')[0], doc4);
        });

        test('Can remove an array of documents', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'};
            idx.insert([doc1, doc2, doc3]);
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            idx.remove([doc1, doc3]);
            assert.equal(idx.tree.getNumberOfKeys(), 1);
            assert.deepEqual(idx.tree.search('hello'), []);
            assert.deepEqual(idx.tree.search('world'), [doc2]);
            assert.deepEqual(idx.tree.search('bloup'), []);
        });
    }); // ==== End of 'Removal' ==== //

    describe('Update', function () {
        test('Can update a document whose key did or didnt change', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                doc4 = {a: 23, tf: 'world'},
                doc5 = {a: 1, tf: 'changed'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('world'), [doc2]);

            idx.update(doc2, doc4);
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('world'), [doc4]);

            idx.update(doc1, doc5);
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('hello'), []);
            assert.deepEqual(idx.tree.search('changed'), [doc5]);
        });

        test('If a simple update violates a unique constraint, changes are rolled back and an error thrown', function () {
            var idx = new Index({fieldName: 'tf', unique: true}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                bad = {a: 23, tf: 'world'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('hello'), [doc1]);
            assert.deepEqual(idx.tree.search('world'), [doc2]);
            assert.deepEqual(idx.tree.search('bloup'), [doc3]);

            try {
                idx.update(doc3, bad);
            } catch (e) {
                assert.equal(e.errorType, 'uniqueViolated');
            }

            // No change
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('hello'), [doc1]);
            assert.deepEqual(idx.tree.search('world'), [doc2]);
            assert.deepEqual(idx.tree.search('bloup'), [doc3]);
        });

        test('Can update an array of documents', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                doc1b = {a: 23, tf: 'world'},
                doc2b = {a: 1, tf: 'changed'},
                doc3b = {a: 44, tf: 'bloup'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            assert.equal(idx.tree.getNumberOfKeys(), 3);

            idx.update([
                {oldDoc: doc1, newDoc: doc1b},
                {oldDoc: doc2, newDoc: doc2b},
                {oldDoc: doc3, newDoc: doc3b},
            ]);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('world')[0], doc1b);
            assert.equal(idx.getMatching('changed').length, 1);
            assert.equal(idx.getMatching('changed')[0], doc2b);
            assert.equal(idx.getMatching('bloup').length, 1);
            assert.equal(idx.getMatching('bloup')[0], doc3b);
        });

        test('If a unique constraint is violated during an array-update, all changes are rolled back and an error thrown', function () {
            var idx = new Index({fieldName: 'tf', unique: true}),
                doc0 = {a: 432, tf: 'notthistoo'},
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                doc1b = {a: 23, tf: 'changed'},
                doc2b = {a: 1, tf: 'changed'}, // Will violate the constraint (first try)
                doc2c = {a: 1, tf: 'notthistoo'}, // Will violate the constraint (second try)
                doc3b = {a: 44, tf: 'alsochanged'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            assert.equal(idx.tree.getNumberOfKeys(), 3);

            try {
                idx.update([
                    {oldDoc: doc1, newDoc: doc1b},
                    {oldDoc: doc2, newDoc: doc2b},
                    {oldDoc: doc3, newDoc: doc3b},
                ]);
            } catch (e) {
                assert.equal(e.errorType, 'uniqueViolated');
            }

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('hello').length, 1);
            assert.equal(idx.getMatching('hello')[0], doc1);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('world')[0], doc2);
            assert.equal(idx.getMatching('bloup').length, 1);
            assert.equal(idx.getMatching('bloup')[0], doc3);

            try {
                idx.update([
                    {oldDoc: doc1, newDoc: doc1b},
                    {oldDoc: doc2, newDoc: doc2b},
                    {oldDoc: doc3, newDoc: doc3b},
                ]);
            } catch (e) {
                assert.equal(e.errorType, 'uniqueViolated');
            }

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('hello').length, 1);
            assert.equal(idx.getMatching('hello')[0], doc1);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('world')[0], doc2);
            assert.equal(idx.getMatching('bloup').length, 1);
            assert.equal(idx.getMatching('bloup')[0], doc3);
        });

        test('If an update doesnt change a document, the unique constraint is not violated', function () {
            var idx = new Index({fieldName: 'tf', unique: true}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                noChange = {a: 8, tf: 'world'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('world'), [doc2]);

            idx.update(doc2, noChange); // No error thrown
            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.deepEqual(idx.tree.search('world'), [noChange]);
        });

        test('Can revert simple and batch updates', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                doc1b = {a: 23, tf: 'world'},
                doc2b = {a: 1, tf: 'changed'},
                doc3b = {a: 44, tf: 'bloup'},
                batchUpdate = [
                    {oldDoc: doc1, newDoc: doc1b},
                    {oldDoc: doc2, newDoc: doc2b},
                    {oldDoc: doc3, newDoc: doc3b},
                ];
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            assert.equal(idx.tree.getNumberOfKeys(), 3);

            idx.update(batchUpdate);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('world')[0], doc1b);
            assert.equal(idx.getMatching('changed').length, 1);
            assert.equal(idx.getMatching('changed')[0], doc2b);
            assert.equal(idx.getMatching('bloup').length, 1);
            assert.equal(idx.getMatching('bloup')[0], doc3b);

            idx.revertUpdate(batchUpdate);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('hello').length, 1);
            assert.equal(idx.getMatching('hello')[0], doc1);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('world')[0], doc2);
            assert.equal(idx.getMatching('bloup').length, 1);
            assert.equal(idx.getMatching('bloup')[0], doc3);

            // Now a simple update
            idx.update(doc2, doc2b);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('hello').length, 1);
            assert.equal(idx.getMatching('hello')[0], doc1);
            assert.equal(idx.getMatching('changed').length, 1);
            assert.equal(idx.getMatching('changed')[0], doc2b);
            assert.equal(idx.getMatching('bloup').length, 1);
            assert.equal(idx.getMatching('bloup')[0], doc3);

            idx.revertUpdate(doc2, doc2b);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('hello').length, 1);
            assert.equal(idx.getMatching('hello')[0], doc1);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('world')[0], doc2);
            assert.equal(idx.getMatching('bloup').length, 1);
            assert.equal(idx.getMatching('bloup')[0], doc3);
        });
    }); // ==== End of 'Update' ==== //

    describe('Get matching documents', function () {
        test('Get all documents where fieldName is equal to the given value, or an empty array if no match', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                doc4 = {a: 23, tf: 'world'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            idx.insert(doc4);

            assert.deepEqual(idx.getMatching('bloup'), [doc3]);
            assert.deepEqual(idx.getMatching('world'), [doc2, doc4]);
            assert.deepEqual(idx.getMatching('nope'), []);
        });

        test('Can get all documents for a given key in a unique index', function () {
            var idx = new Index({fieldName: 'tf', unique: true}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            assert.deepEqual(idx.getMatching('bloup'), [doc3]);
            assert.deepEqual(idx.getMatching('world'), [doc2]);
            assert.deepEqual(idx.getMatching('nope'), []);
        });

        test('Can get all documents for which a field is undefined', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 2, nottf: 'bloup'},
                doc3 = {a: 8, tf: 'world'},
                doc4 = {a: 7, nottf: 'yes'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            assert.deepEqual(idx.getMatching('bloup'), []);
            assert.deepEqual(idx.getMatching('hello'), [doc1]);
            assert.deepEqual(idx.getMatching('world'), [doc3]);
            assert.deepEqual(idx.getMatching('yes'), []);
            assert.deepEqual(idx.getMatching(undefined), [doc2]);

            idx.insert(doc4);

            assert.deepEqual(idx.getMatching('bloup'), []);
            assert.deepEqual(idx.getMatching('hello'), [doc1]);
            assert.deepEqual(idx.getMatching('world'), [doc3]);
            assert.deepEqual(idx.getMatching('yes'), []);
            assert.deepEqual(idx.getMatching(undefined), [doc2, doc4]);
        });

        test('Can get all documents for which a field is null', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 2, tf: null},
                doc3 = {a: 8, tf: 'world'},
                doc4 = {a: 7, tf: null};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            assert.deepEqual(idx.getMatching('bloup'), []);
            assert.deepEqual(idx.getMatching('hello'), [doc1]);
            assert.deepEqual(idx.getMatching('world'), [doc3]);
            assert.deepEqual(idx.getMatching('yes'), []);
            assert.deepEqual(idx.getMatching(null), [doc2]);

            idx.insert(doc4);

            assert.deepEqual(idx.getMatching('bloup'), []);
            assert.deepEqual(idx.getMatching('hello'), [doc1]);
            assert.deepEqual(idx.getMatching('world'), [doc3]);
            assert.deepEqual(idx.getMatching('yes'), []);
            assert.deepEqual(idx.getMatching(null), [doc2, doc4]);
        });

        test('Can get all documents for a given key in a sparse index, but not unindexed docs (= field undefined)', function () {
            var idx = new Index({fieldName: 'tf', sparse: true}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 2, nottf: 'bloup'},
                doc3 = {a: 8, tf: 'world'},
                doc4 = {a: 7, nottf: 'yes'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            idx.insert(doc4);

            assert.deepEqual(idx.getMatching('bloup'), []);
            assert.deepEqual(idx.getMatching('hello'), [doc1]);
            assert.deepEqual(idx.getMatching('world'), [doc3]);
            assert.deepEqual(idx.getMatching('yes'), []);
            assert.deepEqual(idx.getMatching(undefined), []);
        });

        test('Can get all documents whose key is in an array of keys', function () {
            // For this test only we have to use objects with _ids as the array version of getMatching
            // relies on the _id property being set, otherwise we have to use a quadratic algorithm
            // or a fingerprinting algorithm, both solutions too complicated and slow given that live nedb
            // indexes documents with _id always set
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello', _id: '1'},
                doc2 = {a: 2, tf: 'bloup', _id: '2'},
                doc3 = {a: 8, tf: 'world', _id: '3'},
                doc4 = {a: 7, tf: 'yes', _id: '4'},
                doc5 = {a: 7, tf: 'yes', _id: '5'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            idx.insert(doc4);
            idx.insert(doc5);

            assert.deepEqual(idx.getMatching([]), []);
            assert.deepEqual(idx.getMatching(['bloup']), [doc2]);
            assert.deepEqual(idx.getMatching(['bloup', 'yes']), [doc2, doc4, doc5]);
            assert.deepEqual(idx.getMatching(['hello', 'no']), [doc1]);
            assert.deepEqual(idx.getMatching(['nope', 'no']), []);
        });

        test('Can get all documents whose key is between certain bounds', function () {
            var idx = new Index({fieldName: 'a'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 2, tf: 'bloup'},
                doc3 = {a: 8, tf: 'world'},
                doc4 = {a: 7, tf: 'yes'},
                doc5 = {a: 10, tf: 'yes'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);
            idx.insert(doc4);
            idx.insert(doc5);

            assert.deepEqual(idx.getBetweenBounds({$lt: 10, $gte: 5}), [doc1, doc4, doc3]);
            assert.deepEqual(idx.getBetweenBounds({$lte: 8}), [doc2, doc1, doc4, doc3]);
            assert.deepEqual(idx.getBetweenBounds({$gt: 7}), [doc3, doc5]);
        });
    }); // ==== End of 'Get matching documents' ==== //

    describe('Resetting', function () {
        test('Can reset an index without any new data, the index will be empty afterwards', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('hello').length, 1);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('bloup').length, 1);

            idx.reset();
            assert.equal(idx.tree.getNumberOfKeys(), 0);
            assert.equal(idx.getMatching('hello').length, 0);
            assert.equal(idx.getMatching('world').length, 0);
            assert.equal(idx.getMatching('bloup').length, 0);
        });

        test('Can reset an index and initialize it with one document', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                newDoc = {a: 555, tf: 'new'};
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('hello').length, 1);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('bloup').length, 1);

            idx.reset(newDoc);
            assert.equal(idx.tree.getNumberOfKeys(), 1);
            assert.equal(idx.getMatching('hello').length, 0);
            assert.equal(idx.getMatching('world').length, 0);
            assert.equal(idx.getMatching('bloup').length, 0);
            assert.equal(idx.getMatching('new')[0].a, 555);
        });

        test('Can reset an index and initialize it with an array of documents', function () {
            var idx = new Index({fieldName: 'tf'}),
                doc1 = {a: 5, tf: 'hello'},
                doc2 = {a: 8, tf: 'world'},
                doc3 = {a: 2, tf: 'bloup'},
                newDocs = [
                    {a: 555, tf: 'new'},
                    {a: 666, tf: 'again'},
                ];
            idx.insert(doc1);
            idx.insert(doc2);
            idx.insert(doc3);

            assert.equal(idx.tree.getNumberOfKeys(), 3);
            assert.equal(idx.getMatching('hello').length, 1);
            assert.equal(idx.getMatching('world').length, 1);
            assert.equal(idx.getMatching('bloup').length, 1);

            idx.reset(newDocs);
            assert.equal(idx.tree.getNumberOfKeys(), 2);
            assert.equal(idx.getMatching('hello').length, 0);
            assert.equal(idx.getMatching('world').length, 0);
            assert.equal(idx.getMatching('bloup').length, 0);
            assert.equal(idx.getMatching('new')[0].a, 555);
            assert.equal(idx.getMatching('again')[0].a, 666);
        });
    }); // ==== End of 'Resetting' ==== //

    test('Get all elements in the index', function () {
        var idx = new Index({fieldName: 'a'}),
            doc1 = {a: 5, tf: 'hello'},
            doc2 = {a: 8, tf: 'world'},
            doc3 = {a: 2, tf: 'bloup'};
        idx.insert(doc1);
        idx.insert(doc2);
        idx.insert(doc3);

        assert.deepEqual(idx.getAll(), [
            {a: 2, tf: 'bloup'},
            {a: 5, tf: 'hello'},
            {a: 8, tf: 'world'},
        ]);
    });
});
