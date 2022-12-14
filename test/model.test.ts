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
import customUtils from '../lib/customUtils';
import Cursor from '../lib/cursor';
import storage from '../lib/storage';
import child_process from 'child_process';
import os from 'os';
import util from 'util';

const testDb = 'workspace/test.db';

describe('Model', function () {
    describe('Serialization, deserialization', function () {
        test('Can serialize and deserialize strings', function () {
            var a, b, c;

            a = {test: 'Some string'};
            b = model.serialize(a);
            c = model.deserialize(b);
            assert.equal(b.indexOf('\n'), -1);
            assert.equal(c.test, 'Some string');

            // Even if a property is a string containing a new line, the serialized
            // version doesn't. The new line must still be there upon deserialization
            a = {test: 'With a new\nline'};
            b = model.serialize(a);
            c = model.deserialize(b);

            assert.equal(c.test, 'With a new\nline');
            assert.equal(a.test.includes('\n'), true);
            assert.equal(b.indexOf('\n'), -1);
            assert.equal(c.test.includes('\n'), true);
        });

        test('Can serialize and deserialize booleans', function () {
            var a, b, c;

            a = {test: true};
            b = model.serialize(a);
            c = model.deserialize(b);
            assert.equal(b.indexOf('\n'), -1);
            assert.equal(c.test, true);
        });

        test('Can serialize and deserialize numbers', function () {
            var a, b, c;

            a = {test: 5};
            b = model.serialize(a);
            c = model.deserialize(b);
            assert.equal(b.indexOf('\n'), -1);
            assert.equal(c.test, 5);
        });

        test('Can serialize and deserialize null', function () {
            var a, b, c;

            a = {test: null};
            b = model.serialize(a);
            c = model.deserialize(b);
            assert.equal(b.indexOf('\n'), -1);
            assert.equal(a.test, null);
        });

        test('undefined fields are removed when serialized', function () {
            var a = {bloup: undefined, hello: 'world'},
                b = model.serialize(a),
                c = model.deserialize(b);
            assert.equal(Object.keys(c).length, 1);
            assert.equal(c.hello, 'world');
            assert.equal(c.bloup, undefined);
        });

        test('Can serialize and deserialize a date', function () {
            var a,
                b,
                c,
                d = new Date();

            a = {test: d};
            b = model.serialize(a);
            c = model.deserialize(b);
            assert.equal(b.indexOf('\n'), -1);
            assert.equal(b, '{"test":{"$$date":' + d.getTime() + '}}');
            assert.equal(util.isDate(c.test), true);
            assert.equal(c.test.getTime(), d.getTime());
        });

        test('Can serialize and deserialize sub objects', function () {
            var a,
                b,
                c,
                d = new Date();

            a = {test: {something: 39, also: d, yes: {again: 'yes'}}};
            b = model.serialize(a);
            c = model.deserialize(b);
            assert.equal(b.indexOf('\n'), -1);
            assert.equal(c.test.something, 39);
            assert.equal(c.test.also.getTime(), d.getTime());
            assert.equal(c.test.yes.again, 'yes');
        });

        test('Can serialize and deserialize sub arrays', function () {
            var a,
                b,
                c,
                d = new Date();

            a = {test: [39, d, {again: 'yes'}]};
            b = model.serialize(a);
            c = model.deserialize(b);
            assert.equal(b.indexOf('\n'), -1);
            assert.equal(c.test[0], 39);
            assert.equal(c.test[1].getTime(), d.getTime());
            assert.equal(c.test[2].again, 'yes');
        });

        test('Reject field names beginning with a $ sign or containing a dot, except the four edge cases', function () {
            var a1 = {$something: 'totest'},
                a2 = {'with.dot': 'totest'},
                e1 = {$$date: 4321},
                e2 = {$$deleted: true},
                e3 = {$$indexCreated: 'indexName'},
                e4 = {$$indexRemoved: 'indexName'},
                b;

            // Normal cases
            assert.throws(function () {
                b = model.serialize(a1);
            });
            assert.throws(function () {
                b = model.serialize(a2);
            });

            // Edge cases
            b = model.serialize(e1);
            b = model.serialize(e2);
            b = model.serialize(e3);
            b = model.serialize(e4);
        });

        test('Can serialize string fields with a new line without breaking the DB', function (done) {
            var db1,
                db2,
                badString = 'world\r\nearth\nother\rline';
            if (fs.existsSync('workspace/test1.db')) {
                fs.unlinkSync('workspace/test1.db');
            }
            assert.equal(fs.existsSync('workspace/test1.db'), false);
            db1 = new Datastore({filename: 'workspace/test1.db'});

            db1.loadDatabase(function (err) {
                assert.equal(err, null);
                db1.insert({hello: badString}, function (err) {
                    assert.equal(err, null);

                    db2 = new Datastore({filename: 'workspace/test1.db'});
                    db2.loadDatabase(function (err) {
                        assert.equal(err, null);
                        db2.find({}, function (err, docs) {
                            assert.equal(err, null);
                            assert.equal(docs.length, 1);
                            assert.equal(docs[0].hello, badString);

                            done();
                        });
                    });
                });
            });
        });

        test('Can accept objects whose keys are numbers', function () {
            var o = {42: true};

            var s = model.serialize(o);
        });
    }); // ==== End of 'Serialization, deserialization' ==== //

    describe('Object checking', function () {
        test('Field names beginning with a $ sign are forbidden', function () {
            assert.notEqual(typeof model.checkObject, 'undefined');

            assert.throws(function () {
                model.checkObject({$bad: true});
            });

            assert.throws(function () {
                model.checkObject({some: 42, nested: {again: 'no', $worse: true}});
            });

            // This shouldn't throw since "$actuallyok" is not a field name
            model.checkObject({some: 42, nested: [5, 'no', '$actuallyok', true]});

            assert.throws(function () {
                model.checkObject({some: 42, nested: [5, 'no', '$actuallyok', true, {$hidden: 'useless'}]});
            });
        });

        test('Field names cannot contain a .', function () {
            assert.notEqual(typeof model.checkObject, 'undefined');

            assert.throws(function () {
                model.checkObject({'so.bad': true});
            });

            // Recursive behaviour testing done in the above test on $ signs
        });

        test('Properties with a null value dont trigger an error', function () {
            var obj = {prop: null};

            model.checkObject(obj);
        });

        test('Can check if an object is a primitive or not', function () {
            assert.equal(model.isPrimitiveType(5), true);
            assert.equal(model.isPrimitiveType('sdsfdfs'), true);
            assert.equal(model.isPrimitiveType(0), true);
            assert.equal(model.isPrimitiveType(true), true);
            assert.equal(model.isPrimitiveType(false), true);
            assert.equal(model.isPrimitiveType(new Date()), true);
            assert.equal(model.isPrimitiveType([]), true);
            assert.equal(model.isPrimitiveType([3, 'try']), true);
            assert.equal(model.isPrimitiveType(null), true);

            assert.equal(model.isPrimitiveType({}), false);
            assert.equal(model.isPrimitiveType({a: 42}), false);
        });
    }); // ==== End of 'Object checking' ==== //

    describe('Deep copying', function () {
        test('Should be able to deep copy any serializable model', function () {
            var d = new Date(),
                obj = {a: ['ee', 'ff', 42], date: d, subobj: {a: 'b', b: 'c'}},
                res = model.deepCopy(obj);
            assert.equal(res.a.length, 3);
            assert.equal(res.a[0], 'ee');
            assert.equal(res.a[1], 'ff');
            assert.equal(res.a[2], 42);
            assert.equal(res.date.getTime(), d.getTime());
            assert.equal(res.subobj.a, 'b');
            assert.equal(res.subobj.b, 'c');

            obj.a.push('ggg');
            obj.date = 'notadate';
            obj.subobj = [];

            // Even if the original object is modified, the copied one isn't
            assert.equal(res.a.length, 3);
            assert.equal(res.a[0], 'ee');
            assert.equal(res.a[1], 'ff');
            assert.equal(res.a[2], 42);
            assert.equal(res.date.getTime(), d.getTime());
            assert.equal(res.subobj.a, 'b');
            assert.equal(res.subobj.b, 'c');
        });

        test('Should deep copy the contents of an array', function () {
            var a = [{hello: 'world'}],
                b = model.deepCopy(a);
            assert.equal(b[0].hello, 'world');
            b[0].hello = 'another';
            assert.equal(b[0].hello, 'another');
            assert.equal(a[0].hello, 'world');
        });

        test('Without the strictKeys option, everything gets deep copied', function () {
            var a = {
                    a: 4,
                    $e: 'rrr',
                    'eee.rt': 42,
                    nested: {yes: 1, 'tt.yy': 2, $nopenope: 3},
                    array: [{'rr.hh': 1}, {yes: true}, {$yes: false}],
                },
                b = model.deepCopy(a);
            assert.deepEqual(a, b);
        });

        test('With the strictKeys option, only valid keys gets deep copied', function () {
            var a = {
                    a: 4,
                    $e: 'rrr',
                    'eee.rt': 42,
                    nested: {yes: 1, 'tt.yy': 2, $nopenope: 3},
                    array: [{'rr.hh': 1}, {yes: true}, {$yes: false}],
                },
                b = model.deepCopy(a, true);
            assert.deepEqual(b, {a: 4, nested: {yes: 1}, array: [{}, {yes: true}, {}]});
        });
    }); // ==== End of 'Deep copying' ==== //

    describe('Modifying documents', function () {
        test('Queries not containing any modifier just replace the document by the contents of the query but keep its _id', function () {
            var obj = {some: 'thing', _id: 'keepit'},
                updateQuery = {replace: 'done', bloup: [1, 8]},
                t;

            t = model.modify(obj, updateQuery);
            assert.equal(t.replace, 'done');
            assert.equal(t.bloup.length, 2);
            assert.equal(t.bloup[0], 1);
            assert.equal(t.bloup[1], 8);

            assert.equal(t.some, undefined);
            assert.equal(t._id, 'keepit');
        });

        test('Throw an error if trying to change the _id field in a copy-type modification', function () {
            var obj = {some: 'thing', _id: 'keepit'},
                updateQuery = {replace: 'done', bloup: [1, 8], _id: 'donttryit'};
            assert.throws(function () {
                model.modify(obj, updateQuery);
            }, /You cannot change a document's _id/);

            updateQuery._id = 'keepit';
            model.modify(obj, updateQuery); // No error thrown
        });

        test('Throw an error if trying to use modify in a mixed copy+modify way', function () {
            var obj = {some: 'thing'},
                updateQuery = {replace: 'me', $modify: 'metoo'};

            assert.throws(function () {
                model.modify(obj, updateQuery);
            }, /You cannot mix modifiers and normal fields/);
        });

        test('Throw an error if trying to use an inexistent modifier', function () {
            var obj = {some: 'thing'},
                updateQuery = {$set: {it: 'exists'}, $modify: 'not this one'};

            assert.throws(function () {
                model.modify(obj, updateQuery);
            }, /Unknown modifier \$modify/);
        });

        test('Throw an error if a modifier is used with a non-object argument', function () {
            var obj = {some: 'thing'},
                updateQuery = {$set: 'this exists'};

            assert.throws(function () {
                model.modify(obj, updateQuery);
            }, /Modifier \$set's argument must be an object/);
        });

        describe('$set modifier', function () {
            test('Can change already set fields without modfifying the underlying object', function () {
                var obj = {some: 'thing', yup: 'yes', nay: 'noes'},
                    updateQuery = {$set: {some: 'changed', nay: 'yes indeed'}},
                    modified = model.modify(obj, updateQuery);

                assert.equal(Object.keys(modified).length, 3);
                assert.equal(modified.some, 'changed');
                assert.equal(modified.yup, 'yes');
                assert.equal(modified.nay, 'yes indeed');

                assert.equal(Object.keys(obj).length, 3);
                assert.equal(obj.some, 'thing');
                assert.equal(obj.yup, 'yes');
                assert.equal(obj.nay, 'noes');
            });

            test('Creates fields to set if they dont exist yet', function () {
                var obj = {yup: 'yes'},
                    updateQuery = {$set: {some: 'changed', nay: 'yes indeed'}},
                    modified = model.modify(obj, updateQuery);

                assert.equal(Object.keys(modified).length, 3);
                assert.equal(modified.some, 'changed');
                assert.equal(modified.yup, 'yes');
                assert.equal(modified.nay, 'yes indeed');
            });

            test('Can set sub-fields and create them if necessary', function () {
                var obj = {yup: {subfield: 'bloup'}},
                    updateQuery = {
                        $set: {
                            'yup.subfield': 'changed',
                            'yup.yop': 'yes indeed',
                            'totally.doesnt.exist': 'now it does',
                        },
                    },
                    modified = model.modify(obj, updateQuery);

                assert.equal(
                    _.isEqual(modified, {
                        yup: {subfield: 'changed', yop: 'yes indeed'},
                        totally: {doesnt: {exist: 'now it does'}},
                    }),
                    true
                );
            });

            test("Doesn't replace a falsy field by an object when recursively following dot notation", function () {
                var obj = {nested: false},
                    updateQuery = {$set: {'nested.now': 'it is'}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {nested: false}); // Object not modified as the nested field doesn't exist
            });
        }); // End of '$set modifier'

        describe('$unset modifier', function () {
            test('Can delete a field, not throwing an error if the field doesnt exist', function () {
                var obj, updateQuery, modified;

                obj = {yup: 'yes', other: 'also'};
                updateQuery = {$unset: {yup: true}};
                modified = model.modify(obj, updateQuery);
                assert.deepEqual(modified, {other: 'also'});

                obj = {yup: 'yes', other: 'also'};
                updateQuery = {$unset: {nope: true}};
                modified = model.modify(obj, updateQuery);
                assert.deepEqual(modified, obj);

                obj = {yup: 'yes', other: 'also'};
                updateQuery = {$unset: {nope: true, other: true}};
                modified = model.modify(obj, updateQuery);
                assert.deepEqual(modified, {yup: 'yes'});
            });

            test('Can unset sub-fields and entire nested documents', function () {
                var obj, updateQuery, modified;

                obj = {yup: 'yes', nested: {a: 'also', b: 'yeah'}};
                updateQuery = {$unset: {nested: true}};
                modified = model.modify(obj, updateQuery);
                assert.deepEqual(modified, {yup: 'yes'});

                obj = {yup: 'yes', nested: {a: 'also', b: 'yeah'}};
                updateQuery = {$unset: {'nested.a': true}};
                modified = model.modify(obj, updateQuery);
                assert.deepEqual(modified, {yup: 'yes', nested: {b: 'yeah'}});

                obj = {yup: 'yes', nested: {a: 'also', b: 'yeah'}};
                updateQuery = {$unset: {'nested.a': true, 'nested.b': true}};
                modified = model.modify(obj, updateQuery);
                assert.deepEqual(modified, {yup: 'yes', nested: {}});
            });

            test('When unsetting nested fields, should not create an empty parent to nested field', function () {
                var obj = model.modify({argh: true}, {$unset: {'bad.worse': true}});
                assert.deepEqual(obj, {argh: true});

                obj = model.modify({argh: true, bad: {worse: 'oh'}}, {$unset: {'bad.worse': true}});
                assert.deepEqual(obj, {argh: true, bad: {}});

                obj = model.modify({argh: true, bad: {}}, {$unset: {'bad.worse': true}});
                assert.deepEqual(obj, {argh: true, bad: {}});
            });
        }); // End of '$unset modifier'

        describe('$inc modifier', function () {
            test('Throw an error if you try to use it with a non-number or on a non number field', function () {
                assert.throws(function () {
                    var obj = {some: 'thing', yup: 'yes', nay: 2},
                        updateQuery = {$inc: {nay: 'notanumber'}},
                        modified = model.modify(obj, updateQuery);
                });

                assert.throws(function () {
                    var obj = {some: 'thing', yup: 'yes', nay: 'nope'},
                        updateQuery = {$inc: {nay: 1}},
                        modified = model.modify(obj, updateQuery);
                });
            });

            test('Can increment number fields or create and initialize them if needed', function () {
                var obj = {some: 'thing', nay: 40},
                    modified;

                modified = model.modify(obj, {$inc: {nay: 2}});
                assert.equal(_.isEqual(modified, {some: 'thing', nay: 42}), true);

                // Incidentally, this tests that obj was not modified
                modified = model.modify(obj, {$inc: {inexistent: -6}});
                assert.equal(_.isEqual(modified, {some: 'thing', nay: 40, inexistent: -6}), true);
            });

            test('Works recursively', function () {
                var obj = {some: 'thing', nay: {nope: 40}},
                    modified;

                modified = model.modify(obj, {$inc: {'nay.nope': -2, 'blip.blop': 123}});
                assert.equal(_.isEqual(modified, {some: 'thing', nay: {nope: 38}, blip: {blop: 123}}), true);
            });
        }); // End of '$inc modifier'

        describe('$push modifier', function () {
            test('Can push an element to the end of an array', function () {
                var obj = {arr: ['hello']},
                    modified;

                modified = model.modify(obj, {$push: {arr: 'world'}});
                assert.deepEqual(modified, {arr: ['hello', 'world']});
            });

            test('Can push an element to a non-existent field and will create the array', function () {
                var obj = {},
                    modified;

                modified = model.modify(obj, {$push: {arr: 'world'}});
                assert.deepEqual(modified, {arr: ['world']});
            });

            test('Can push on nested fields', function () {
                var obj = {arr: {nested: ['hello']}},
                    modified;

                modified = model.modify(obj, {$push: {'arr.nested': 'world'}});
                assert.deepEqual(modified, {arr: {nested: ['hello', 'world']}});

                obj = {arr: {a: 2}};
                modified = model.modify(obj, {$push: {'arr.nested': 'world'}});
                assert.deepEqual(modified, {arr: {a: 2, nested: ['world']}});
            });

            test('Throw if we try to push to a non-array', function () {
                var obj = {arr: 'hello'},
                    modified;

                assert.throws(function () {
                    modified = model.modify(obj, {$push: {arr: 'world'}});
                });

                obj = {arr: {nested: 45}};
                assert.throws(function () {
                    modified = model.modify(obj, {$push: {'arr.nested': 'world'}});
                });
            });

            test('Can use the $each modifier to add multiple values to an array at once', function () {
                var obj = {arr: ['hello']},
                    modified;

                modified = model.modify(obj, {$push: {arr: {$each: ['world', 'earth', 'everything']}}});
                assert.deepEqual(modified, {arr: ['hello', 'world', 'earth', 'everything']});

                assert.throws(function () {
                    modified = model.modify(obj, {$push: {arr: {$each: 45}}});
                });

                assert.throws(function () {
                    modified = model.modify(obj, {$push: {arr: {$each: ['world'], unauthorized: true}}});
                });
            });

            test('Can use the $slice modifier to limit the number of array elements', function () {
                var obj = {arr: ['hello']},
                    modified;

                modified = model.modify(obj, {$push: {arr: {$each: ['world', 'earth', 'everything'], $slice: 1}}});
                assert.deepEqual(modified, {arr: ['hello']});

                modified = model.modify(obj, {$push: {arr: {$each: ['world', 'earth', 'everything'], $slice: -1}}});
                assert.deepEqual(modified, {arr: ['everything']});

                modified = model.modify(obj, {$push: {arr: {$each: ['world', 'earth', 'everything'], $slice: 0}}});
                assert.deepEqual(modified, {arr: []});

                modified = model.modify(obj, {$push: {arr: {$each: ['world', 'earth', 'everything'], $slice: 2}}});
                assert.deepEqual(modified, {arr: ['hello', 'world']});

                modified = model.modify(obj, {$push: {arr: {$each: ['world', 'earth', 'everything'], $slice: -2}}});
                assert.deepEqual(modified, {arr: ['earth', 'everything']});

                modified = model.modify(obj, {$push: {arr: {$each: ['world', 'earth', 'everything'], $slice: -20}}});
                assert.deepEqual(modified, {arr: ['hello', 'world', 'earth', 'everything']});

                modified = model.modify(obj, {$push: {arr: {$each: ['world', 'earth', 'everything'], $slice: 20}}});
                assert.deepEqual(modified, {arr: ['hello', 'world', 'earth', 'everything']});

                modified = model.modify(obj, {$push: {arr: {$each: [], $slice: 1}}});
                assert.deepEqual(modified, {arr: ['hello']});

                // $each not specified, but $slice is
                modified = model.modify(obj, {$push: {arr: {$slice: 1}}});
                assert.deepEqual(modified, {arr: ['hello']});

                assert.throws(function () {
                    modified = model.modify(obj, {$push: {arr: {$slice: 1, unauthorized: true}}});
                });

                assert.throws(function () {
                    modified = model.modify(obj, {$push: {arr: {$each: [], unauthorized: true}}});
                });
            });
        }); // End of '$push modifier'

        describe('$addToSet modifier', function () {
            test('Can add an element to a set', function () {
                var obj = {arr: ['hello']},
                    modified;

                modified = model.modify(obj, {$addToSet: {arr: 'world'}});
                assert.deepEqual(modified, {arr: ['hello', 'world']});

                obj = {arr: ['hello']};
                modified = model.modify(obj, {$addToSet: {arr: 'hello'}});
                assert.deepEqual(modified, {arr: ['hello']});
            });

            test('Can add an element to a non-existent set and will create the array', function () {
                var obj = {arr: []},
                    modified;

                modified = model.modify(obj, {$addToSet: {arr: 'world'}});
                assert.deepEqual(modified, {arr: ['world']});
            });

            test('Throw if we try to addToSet to a non-array', function () {
                var obj = {arr: 'hello'},
                    modified;

                assert.throws(function () {
                    modified = model.modify(obj, {$addToSet: {arr: 'world'}});
                });
            });

            test('Use deep-equality to check whether we can add a value to a set', function () {
                var obj = {arr: [{b: 2}]},
                    modified;

                modified = model.modify(obj, {$addToSet: {arr: {b: 3}}});
                assert.deepEqual(modified, {arr: [{b: 2}, {b: 3}]});

                obj = {arr: [{b: 2}]};
                modified = model.modify(obj, {$addToSet: {arr: {b: 2}}});
                assert.deepEqual(modified, {arr: [{b: 2}]});
            });

            test('Can use the $each modifier to add multiple values to a set at once', function () {
                var obj = {arr: ['hello']},
                    modified;

                modified = model.modify(obj, {$addToSet: {arr: {$each: ['world', 'earth', 'hello', 'earth']}}});
                assert.deepEqual(modified, {arr: ['hello', 'world', 'earth']});

                assert.throws(function () {
                    modified = model.modify(obj, {$addToSet: {arr: {$each: 45}}});
                });

                assert.throws(function () {
                    modified = model.modify(obj, {$addToSet: {arr: {$each: ['world'], unauthorized: true}}});
                });
            });
        }); // End of '$addToSet modifier'

        describe('$pop modifier', function () {
            test('Throw if called on a non array, a non defined field or a non integer', function () {
                var obj = {arr: 'hello'},
                    modified;

                assert.throws(function () {
                    modified = model.modify(obj, {$pop: {arr: 1}});
                });

                obj = {bloup: 'nope'};
                assert.throws(function () {
                    modified = model.modify(obj, {$pop: {arr: 1}});
                });

                obj = {arr: [1, 4, 8]};
                assert.throws(function () {
                    modified = model.modify(obj, {$pop: {arr: true}});
                });
            });

            test('Can remove the first and last element of an array', function () {
                var obj, modified;

                obj = {arr: [1, 4, 8]};
                modified = model.modify(obj, {$pop: {arr: 1}});
                assert.deepEqual(modified, {arr: [1, 4]});

                obj = {arr: [1, 4, 8]};
                modified = model.modify(obj, {$pop: {arr: -1}});
                assert.deepEqual(modified, {arr: [4, 8]});

                // Empty arrays are not changed
                obj = {arr: []};
                modified = model.modify(obj, {$pop: {arr: 1}});
                assert.deepEqual(modified, {arr: []});
                modified = model.modify(obj, {$pop: {arr: -1}});
                assert.deepEqual(modified, {arr: []});
            });
        }); // End of '$pop modifier'

        describe('$pull modifier', function () {
            test('Can remove an element from a set', function () {
                var obj = {arr: ['hello', 'world']},
                    modified;

                modified = model.modify(obj, {$pull: {arr: 'world'}});
                assert.deepEqual(modified, {arr: ['hello']});

                obj = {arr: ['hello']};
                modified = model.modify(obj, {$pull: {arr: 'world'}});
                assert.deepEqual(modified, {arr: ['hello']});
            });

            test('Can remove multiple matching elements', function () {
                var obj = {arr: ['hello', 'world', 'hello', 'world']},
                    modified;

                modified = model.modify(obj, {$pull: {arr: 'world'}});
                assert.deepEqual(modified, {arr: ['hello', 'hello']});
            });

            test('Throw if we try to pull from a non-array', function () {
                var obj = {arr: 'hello'},
                    modified;

                assert.throws(function () {
                    modified = model.modify(obj, {$pull: {arr: 'world'}});
                });
            });

            test('Use deep-equality to check whether we can remove a value from a set', function () {
                var obj = {arr: [{b: 2}, {b: 3}]},
                    modified;

                modified = model.modify(obj, {$pull: {arr: {b: 3}}});
                assert.deepEqual(modified, {arr: [{b: 2}]});

                obj = {arr: [{b: 2}]};
                modified = model.modify(obj, {$pull: {arr: {b: 3}}});
                assert.deepEqual(modified, {arr: [{b: 2}]});
            });

            test('Can use any kind of nedb query with $pull', function () {
                var obj = {arr: [4, 7, 12, 2], other: 'yup'},
                    modified;

                modified = model.modify(obj, {$pull: {arr: {$gte: 5}}});
                assert.deepEqual(modified, {arr: [4, 2], other: 'yup'});

                obj = {arr: [{b: 4}, {b: 7}, {b: 1}], other: 'yeah'};
                modified = model.modify(obj, {$pull: {arr: {b: {$gte: 5}}}});
                assert.deepEqual(modified, {arr: [{b: 4}, {b: 1}], other: 'yeah'});
            });
        }); // End of '$pull modifier'

        describe('$max modifier', function () {
            test('Will set the field to the updated value if value is greater than current one, without modifying the original object', function () {
                var obj = {some: 'thing', number: 10},
                    updateQuery = {$max: {number: 12}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {some: 'thing', number: 12});
                assert.deepEqual(obj, {some: 'thing', number: 10});
            });

            test('Will not update the field if new value is smaller than current one', function () {
                var obj = {some: 'thing', number: 10},
                    updateQuery = {$max: {number: 9}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {some: 'thing', number: 10});
            });

            test('Will create the field if it does not exist', function () {
                var obj = {some: 'thing'},
                    updateQuery = {$max: {number: 10}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {some: 'thing', number: 10});
            });

            test('Works on embedded documents', function () {
                var obj = {some: 'thing', somethingElse: {number: 10}},
                    updateQuery = {$max: {'somethingElse.number': 12}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {some: 'thing', somethingElse: {number: 12}});
            });
        }); // End of '$max modifier'

        describe('$min modifier', function () {
            test('Will set the field to the updated value if value is smaller than current one, without modifying the original object', function () {
                var obj = {some: 'thing', number: 10},
                    updateQuery = {$min: {number: 8}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {some: 'thing', number: 8});
                assert.deepEqual(obj, {some: 'thing', number: 10});
            });

            test('Will not update the field if new value is greater than current one', function () {
                var obj = {some: 'thing', number: 10},
                    updateQuery = {$min: {number: 12}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {some: 'thing', number: 10});
            });

            test('Will create the field if it does not exist', function () {
                var obj = {some: 'thing'},
                    updateQuery = {$min: {number: 10}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {some: 'thing', number: 10});
            });

            test('Works on embedded documents', function () {
                var obj = {some: 'thing', somethingElse: {number: 10}},
                    updateQuery = {$min: {'somethingElse.number': 8}},
                    modified = model.modify(obj, updateQuery);

                assert.deepEqual(modified, {some: 'thing', somethingElse: {number: 8}});
            });
        }); // End of '$min modifier'
    }); // ==== End of 'Modifying documents' ==== //

    describe('Comparing things', function () {
        test('undefined is the smallest', function () {
            var otherStuff = [
                null,
                'string',
                '',
                -1,
                0,
                5.3,
                12,
                true,
                false,
                new Date(12345),
                {},
                {hello: 'world'},
                [],
                ['quite', 5],
            ];

            assert.equal(model.compareThings(undefined, undefined), 0);

            otherStuff.forEach(function (stuff) {
                assert.equal(model.compareThings(undefined, stuff), -1);
                assert.equal(model.compareThings(stuff, undefined), 1);
            });
        });

        test('Then null', function () {
            var otherStuff = [
                'string',
                '',
                -1,
                0,
                5.3,
                12,
                true,
                false,
                new Date(12345),
                {},
                {hello: 'world'},
                [],
                ['quite', 5],
            ];

            assert.equal(model.compareThings(null, null), 0);

            otherStuff.forEach(function (stuff) {
                assert.equal(model.compareThings(null, stuff), -1);
                assert.equal(model.compareThings(stuff, null), 1);
            });
        });

        test('Then numbers', function () {
            var otherStuff = ['string', '', true, false, new Date(4312), {}, {hello: 'world'}, [], ['quite', 5]],
                numbers = [-12, 0, 12, 5.7];

            assert.equal(model.compareThings(-12, 0), -1);
            assert.equal(model.compareThings(0, -3), 1);
            assert.equal(model.compareThings(5.7, 2), 1);
            assert.equal(model.compareThings(5.7, 12.3), -1);
            assert.equal(model.compareThings(0, 0), 0);
            assert.equal(model.compareThings(-2.6, -2.6), 0);
            assert.equal(model.compareThings(5, 5), 0);

            otherStuff.forEach(function (stuff) {
                numbers.forEach(function (number) {
                    assert.equal(model.compareThings(number, stuff), -1);
                    assert.equal(model.compareThings(stuff, number), 1);
                });
            });
        });

        test('Then strings', function () {
            var otherStuff = [true, false, new Date(4321), {}, {hello: 'world'}, [], ['quite', 5]],
                strings = ['', 'string', 'hello world'];

            assert.equal(model.compareThings('', 'hey'), -1);
            assert.equal(model.compareThings('hey', ''), 1);
            assert.equal(model.compareThings('hey', 'hew'), 1);
            assert.equal(model.compareThings('hey', 'hey'), 0);

            otherStuff.forEach(function (stuff) {
                strings.forEach(function (string) {
                    assert.equal(model.compareThings(string, stuff), -1);
                    assert.equal(model.compareThings(stuff, string), 1);
                });
            });
        });

        test('Then booleans', function () {
            var otherStuff = [new Date(4321), {}, {hello: 'world'}, [], ['quite', 5]],
                bools = [true, false];

            assert.equal(model.compareThings(true, true), 0);
            assert.equal(model.compareThings(false, false), 0);
            assert.equal(model.compareThings(true, false), 1);
            assert.equal(model.compareThings(false, true), -1);

            otherStuff.forEach(function (stuff) {
                bools.forEach(function (bool) {
                    assert.equal(model.compareThings(bool, stuff), -1);
                    assert.equal(model.compareThings(stuff, bool), 1);
                });
            });
        });

        test('Then dates', function () {
            var otherStuff = [{}, {hello: 'world'}, [], ['quite', 5]],
                dates = [new Date(-123), new Date(), new Date(5555), new Date(0)],
                now = new Date();

            assert.equal(model.compareThings(now, now), 0);
            assert.equal(model.compareThings(new Date(54341), now), -1);
            assert.equal(model.compareThings(now, new Date(54341)), 1);
            assert.equal(model.compareThings(new Date(0), new Date(-54341)), 1);
            assert.equal(model.compareThings(new Date(123), new Date(4341)), -1);

            otherStuff.forEach(function (stuff) {
                dates.forEach(function (date) {
                    assert.equal(model.compareThings(date, stuff), -1);
                    assert.equal(model.compareThings(stuff, date), 1);
                });
            });
        });

        test('Then arrays', function () {
            var otherStuff = [{}, {hello: 'world'}],
                arrays = [[], ['yes'], ['hello', 5]];
            assert.equal(model.compareThings([], []), 0);
            assert.equal(model.compareThings(['hello'], []), 1);
            assert.equal(model.compareThings([], ['hello']), -1);
            assert.equal(model.compareThings(['hello'], ['hello', 'world']), -1);
            assert.equal(model.compareThings(['hello', 'earth'], ['hello', 'world']), -1);
            assert.equal(model.compareThings(['hello', 'zzz'], ['hello', 'world']), 1);
            assert.equal(model.compareThings(['hello', 'world'], ['hello', 'world']), 0);

            otherStuff.forEach(function (stuff) {
                arrays.forEach(function (array) {
                    assert.equal(model.compareThings(array, stuff), -1);
                    assert.equal(model.compareThings(stuff, array), 1);
                });
            });
        });

        test('And finally objects', function () {
            assert.equal(model.compareThings({}, {}), 0);
            assert.equal(model.compareThings({a: 42}, {a: 312}), -1);
            assert.equal(model.compareThings({a: '42'}, {a: '312'}), 1);
            assert.equal(model.compareThings({a: 42, b: 312}, {b: 312, a: 42}), 0);
            assert.equal(model.compareThings({a: 42, b: 312, c: 54}, {b: 313, a: 42}), -1);
        });

        test('Can specify custom string comparison function', function () {
            assert.equal(
                model.compareThings('hello', 'bloup', function (a, b) {
                    return a < b ? -1 : 1;
                }),
                1
            );
            assert.equal(
                model.compareThings('hello', 'bloup', function (a, b) {
                    return a > b ? -1 : 1;
                }),
                -1
            );
        });
    }); // ==== End of 'Comparing things' ==== //

    describe('Querying', function () {
        describe('Comparing things', function () {
            test('Two things of different types cannot be equal, two identical native things are equal', function () {
                var toTest = [null, 'somestring', 42, true, new Date(72998322), {hello: 'world'}],
                    toTestAgainst = [null, 'somestring', 42, true, new Date(72998322), {hello: 'world'}], // Use another array so that we don't test pointer equality
                    i,
                    j;

                for (i = 0; i < toTest.length; i += 1) {
                    for (j = 0; j < toTestAgainst.length; j += 1) {
                        assert.equal(model.areThingsEqual(toTest[i], toTestAgainst[j]), i === j);
                    }
                }
            });

            test('Can test native types null undefined string number boolean date equality', function () {
                var toTest = [null, undefined, 'somestring', 42, true, new Date(72998322), {hello: 'world'}],
                    toTestAgainst = [undefined, null, 'someotherstring', 5, false, new Date(111111), {hello: 'mars'}],
                    i;

                for (i = 0; i < toTest.length; i += 1) {
                    assert.equal(model.areThingsEqual(toTest[i], toTestAgainst[i]), false);
                }
            });

            test('If one side is an array or undefined, comparison fails', function () {
                var toTestAgainst = [null, undefined, 'somestring', 42, true, new Date(72998322), {hello: 'world'}],
                    i;

                for (i = 0; i < toTestAgainst.length; i += 1) {
                    assert.equal(model.areThingsEqual([1, 2, 3], toTestAgainst[i]), false);
                    assert.equal(model.areThingsEqual(toTestAgainst[i], []), false);

                    assert.equal(model.areThingsEqual(undefined, toTestAgainst[i]), false);
                    assert.equal(model.areThingsEqual(toTestAgainst[i], undefined), false);
                }
            });

            test('Can test objects equality', function () {
                assert.equal(model.areThingsEqual({hello: 'world'}, {}), false);
                assert.equal(model.areThingsEqual({hello: 'world'}, {hello: 'mars'}), false);
                assert.equal(model.areThingsEqual({hello: 'world'}, {hello: 'world', temperature: 42}), false);
                assert.equal(
                    model.areThingsEqual(
                        {hello: 'world', other: {temperature: 42}},
                        {hello: 'world', other: {temperature: 42}}
                    ),
                    true
                );
            });
        });

        describe('Getting a fields value in dot notation', function () {
            test('Return first-level and nested values', function () {
                assert.equal(model.getDotValue({hello: 'world'}, 'hello'), 'world');
                assert.equal(
                    model.getDotValue({hello: 'world', type: {planet: true, blue: true}}, 'type.planet'),
                    true
                );
            });

            test('Return undefined if the field cannot be found in the object', function () {
                assert.equal(model.getDotValue({hello: 'world'}, 'helloo'), undefined);
                assert.equal(model.getDotValue({hello: 'world', type: {planet: true}}, 'type.plane'), undefined);
            });

            test('Can navigate inside arrays with dot notation, and return the array of values in that case', function () {
                var dv;

                // Simple array of subdocuments
                dv = model.getDotValue(
                    {
                        planets: [
                            {name: 'Earth', number: 3},
                            {name: 'Mars', number: 2},
                            {name: 'Pluton', number: 9},
                        ],
                    },
                    'planets.name'
                );
                assert.deepEqual(dv, ['Earth', 'Mars', 'Pluton']);

                // Nested array of subdocuments
                dv = model.getDotValue(
                    {
                        nedb: true,
                        data: {
                            planets: [
                                {name: 'Earth', number: 3},
                                {name: 'Mars', number: 2},
                                {name: 'Pluton', number: 9},
                            ],
                        },
                    },
                    'data.planets.number'
                );
                assert.deepEqual(dv, [3, 2, 9]);

                // Nested array in a subdocument of an array (yay, inception!)
                // TODO: make sure MongoDB doesn't flatten the array (it wouldn't make sense)
                dv = model.getDotValue(
                    {
                        nedb: true,
                        data: {
                            planets: [
                                {name: 'Earth', numbers: [1, 3]},
                                {name: 'Mars', numbers: [7]},
                                {name: 'Pluton', numbers: [9, 5, 1]},
                            ],
                        },
                    },
                    'data.planets.numbers'
                );
                assert.deepEqual(dv, [[1, 3], [7], [9, 5, 1]]);
            });

            test('Can get a single value out of an array using its index', function () {
                var dv;

                // Simple index in dot notation
                dv = model.getDotValue(
                    {
                        planets: [
                            {name: 'Earth', number: 3},
                            {name: 'Mars', number: 2},
                            {name: 'Pluton', number: 9},
                        ],
                    },
                    'planets.1'
                );
                assert.deepEqual(dv, {name: 'Mars', number: 2});

                // Out of bounds index
                dv = model.getDotValue(
                    {
                        planets: [
                            {name: 'Earth', number: 3},
                            {name: 'Mars', number: 2},
                            {name: 'Pluton', number: 9},
                        ],
                    },
                    'planets.3'
                );
                assert.equal(dv, undefined);

                // Index in nested array
                dv = model.getDotValue(
                    {
                        nedb: true,
                        data: {
                            planets: [
                                {name: 'Earth', number: 3},
                                {name: 'Mars', number: 2},
                                {name: 'Pluton', number: 9},
                            ],
                        },
                    },
                    'data.planets.2'
                );
                assert.deepEqual(dv, {name: 'Pluton', number: 9});

                // Dot notation with index in the middle
                dv = model.getDotValue(
                    {
                        nedb: true,
                        data: {
                            planets: [
                                {name: 'Earth', number: 3},
                                {name: 'Mars', number: 2},
                                {name: 'Pluton', number: 9},
                            ],
                        },
                    },
                    'data.planets.0.name'
                );
                assert.equal(dv, 'Earth');
            });
        });

        describe('Field equality', function () {
            test('Can find documents with simple fields', function () {
                assert.equal(model.match({test: 'yeah'}, {test: 'yea'}), false);
                assert.equal(model.match({test: 'yeah'}, {test: 'yeahh'}), false);
                assert.equal(model.match({test: 'yeah'}, {test: 'yeah'}), true);
            });

            test('Can find documents with the dot-notation', function () {
                assert.equal(model.match({test: {ooo: 'yeah'}}, {'test.ooo': 'yea'}), false);
                assert.equal(model.match({test: {ooo: 'yeah'}}, {'test.oo': 'yeah'}), false);
                assert.equal(model.match({test: {ooo: 'yeah'}}, {'tst.ooo': 'yeah'}), false);
                assert.equal(model.match({test: {ooo: 'yeah'}}, {'test.ooo': 'yeah'}), true);
            });

            test('Cannot find undefined', function () {
                assert.equal(model.match({test: undefined}, {test: undefined}), false);
                assert.equal(model.match({test: {pp: undefined}}, {'test.pp': undefined}), false);
            });

            test('Nested objects are deep-equality matched and not treated as sub-queries', function () {
                assert.equal(model.match({a: {b: 5}}, {a: {b: 5}}), true);
                assert.equal(model.match({a: {b: 5, c: 3}}, {a: {b: 5}}), false);

                assert.equal(model.match({a: {b: 5}}, {a: {b: {$lt: 10}}}), false);
                assert.throws(function () {
                    model.match({a: {b: 5}}, {a: {$or: [{b: 10}, {b: 5}]}});
                });
            });

            test('Can match for field equality inside an array with the dot notation', function () {
                assert.equal(model.match({a: true, b: ['node', 'embedded', 'database']}, {'b.1': 'node'}), false);
                assert.equal(model.match({a: true, b: ['node', 'embedded', 'database']}, {'b.1': 'embedded'}), true);
                assert.equal(model.match({a: true, b: ['node', 'embedded', 'database']}, {'b.1': 'database'}), false);
            });
        });

        describe('Regular expression matching', function () {
            test('Matching a non-string to a regular expression always yields false', function () {
                var d = new Date(),
                    r = new RegExp(d.getTime());

                assert.equal(model.match({test: true}, {test: /true/}), false);
                assert.equal(model.match({test: null}, {test: /null/}), false);
                assert.equal(model.match({test: 42}, {test: /42/}), false);
                assert.equal(model.match({test: d}, {test: r}), false);
            });

            test('Can match strings using basic querying', function () {
                assert.equal(model.match({test: 'true'}, {test: /true/}), true);
                assert.equal(model.match({test: 'babaaaar'}, {test: /aba+r/}), true);
                assert.equal(model.match({test: 'babaaaar'}, {test: /^aba+r/}), false);
                assert.equal(model.match({test: 'true'}, {test: /t[ru]e/}), false);
            });

            test('Can match strings using the $regex operator', function () {
                assert.equal(model.match({test: 'true'}, {test: {$regex: /true/}}), true);
                assert.equal(model.match({test: 'babaaaar'}, {test: {$regex: /aba+r/}}), true);
                assert.equal(model.match({test: 'babaaaar'}, {test: {$regex: /^aba+r/}}), false);
                assert.equal(model.match({test: 'true'}, {test: {$regex: /t[ru]e/}}), false);
            });

            test('Will throw if $regex operator is used with a non regex value', function () {
                assert.throws(function () {
                    model.match({test: 'true'}, {test: {$regex: 42}});
                });

                assert.throws(function () {
                    model.match({test: 'true'}, {test: {$regex: 'true'}});
                });
            });

            test('Can use the $regex operator in cunjunction with other operators', function () {
                assert.equal(model.match({test: 'helLo'}, {test: {$regex: /ll/i, $nin: ['helL', 'helLop']}}), true);
                assert.equal(model.match({test: 'helLo'}, {test: {$regex: /ll/i, $nin: ['helLo', 'helLop']}}), false);
            });

            test('Can use dot-notation', function () {
                assert.equal(model.match({test: {nested: 'true'}}, {'test.nested': /true/}), true);
                assert.equal(model.match({test: {nested: 'babaaaar'}}, {'test.nested': /^aba+r/}), false);

                assert.equal(model.match({test: {nested: 'true'}}, {'test.nested': {$regex: /true/}}), true);
                assert.equal(model.match({test: {nested: 'babaaaar'}}, {'test.nested': {$regex: /^aba+r/}}), false);
            });
        });

        describe('$lt', function () {
            test('Cannot compare a field to an object, an array, null or a boolean, it will return false', function () {
                assert.equal(model.match({a: 5}, {a: {$lt: {a: 6}}}), false);
                assert.equal(model.match({a: 5}, {a: {$lt: [6, 7]}}), false);
                assert.equal(model.match({a: 5}, {a: {$lt: null}}), false);
                assert.equal(model.match({a: 5}, {a: {$lt: true}}), false);
            });

            test('Can compare numbers, with or without dot notation', function () {
                assert.equal(model.match({a: 5}, {a: {$lt: 6}}), true);
                assert.equal(model.match({a: 5}, {a: {$lt: 5}}), false);
                assert.equal(model.match({a: 5}, {a: {$lt: 4}}), false);

                assert.equal(model.match({a: {b: 5}}, {'a.b': {$lt: 6}}), true);
                assert.equal(model.match({a: {b: 5}}, {'a.b': {$lt: 3}}), false);
            });

            test('Can compare strings, with or without dot notation', function () {
                assert.equal(model.match({a: 'nedb'}, {a: {$lt: 'nedc'}}), true);
                assert.equal(model.match({a: 'nedb'}, {a: {$lt: 'neda'}}), false);

                assert.equal(model.match({a: {b: 'nedb'}}, {'a.b': {$lt: 'nedc'}}), true);
                assert.equal(model.match({a: {b: 'nedb'}}, {'a.b': {$lt: 'neda'}}), false);
            });

            test('If field is an array field, a match means a match on at least one element', function () {
                assert.equal(model.match({a: [5, 10]}, {a: {$lt: 4}}), false);
                assert.equal(model.match({a: [5, 10]}, {a: {$lt: 6}}), true);
                assert.equal(model.match({a: [5, 10]}, {a: {$lt: 11}}), true);
            });

            test('Works with dates too', function () {
                assert.equal(model.match({a: new Date(1000)}, {a: {$gte: new Date(1001)}}), false);
                assert.equal(model.match({a: new Date(1000)}, {a: {$lt: new Date(1001)}}), true);
            });
        });

        // General behaviour is tested in the block about $lt. Here we just test operators work
        describe('Other comparison operators: $lte, $gt, $gte, $ne, $in, $exists', function () {
            test('$lte', function () {
                assert.equal(model.match({a: 5}, {a: {$lte: 6}}), true);
                assert.equal(model.match({a: 5}, {a: {$lte: 5}}), true);
                assert.equal(model.match({a: 5}, {a: {$lte: 4}}), false);
            });

            test('$gt', function () {
                assert.equal(model.match({a: 5}, {a: {$gt: 6}}), false);
                assert.equal(model.match({a: 5}, {a: {$gt: 5}}), false);
                assert.equal(model.match({a: 5}, {a: {$gt: 4}}), true);
            });

            test('$gte', function () {
                assert.equal(model.match({a: 5}, {a: {$gte: 6}}), false);
                assert.equal(model.match({a: 5}, {a: {$gte: 5}}), true);
                assert.equal(model.match({a: 5}, {a: {$gte: 4}}), true);
            });

            test('$ne', function () {
                assert.equal(model.match({a: 5}, {a: {$ne: 4}}), true);
                assert.equal(model.match({a: 5}, {a: {$ne: 5}}), false);
                assert.equal(model.match({a: 5}, {b: {$ne: 5}}), true);
                assert.equal(model.match({a: false}, {a: {$ne: false}}), false);
            });

            test('$in', function () {
                assert.equal(model.match({a: 5}, {a: {$in: [6, 8, 9]}}), false);
                assert.equal(model.match({a: 6}, {a: {$in: [6, 8, 9]}}), true);
                assert.equal(model.match({a: 7}, {a: {$in: [6, 8, 9]}}), false);
                assert.equal(model.match({a: 8}, {a: {$in: [6, 8, 9]}}), true);
                assert.equal(model.match({a: 9}, {a: {$in: [6, 8, 9]}}), true);

                assert.throws(function () {
                    model.match({a: 5}, {a: {$in: 5}});
                });
            });

            test('$nin', function () {
                assert.equal(model.match({a: 5}, {a: {$nin: [6, 8, 9]}}), true);
                assert.equal(model.match({a: 6}, {a: {$nin: [6, 8, 9]}}), false);
                assert.equal(model.match({a: 7}, {a: {$nin: [6, 8, 9]}}), true);
                assert.equal(model.match({a: 8}, {a: {$nin: [6, 8, 9]}}), false);
                assert.equal(model.match({a: 9}, {a: {$nin: [6, 8, 9]}}), false);

                // Matches if field doesn't exist
                assert.equal(model.match({a: 9}, {b: {$nin: [6, 8, 9]}}), true);

                assert.throws(function () {
                    model.match({a: 5}, {a: {$in: 5}});
                });
            });

            test('$exists', function () {
                assert.equal(model.match({a: 5}, {a: {$exists: 1}}), true);
                assert.equal(model.match({a: 5}, {a: {$exists: true}}), true);
                assert.equal(model.match({a: 5}, {a: {$exists: new Date()}}), true);
                assert.equal(model.match({a: 5}, {a: {$exists: ''}}), true);
                assert.equal(model.match({a: 5}, {a: {$exists: []}}), true);
                assert.equal(model.match({a: 5}, {a: {$exists: {}}}), true);

                assert.equal(model.match({a: 5}, {a: {$exists: 0}}), false);
                assert.equal(model.match({a: 5}, {a: {$exists: false}}), false);
                assert.equal(model.match({a: 5}, {a: {$exists: null}}), false);
                assert.equal(model.match({a: 5}, {a: {$exists: undefined}}), false);

                assert.equal(model.match({a: 5}, {b: {$exists: true}}), false);

                assert.equal(model.match({a: 5}, {b: {$exists: false}}), true);
            });
        });

        describe('Comparing on arrays', function () {
            test('Can perform a direct array match', function () {
                assert.equal(
                    model.match({planets: ['Earth', 'Mars', 'Pluto'], something: 'else'}, {planets: ['Earth', 'Mars']}),
                    false
                );
                assert.equal(
                    model.match(
                        {planets: ['Earth', 'Mars', 'Pluto'], something: 'else'},
                        {planets: ['Earth', 'Mars', 'Pluto']}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {planets: ['Earth', 'Mars', 'Pluto'], something: 'else'},
                        {planets: ['Earth', 'Pluto', 'Mars']}
                    ),
                    false
                );
            });

            test('Can query on the size of an array field', function () {
                // Non nested documents
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$size: 0}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$size: 1}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$size: 2}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$size: 3}}
                    ),
                    true
                );

                // Nested documents
                assert.equal(
                    model.match(
                        {hello: 'world', description: {satellites: ['Moon', 'Hubble'], diameter: 6300}},
                        {'description.satellites': {$size: 0}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {hello: 'world', description: {satellites: ['Moon', 'Hubble'], diameter: 6300}},
                        {'description.satellites': {$size: 1}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {hello: 'world', description: {satellites: ['Moon', 'Hubble'], diameter: 6300}},
                        {'description.satellites': {$size: 2}}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {hello: 'world', description: {satellites: ['Moon', 'Hubble'], diameter: 6300}},
                        {'description.satellites': {$size: 3}}
                    ),
                    false
                );

                // Using a projected array
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.names': {$size: 0}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.names': {$size: 1}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.names': {$size: 2}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.names': {$size: 3}}
                    ),
                    true
                );
            });

            test('$size operator works with empty arrays', function () {
                assert.equal(model.match({childrens: []}, {'childrens': {$size: 0}}), true);
                assert.equal(model.match({childrens: []}, {'childrens': {$size: 2}}), false);
                assert.equal(model.match({childrens: []}, {'childrens': {$size: 3}}), false);
            });

            test('Should throw an error if a query operator is used without comparing to an integer', function () {
                assert.throws(function () {
                    model.match({a: [1, 5]}, {a: {$size: 1.4}});
                });
                assert.throws(function () {
                    model.match({a: [1, 5]}, {a: {$size: 'fdf'}});
                });
                assert.throws(function () {
                    model.match({a: [1, 5]}, {a: {$size: {$lt: 5}}});
                });
            });

            test('Using $size operator on a non-array field should prevent match but not throw', function () {
                assert.equal(model.match({a: 5}, {a: {$size: 1}}), false);
            });

            test('Can use $size several times in the same matcher', function () {
                assert.equal(
                    model.match({childrens: ['Riri', 'Fifi', 'Loulou']}, {'childrens': {$size: 3, $size: 3}}),
                    true
                );
                assert.equal(
                    model.match({childrens: ['Riri', 'Fifi', 'Loulou']}, {'childrens': {$size: 3, $size: 4}}),
                    false
                ); // Of course this can never be true
            });

            test('Can query array documents with multiple simultaneous conditions', function () {
                // Non nested documents
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$elemMatch: {name: 'Dewey', age: 7}}}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$elemMatch: {name: 'Dewey', age: 12}}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$elemMatch: {name: 'Louie', age: 3}}}
                    ),
                    false
                );

                // Nested documents
                assert.equal(
                    model.match(
                        {
                            outer: {
                                childrens: [
                                    {name: 'Huey', age: 3},
                                    {name: 'Dewey', age: 7},
                                    {name: 'Louie', age: 12},
                                ],
                            },
                        },
                        {'outer.childrens': {$elemMatch: {name: 'Dewey', age: 7}}}
                    ),
                    true
                );

                assert.equal(
                    model.match(
                        {
                            outer: {
                                childrens: [
                                    {name: 'Huey', age: 3},
                                    {name: 'Dewey', age: 7},
                                    {name: 'Louie', age: 12},
                                ],
                            },
                        },
                        {'outer.childrens': {$elemMatch: {name: 'Dewey', age: 12}}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            outer: {
                                childrens: [
                                    {name: 'Huey', age: 3},
                                    {name: 'Dewey', age: 7},
                                    {name: 'Louie', age: 12},
                                ],
                            },
                        },
                        {'outer.childrens': {$elemMatch: {name: 'Louie', age: 3}}}
                    ),
                    false
                );
            });

            test('$elemMatch operator works with empty arrays', function () {
                assert.equal(model.match({childrens: []}, {'childrens': {$elemMatch: {name: 'Mitsos'}}}), false);
                assert.equal(model.match({childrens: []}, {'childrens': {$elemMatch: {}}}), false);
            });

            test('Can use more complex comparisons inside nested query documents', function () {
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$elemMatch: {name: 'Dewey', age: {$gt: 6, $lt: 8}}}}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$elemMatch: {name: 'Dewey', age: {$in: [6, 7, 8]}}}}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$elemMatch: {name: 'Dewey', age: {$gt: 6, $lt: 7}}}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens': {$elemMatch: {name: 'Louie', age: {$gt: 6, $lte: 7}}}}
                    ),
                    false
                );
            });
        });

        describe('Logical operators $or, $and, $not', function () {
            test('Any of the subqueries should match for an $or to match', function () {
                assert.equal(model.match({hello: 'world'}, {$or: [{hello: 'pluton'}, {hello: 'world'}]}), true);
                assert.equal(model.match({hello: 'pluton'}, {$or: [{hello: 'pluton'}, {hello: 'world'}]}), true);
                assert.equal(model.match({hello: 'nope'}, {$or: [{hello: 'pluton'}, {hello: 'world'}]}), false);
                assert.equal(
                    model.match({hello: 'world', age: 15}, {$or: [{hello: 'pluton'}, {age: {$lt: 20}}]}),
                    true
                );
                assert.equal(
                    model.match({hello: 'world', age: 15}, {$or: [{hello: 'pluton'}, {age: {$lt: 10}}]}),
                    false
                );
            });

            test('All of the subqueries should match for an $and to match', function () {
                assert.equal(model.match({hello: 'world', age: 15}, {$and: [{age: 15}, {hello: 'world'}]}), true);
                assert.equal(model.match({hello: 'world', age: 15}, {$and: [{age: 16}, {hello: 'world'}]}), false);
                assert.equal(
                    model.match({hello: 'world', age: 15}, {$and: [{hello: 'world'}, {age: {$lt: 20}}]}),
                    true
                );
                assert.equal(
                    model.match({hello: 'world', age: 15}, {$and: [{hello: 'pluton'}, {age: {$lt: 20}}]}),
                    false
                );
            });

            test('Subquery should not match for a $not to match', function () {
                assert.equal(model.match({a: 5, b: 10}, {a: 5}), true);
                assert.equal(model.match({a: 5, b: 10}, {$not: {a: 5}}), false);
            });

            test('Logical operators are all top-level, only other logical operators can be above', function () {
                assert.throws(function () {
                    model.match({a: {b: 7}}, {a: {$or: [{b: 5}, {b: 7}]}});
                });
                assert.equal(model.match({a: {b: 7}}, {$or: [{'a.b': 5}, {'a.b': 7}]}), true);
            });

            test('Logical operators can be combined as long as they are on top of the decision tree', function () {
                assert.equal(
                    model.match(
                        {a: 5, b: 7, c: 12},
                        {$or: [{$and: [{a: 5}, {b: 8}]}, {$and: [{a: 5}, {c: {$lt: 40}}]}]}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {a: 5, b: 7, c: 12},
                        {$or: [{$and: [{a: 5}, {b: 8}]}, {$and: [{a: 5}, {c: {$lt: 10}}]}]}
                    ),
                    false
                );
            });

            test('Should throw an error if a logical operator is used without an array or if an unknown logical operator is used', function () {
                assert.throws(function () {
                    model.match({a: 5}, {$or: {a: 5, a: 6}});
                });
                assert.throws(function () {
                    model.match({a: 5}, {$and: {a: 5, a: 6}});
                });
                assert.throws(function () {
                    model.match({a: 5}, {$unknown: [{a: 5}]});
                });
            });
        });

        describe('Comparison operator $where', function () {
            test('Function should match and not match correctly', function () {
                assert.equal(
                    model.match(
                        {a: 4},
                        {
                            $where: function () {
                                return this.a === 4;
                            },
                        }
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {a: 4},
                        {
                            $where: function () {
                                return this.a === 5;
                            },
                        }
                    ),
                    false
                );
            });

            test('Should throw an error if the $where function is not, in fact, a function', function () {
                assert.throws(function () {
                    model.match({a: 4}, {$where: 'not a function'});
                });
            });

            test('Should throw an error if the $where function returns a non-boolean', function () {
                assert.throws(function () {
                    model.match(
                        {a: 4},
                        {
                            $where: function () {
                                return 'not a boolean';
                            },
                        }
                    );
                });
            });

            test('Should be able to do the complex matching it must be used for', function () {
                var checkEmail = function () {
                    if (!this.firstName || !this.lastName) {
                        return false;
                    }
                    return (
                        this.firstName.toLowerCase() + '.' + this.lastName.toLowerCase() + '@gmail.com' === this.email
                    );
                };
                assert.equal(
                    model.match(
                        {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                        {$where: checkEmail}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {firstName: 'john', lastName: 'doe', email: 'john.doe@gmail.com'},
                        {$where: checkEmail}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {firstName: 'Jane', lastName: 'Doe', email: 'john.doe@gmail.com'},
                        {$where: checkEmail}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {firstName: 'John', lastName: 'Deere', email: 'john.doe@gmail.com'},
                        {$where: checkEmail}
                    ),
                    false
                );
                assert.equal(model.match({lastName: 'Doe', email: 'john.doe@gmail.com'}, {$where: checkEmail}), false);
            });
        });

        describe('Array fields', function () {
            test('Field equality', function () {
                assert.equal(model.match({tags: ['node', 'js', 'db']}, {tags: 'python'}), false);
                assert.equal(model.match({tags: ['node', 'js', 'db']}, {tagss: 'js'}), false);
                assert.equal(model.match({tags: ['node', 'js', 'db']}, {tags: 'js'}), true);
                assert.equal(model.match({tags: ['node', 'js', 'db']}, {tags: 'js', tags: 'node'}), true);

                // Mixed matching with array and non array
                assert.equal(model.match({tags: ['node', 'js', 'db'], nedb: true}, {tags: 'js', nedb: true}), true);

                // Nested matching
                assert.equal(model.match({number: 5, data: {tags: ['node', 'js', 'db']}}, {'data.tags': 'js'}), true);
                assert.equal(model.match({number: 5, data: {tags: ['node', 'js', 'db']}}, {'data.tags': 'j'}), false);
            });

            test('With one comparison operator', function () {
                assert.equal(model.match({ages: [3, 7, 12]}, {ages: {$lt: 2}}), false);
                assert.equal(model.match({ages: [3, 7, 12]}, {ages: {$lt: 3}}), false);
                assert.equal(model.match({ages: [3, 7, 12]}, {ages: {$lt: 4}}), true);
                assert.equal(model.match({ages: [3, 7, 12]}, {ages: {$lt: 8}}), true);
                assert.equal(model.match({ages: [3, 7, 12]}, {ages: {$lt: 13}}), true);
            });

            test('Works with arrays that are in subdocuments', function () {
                assert.equal(model.match({children: {ages: [3, 7, 12]}}, {'children.ages': {$lt: 2}}), false);
                assert.equal(model.match({children: {ages: [3, 7, 12]}}, {'children.ages': {$lt: 3}}), false);
                assert.equal(model.match({children: {ages: [3, 7, 12]}}, {'children.ages': {$lt: 4}}), true);
                assert.equal(model.match({children: {ages: [3, 7, 12]}}, {'children.ages': {$lt: 8}}), true);
                assert.equal(model.match({children: {ages: [3, 7, 12]}}, {'children.ages': {$lt: 13}}), true);
            });

            test('Can query inside arrays thanks to dot notation', function () {
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.age': {$lt: 2}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.age': {$lt: 3}}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.age': {$lt: 4}}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.age': {$lt: 8}}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.age': {$lt: 13}}
                    ),
                    true
                );

                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.name': 'Louis'}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.name': 'Louie'}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.name': 'Lewi'}
                    ),
                    false
                );
            });

            test('Can query for a specific element inside arrays thanks to dot notation', function () {
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.0.name': 'Louie'}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.1.name': 'Louie'}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.2.name': 'Louie'}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {
                            childrens: [
                                {name: 'Huey', age: 3},
                                {name: 'Dewey', age: 7},
                                {name: 'Louie', age: 12},
                            ],
                        },
                        {'childrens.3.name': 'Louie'}
                    ),
                    false
                );
            });

            test('A single array-specific operator and the query is treated as array specific', function () {
                assert.throws(() => {
                    model.match({childrens: ['Riri', 'Fifi', 'Loulou']}, {'childrens': {'Fifi': true, $size: 3}});
                });
            });

            test('Can mix queries on array fields and non array filds with array specific operators', function () {
                assert.equal(
                    model.match(
                        {uncle: 'Donald', nephews: ['Riri', 'Fifi', 'Loulou']},
                        {nephews: {$size: 2}, uncle: 'Donald'}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {uncle: 'Donald', nephews: ['Riri', 'Fifi', 'Loulou']},
                        {nephews: {$size: 3}, uncle: 'Donald'}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {uncle: 'Donald', nephews: ['Riri', 'Fifi', 'Loulou']},
                        {nephews: {$size: 4}, uncle: 'Donald'}
                    ),
                    false
                );

                assert.equal(
                    model.match(
                        {uncle: 'Donals', nephews: ['Riri', 'Fifi', 'Loulou']},
                        {nephews: {$size: 3}, uncle: 'Picsou'}
                    ),
                    false
                );
                assert.equal(
                    model.match(
                        {uncle: 'Donald', nephews: ['Riri', 'Fifi', 'Loulou']},
                        {nephews: {$size: 3}, uncle: 'Donald'}
                    ),
                    true
                );
                assert.equal(
                    model.match(
                        {uncle: 'Donald', nephews: ['Riri', 'Fifi', 'Loulou']},
                        {nephews: {$size: 3}, uncle: 'Daisy'}
                    ),
                    false
                );
            });
        });
    }); // ==== End of 'Querying' ==== //
});
