// @ts-nocheck

import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import {describe, test, beforeEach} from '@jest/globals';
// import _ from 'underscore';
import async from 'async';
import model from './../src/model';
import Datastore from '../src/datastore';
import Persistence from '../src/persistence';
import {customUtils} from '../src/customUtils';
import Cursor from '../src/cursor';

const testDb = 'workspace/test.db';
describe('customUtils', function () {
    describe('uid', function () {
        test('Generates a string of the expected length', function () {
            assert.equal(customUtils.uid(3).length, 3);
            assert.equal(customUtils.uid(16).length, 16);
            assert.equal(customUtils.uid(42).length, 42);
            assert.equal(customUtils.uid(1000).length, 1000);
        });

        // Very small probability of conflict
        test('Generated uids should not be the same', function () {
            assert.notEqual(customUtils.uid(56), customUtils.uid(56));
        });
    });
});
