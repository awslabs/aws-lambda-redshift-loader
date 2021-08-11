const assert = require('assert');
const expect = require('expect')
const prefixes = ["bucket/a=1/b=2/bob", "bucket/z=200/y=whatever/dude"];
const suppress = ["bucket/a=1/b=2/bob"];
require('../constants');
const common = require('../common')

describe('Common', function () {
    describe('Hive Prefix Tests', function () {
        it("Should parse string suppresion lists correctly", function () {
            process.env[SUPPRESS_WILDCARD_EXPANSION_PREFIXES] = "bucket/a=1/b=2/bob, bucket/z=200/y=whatever/dude";
            expect(common.getWildcardPrefixSuppressionList()).toEqual(prefixes);
        });

        it('Should suppress all wildcard transforms', function () {
            assert.strictEqual(prefixes[0].transformHiveStylePrefix(true), prefixes[0]);
        });

        it('Should transform prefix', function () {
            assert.strictEqual(prefixes[1].transformHiveStylePrefix(suppress), "bucket/z=*/y=*/dude");
        });

        it('Should not transform prefix', function () {
            assert.strictEqual(prefixes[0].transformHiveStylePrefix(suppress), prefixes[0]);
        });
    });
});