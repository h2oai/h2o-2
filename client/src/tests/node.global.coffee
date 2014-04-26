require 'mocha'

_ = require 'lodash'

# Chai Assert API
# fail(actual, expected, message, operator)
# ok(value, [message])
# notOk(value, [message])
# equal(actual, expected, [message])
# notEqual(actual, expected, [message])
# deepEqual(actual, expected, [message])
# notDeepEqual(actual, expected, [message])
# strictEqual(actual, expected, [message])
# notStrictEqual(actual, expected, [message])
# throws(block, [error], [message])
# doesNotThrow(block, [message])
{ fail, ok, notOk, equal, notEqual, deepEqual, notDeepEqual, strictEqual, notStrictEqual, throws, doesNotThrow } = require('chai').assert


