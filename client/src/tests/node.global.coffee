#mocha = require 'mocha'
_ = require 'lodash'
chai = require 'chai'
httpRequest = require 'request'
clc = require 'cli-color'

#
# Chai Assert API
#
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
{ fail, ok, notOk, equal, notEqual, deepEqual, notDeepEqual, strictEqual, notStrictEqual, throws, doesNotThrow } = chai.assert

[ testStatus, describe, it ] = do ->
  testStatus = fail: 0, pass: 0, total: 0
  opts = bail: no, verbose: no

  path = []

  rethrow = (label, error) ->
    console.log clc.red "\n[FAIL] #{path.join ' - '} - #{label}"
    console.log clc.red "#{error.name}:#{error.message}"
    testStatus.fail++
    if opts.bail
      throw error
    else
      console.trace()
      console.log ''
    return

  describe = (label, test) ->
    path.push label
    test()
    path.pop()
    return

  it = (label, test) ->
    testStatus.total++
    try
      test (error) ->
        if error
          rethrow label, error
        return
      testStatus.pass++
      console.log clc.green "[pass] #{path.join ' - '} - #{label}" if opts.verbose
    catch error
      rethrow label, error
    return

  [ testStatus, describe, it ]
  
