describe 'Typecheck', ->

  # "Samples" of various types.
  s_undef = undefined
  s_null = null
  s_nan = Number.NaN
  s_0 = 0
  s_num = 42
  s_empty = ''
  s_str = 'foo'
  s_true = yes
  s_false = no
  s_arr = [1, 2, 3]
  s_func = -> yes
  s_err = new Error 'fail'
  s_date = new Date()
  s_regexp = /(.+)/g
  s_obj = {}

  s_all = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_nums = [ s_nan, s_0, s_num ]
  s_not_nums = [ s_undef, s_null, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_strs = [ s_empty, s_str ]
  s_not_strs = [ s_undef, s_null, s_nan, s_0, s_num, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_not_nums_or_strs = [ s_undef, s_null, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_bools = [ s_true, s_false ]
  s_not_bools = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_not_arrs = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_func, s_err, s_date, s_regexp, s_obj ]

  s_not_funcs = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_err, s_date, s_regexp, s_obj ]

  s_not_errs = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_date, s_regexp, s_obj ]

  s_not_dates = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_regexp, s_obj ]

  s_not_regexps = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_date, s_obj ]

  s_not_objs = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp ]

  checkValid = (values, type) ->
    for value in values
      ok (null is T.check value, type), "Failed positive #{JSON.stringify value} against type #{JSON.stringify type.inspect()}"
    return

  checkInvalid = (values, type) ->
    for value in values
      errors = T.check value, type
      # console.log errors
      ok (null isnt errors), "Failed negative #{JSON.stringify value} against type #{JSON.stringify type.inspect()}"
    return

  check = (validValues, invalidValues, type) ->
    checkValid validValues, type
    checkInvalid invalidValues, type


  it 'T.num', ->
    check s_nums, s_not_nums, T Foo: T.num
  it 'T.str', ->
    check s_strs, s_not_strs, T Foo: T.str
  it 'T.bool', ->
    check s_bools, s_not_bools, T Foo: T.bool
  it 'T.func', ->
    check [s_func], s_not_funcs, T Foo: T.func
  it 'T.err', ->
    check [s_err], s_not_errs, T Foo: T.err
  it 'T.date', ->
    check [s_date], s_not_dates, T Foo: T.date
  it 'T.regexp', ->
    check [s_regexp], s_not_regexps, T Foo: T.regexp

  it 'allows validation functions in simple type definitions', ->
    type = T foo: T.str (value) -> if value?.length is 5 then null else 'Invalid length'
    validValues = [ 'alpha', 'gamma' ]
    invalidValues = s_all
    check validValues, invalidValues, type

  it 'allows validation functions with simple type definitions', ->
    type = T foo: T.str, (value) -> if value?.length is 5 then null else 'Invalid length'
    validValues = [ 'alpha', 'gamma' ]
    invalidValues = s_all
    check validValues, invalidValues, type

  it 'allows validation functions on compound type definitions', ->
    type = T foo: { bar: T.str }, (value) -> if value?.bar?.length is 5 then null else 'Invalid length'
    validValues = [
      bar: 'alpha'
    ,
      bar: 'gamma'
    ]
    invalidValues = s_all
    check validValues, invalidValues, type

  it 'does not allow arbitrary items in type definitions', ->
    throws -> T foo: 'bar'
    throws -> T T.str foo: { bar: 'baz' }
    throws ->
      T foo:
        bar:
          baz: 'quux'

  it 'does not allow arbitrary items in type checks', ->
    throws -> T.check { foo: 'bar' }, 'foo'
    throws -> T.check { foo: 'bar' }, 42
    throws -> T.check { foo: 'bar' }, { foo: 'bar' }

  describe 'Any', ->
    it 'is allowed on top level definitions', ->
      check s_all, [], T Foo: T.any

    it 'is allowed on attribute definitions', ->
      validValues = [
        bar: s_num
      ,
        bar: s_arr
      ,
        bar: null
      ,
        bar: undefined
      ]

      invalidValues = [
        qux: s_num
      ]

      check validValues, invalidValues, T
        foo:
          bar: T.any

  describe 'Array', ->
    it 'allows primitive string typed arrays', ->
      type = T foo: T.array T.str
      validValues = [
        []
        [ 'foo', 'bar' ]
      ]
      invalidValues = s_all
      check validValues, invalidValues, type

    it 'allows primitive number typed arrays', ->
      type = T foo: T.array T.num
      validValues = [
        []
        [ 1, 2, 3, 4, 5 ]
      ]
      invalidValues = s_not_arrs
      check validValues, invalidValues, type

    it 'allows compound typed arrays', ->
      fooType = T
        Foo:
          foo: T.str
          bar: T.num

      arrayType = T foos: T.array fooType

      ok null is T.check [ { foo: 'foo', bar: 42 } ], arrayType

    it 'does not allow untyped arrays', ->
      throws -> T foo: T.array()
      throws -> T foo: T.array
      throws -> T foo: T.array identity

    it 'does not allow arbitrary arguments', ->
      throws -> T foo: T.array bar: 'baz'

    it 'allows validators', ->
      type = T foo: T.array T.num, (array) -> if array.length > 5 then 'Fail' else null
      ok null is T.check [ 1, 2, 3, 4, 5 ], type
      ok null isnt T.check [ 1, 2, 3, 4, 5, 6 ], type

  describe 'Tuples', ->
    it 'allows primitive types', ->
      type = T foo: T.tuple T.str, T.num, T.date
      validValues = [
        [ 'foo', 41, new Date() ]
        [ 'bar', 42, new Date() ]
        [ 'baz', 43, new Date() ]
      ]
      invalidValues = [
        [ new Date(), 'foo', 42 ] # wrong order
        [ new Date(), new Date(), new Date() ] # array
        [ 'foo', 'bar', 'baz' ] # array
        [ 41, 42, 43 ] # array
        [ 'foo', 42, new Date(), 'baz' ] # extra items
        [ 'foo', 42 ] # missing items
        [] # empty tuple
        { foo: 'bar' } # arbitrary value
      ]
      check validValues, invalidValues, type

    it 'allows compound types', ->
      fooType = T
        Foo:
          foo: T.str
          bar: T.num

      arrayType = T foos: T.tuple fooType

      ok null is T.check [ { foo: 'foo', bar: 42 } ], arrayType

    it 'does not allow untyped tuples', ->
      throws -> T foo: T.tuple()
      throws -> T foo: T.tuple
      throws -> T foo: T.tuple identity

    it 'does not allow arbitrary arguments', ->
      throws -> T foo: T.tuple bar: 'baz'

    it 'allows validators', ->
      type = T foo: T.tuple T.str, T.num, T.date, (tuple) -> if tuple[0].length > 3 then 'Fail' else null
      ok null is T.check [ 'foo', 42, new Date()  ], type
      ok null isnt T.check [ 'quux', 42, new Date() ], type

  describe 'Union', ->
    it 'allows single primitive types', ->
      type = T foo: T.union T.num
      validValues = s_nums
      invalidValues = s_not_nums
      check validValues, invalidValues, type

    it 'allows multiple primitive types', ->
      type = T foo: T.union T.str, T.num
      validValues = [ s_str, s_num ]
      invalidValues = s_not_nums_or_strs
      check validValues, invalidValues, type

    it 'allows compound types', ->
      fooType = T
        Foo:
          foo: T.str
          bar: T.num

      barType = T
        Bar:
          baz: T.str

      varType = T qux:
        foobar: T.union fooType, barType

      validValues = [
        foobar:
          foo: 'foo'
          bar: 42
      ,
        foobar:
          baz: 'quux'
      ]
      invalidValues = [
        foobar:
          qux: 'quux'
      ]

      check validValues, invalidValues, varType

    it 'can create unions without T.union', ->
      fooType = T
        Foo:
          foo: T.str
          bar: T.num

      barType = T
        Bar:
          baz: T.str

      varType = T qux:
        foobar: [ fooType, barType ] # shorthand notation

      validValues = [
        foobar:
          foo: 'foo'
          bar: 42
      ,
        foobar:
          baz: 'quux'
      ]
      invalidValues = [
        foobar:
          qux: 'quux'
      ]

      check validValues, invalidValues, varType

    it 'does not allow untyped unions', ->
      throws -> T foo: T.union()
      throws -> T foo: T.union
      throws -> T foo: T.union identity

    it 'does not allow arbitrary arguments', ->
      throws -> T foo: T.union bar: 'baz'
      throws -> T foo: T.union { bar: 'baz' }, { qux: 'qux' }

    it 'allows validators', ->
      type = T foo: T.union T.str, T.num, (value) -> if value is 42 or value is '42' then 'Fail' else null
      ok null is T.check 41, type
      ok null is T.check '41', type
      ok null isnt T.check 42, type
      ok null isnt T.check '42', type

  describe 'Enum', ->

    it 'does not allow invalid definitions', ->
      throws ->
        T foo: T.str [41, 42, 43]
      throws ->
        T foo: T.num ['Windows', 'OSX', 'Linux']

    it 'Number', ->
      validValues = [ 1, 3, 5, 7, 9 ]
      invalidValues = [ 2, 4, 6, 8 ].concat s_not_nums
      type = T foo: T.num validValues
      check validValues, invalidValues, type

    it 'String', ->
      validValues = ['Windows', 'OSX', 'Linux']
      invalidValues = s_all
      type = T foo: T.str validValues
      check validValues, invalidValues, type

    it 'Boolean', ->
      validValues = [ yes, no ]
      invalidValues = s_not_bools
      type = T foo: T.bool validValues
      check validValues, invalidValues, type
     
    it 'Function', ->
      f1 = ->
      f2 = ->
      validValues = [ f1, f2 ]
      invalidValues = s_all
      type = T foo: T.func validValues
      check validValues, invalidValues, type

    it 'Error', ->
      e1 = new Error 'foo'
      e2 = new Error 'bar'
      validValues = [ e1, e2 ]
      invalidValues = s_all
      type = T foo: T.err validValues
      check validValues, invalidValues, type

    it 'Date', ->
      d1 = new Date Date.now() - 1000
      d2 = new Date Date.now() - 10000
      validValues = [ d1, d2 ]
      invalidValues = s_all
      type = T foo: T.date validValues
      check validValues, invalidValues, type

    it 'RegExp', ->
      rx1 = /\s+/
      rx2 = /.+/g
      validValues = [ rx1, rx2 ]
      invalidValues = s_all
      type = T foo: T.regexp validValues
      check validValues, invalidValues, type

  it 'inspect', ->
    type = T
      Foo:
        a: T.any
        b: T.num
        c: T.str
        d: T.bool
        e: T.func
        f: T.err
        g: T.date
        h: T.regexp
    description =
      Foo:
        a: 'Any'
        b: 'Number'
        c: 'String'
        d: 'Boolean'
        e: 'Function'
        f: 'Error'
        g: 'Date'
        h: 'RegExp'

    deepEqual type.inspect(), description

