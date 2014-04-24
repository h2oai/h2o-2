
truthy = [ [1, 2, 3], true, new Date(), { 'a' : 1 }, 42, /x/, 'a' ]
falsy = [ '', 0, false, NaN, null, undefined, ]

describe 'Prelude', ->
  describe 'API', ->
    it 'should have functions available in application scope', ->
      funcs = [
        compact
        difference
        findIndex
        findLastIndex
        flatten
        head
        indexOf
        initial
        intersection
        last
        lastIndexOf
        pull
        range
        removeWhere
        sortedIndex
        tail
        union
        unique
        without
        xor
        zip
        zipObject
        at
        contains
        countBy
        every
        filter
        find
        findLast
        forEach
        forEachRight
        groupBy
        indexBy
        map
        max
        min
        pluck
        reduce
        reduceRight
        reject
        sample
        shuffle
        size
        some
        sortBy
        toArray
        where
        after
        compose
        curry
        debounce
        defer
        delay
        memoize
        once
        partial
        partialRight
        throttle
        wrap
        clone
        cloneDeep
        defaults
        extend
        findKey
        findLastKey
        forIn
        forInRight
        forOwn
        forOwnRight
        functions
        has
        invert
        isArguments
        isArray
        isBoolean
        isDate
        isElement
        isEmpty
        isEqual
        isFinite
        isFunction
        isNaN
        isNull
        isNumber
        isObject
        isPlainObject
        isRegExp
        isString
        isUndefined
        keys
        mapValues
        merge
        omit
        pairs
        pick
        transform
        values
        now
        constant
        escape
        identity
        noop
        property
        random
        times
        unescape
        uniqueId
        apply
        isDefined
        isTruthy
        isFalsy
        isError
        negate
        always
        never
        join
        split
        words
        sort
        copy
        concat
        unshift
        shift
        push
        shove
        pop
        splice
        remove
        clear
        repeat
        mapWithKey
        zipCompare
      ]
      for func in funcs
        ok typeof func is 'function'
      return

  describe 'Utility functions', ->
    it 'isDefined', ->
      for arg in falsy
        strictEqual (isDefined arg), arg isnt undefined

      for arg in truthy
        strictEqual (isDefined arg), yes

      return
      
    it 'isTruthy', ->
      for arg in falsy
        strictEqual (isTruthy arg), if arg then yes else no

      for arg in truthy
        strictEqual (isTruthy arg), if arg then yes else no

      return

    it 'isFalsy', ->
      for arg in falsy
        strictEqual (isFalsy arg), if arg then no else yes

      for arg in truthy
        strictEqual (isFalsy arg), if arg then no else yes

      return

    it 'isError', ->
      strictEqual (isError new Error()), yes
      strictEqual (isError {}), no
      return
    
  describe 'String ops', ->
    it 'join', ->
      array = [ 'foo', 'bar' ]
      delims = [
        undefined
        null
        ''
        ' '
        ','
      ]
      for delim in delims
        strictEqual (join array, delim), (array.join delim)
      return

    it 'split', ->
      args = [
        [ 'foo bar   baz', /\s+/ ]
        [ 'foo, bar, baz', ', ' ]
      ]
      for arg in args
        [ str, delim ] = arg
        deepEqual (split str, delim), (str.split delim)
      return

    it 'words', ->
      deepEqual (words 'foo bar baz'), [ 'foo', 'bar', 'baz' ]
      deepEqual (words ' foo   bar    baz   '), [ '', 'foo', 'bar', 'baz', '' ]

  describe 'Array ops', ->
    it 'sort without comparator', ->
      array = [ 'foo', 'bar', 'baz' ]
      expected = array.slice(0).sort()
      deepEqual (sort array), expected

    it 'sort with comparator', ->
      array = [ 40, 100, 1, 5, 25, 10 ]
      comparator = (a, b) -> b - a
      expected = array.slice(0).sort comparator
      deepEqual (sort array, comparator), expected

    it 'copy', ->
      o1 = {}
      o2 = {}
      o3 = {}
      array = [ o1, o2, o3 ]
      expected = array.slice 0
      actual = copy array
      deepEqual actual, expected
      notStrictEqual actual, expected

    it 'concat', ->
      array1 = [ 1, 2, 3 ]
      array2 = [ 4, 5, 6 ]
      array3 = [ 7, 8, 9 ]
      actual = concat array1, array2, array3
      notStrictEqual array1, actual
      deepEqual actual, [ 1, 2, 3, 4, 5, 6, 7, 8, 9 ]


    it 'unshift', ->
      o1 = {}
      o2 = {}
      o3 = {}
      array = [ o2, o3 ]
      actual = unshift array, o1
      deepEqual actual, [ o1, o2, o3 ]
      strictEqual actual, array

    it 'shift', ->
      o1 = {}
      o2 = {}
      o3 = {}
      array = [ o1, o2, o3 ]
      actual = shift array
      deepEqual array, [ o2, o3 ]
      strictEqual actual, o1

    it 'push', ->
      o1 = {}
      o2 = {}
      o3 = {}
      array = [ o1, o2 ]
      actual = push array, o3
      deepEqual actual, [ o1, o2, o3 ]
      strictEqual actual, array

    it 'shove', ->
      o1 = {}
      o2 = {}
      o3 = {}
      array = [ o1 ]
      actual = shove array, [ o2, o3 ]
      deepEqual actual, [ o1, o2, o3 ]
      strictEqual actual, array

    it 'pop', ->
      o1 = {}
      o2 = {}
      o3 = {}
      array = [ o1, o2, o3 ]
      actual = pop array
      deepEqual array, [ o1, o2 ]
      strictEqual actual, o3

    it 'splice', ->
      o1 = {}
      o2 = {}
      o3 = {}
      array = [ o1, o2, o3 ]
      actual = splice array, 1, 1
      deepEqual array, [ o1, o3 ]
      deepEqual actual, [ o2 ]

    it 'remove', ->
      o1 = {}
      o2 = {}
      o3 = {}
      array = [ o1, o2, o3 ]

      o4 = remove array, o2
      strictEqual o4, o2
      strictEqual array.length, 2
      strictEqual array[0], o1
      strictEqual array[1], o3

      o5 = remove array, o2
      ok is undefined
      strictEqual array.length, 2

    it 'clear', ->
      array1 = [ 'foo', 'bar' ]
      array2 = clear array1
      equal array1.length, 0
      strictEqual array1, array2

    it 'repeat', ->
      deepEqual (repeat 3, 5), [ 5, 5, 5 ]
      deepEqual (repeat 3, 'a'), [ 'a', 'a', 'a' ]

    it 'zipCompare', ->
      ok not zipCompare undefined, undefined
      ok not zipCompare null, null
      ok not zipCompare 1, 2
      ok not zipCompare 1, 1
      ok not zipCompare [], [10]
      ok not zipCompare [], null
      ok not zipCompare null, []
      ok zipCompare [10], [10]
      ok zipCompare [10, 20], [10, 20]
      ok not zipCompare { foo: 'bar' }, { foo: 'bar' }
      ok zipCompare [{ foo: 'bar' }], [{ foo: 'bar' }], (a, b) -> a.foo is b.foo

  describe 'Object ops', ->
    it 'mapWithKey', ->
      obj =
        foo: 10
        bar: 20
        baz: 30
      mapper = (v, k) -> k + '=' + v
      strictEqual (mapWithKey obj, mapper).join(','), 'foo=10,bar=20,baz=30'
