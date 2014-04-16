describe 'Hypergraph', ->
  describe 'API', ->
    it 'should have functions available in application scope', ->
      funcs = [
        edge$
        edges$
        node$
        nodes$
        link$
        unlink$
        call$
        apply$
        join$
        zip$
        lift$
        filter$
        switch$
        if$
        and$
        or$
        not$
        debounce$
        throttle$
      ]
      for func in funcs
        ok typeof func is 'function'
      return

  describe 'Edges', ->
    it 'should not fail when unlinked', ->
      func = do edge$
      result = null
      doesNotThrow -> result = func 1, 2, 3
      ok isUndefined result

    it 'should propagate when linked', ->
      func = do edge$
      link$ func, (a, b, c) -> a + b + c
      strictEqual func(1, 2, 3), 6

    it 'should raise exception when re-linked', ->
      func = do edge$
      link$ func, (a, b, c) -> a + b + c
      strictEqual func(1, 2, 3), 6
      throws -> link$ func, (a, b, c) -> a * b * c

    it 'should stop propagating when unlinked', ->
      func = do edge$
      target = (a, b, c) -> a + b + c
      arrow = link$ func, target
      strictEqual func(1, 2, 3), 6
      unlink$ arrow
      result = null
      doesNotThrow -> result = func 1, 2, 3
      ok isUndefined result

    it 'should stop propagating when disposed', ->
      func = do edge$
      target = (a, b, c) -> a + b + c
      arrow = link$ func, target
      strictEqual func(1, 2, 3), 6
      func.dispose()
      result = null
      doesNotThrow -> result = func 1, 2, 3
      ok isUndefined result

  describe 'Edge (degree > 1)', ->
    it 'should not fail when unlinked', ->
      func = do edges$
      result = null
      doesNotThrow -> result = func 1, 2, 3
      deepEqual result, []

    it 'should propagate when linked', ->
      func = do edges$
      link$ func, (a, b, c) -> a + b + c
      deepEqual func(1, 2, 3), [6]

    it 'should allow multicasting', ->
      func = do edges$
      addition = (a, b, c) -> a + b + c
      multiplication = (a, b, c) -> a * b * c
      link$ func, addition
      link$ func, multiplication
      deepEqual func(2, 3, 4), [9, 24]

    it 'should stop propagating when unlinked', ->
      func = do edges$
      addition = (a, b, c) -> a + b + c
      multiplication = (a, b, c) -> a * b * c
      additionArrow = link$ func, addition
      multiplicationArrow = link$ func, multiplication
      deepEqual func(2, 3, 4), [9, 24]
      unlink$ additionArrow
      deepEqual func(2, 3, 4), [24]
      unlink$ multiplicationArrow
      deepEqual func(2, 3, 4), []

    it 'should stop propagating when disposed', ->
      func = do edges$
      addition = (a, b, c) -> a + b + c
      multiplication = (a, b, c) -> a * b * c
      additionArrow = link$ func, addition
      multiplicationArrow = link$ func, multiplication
      deepEqual func(2, 3, 4), [9, 24]
      func.dispose()
      deepEqual func(2, 3, 4), []

  describe 'Node', ->
    it 'should hold value when initialized', ->
      node = node$ 42
      strictEqual node(), 42

    it 'should return value when called without arguments', ->
      node = node$ 42
      strictEqual node(), 42

    it 'should hold new value when reassigned', ->
      node = node$ 42
      strictEqual node(), 42
      node 43
      strictEqual node(), 43

    it 'should not propagate unless value is changed (without comparator)', ->
      node = node$ 42
      propagated = no
      link$ node, (value) -> propagated = yes
      strictEqual propagated, no
      node 42
      strictEqual propagated, no

    it 'should propagate value when value is changed (without comparator)', ->
      node = node$ 42
      propagated = no
      propagatedValue = 0
      link$ node, (value) ->
        propagated = yes
        propagatedValue = value
      strictEqual propagated, no
      node 43
      strictEqual propagated, yes
      strictEqual propagatedValue, 43

    it 'should not propagate unless value is changed (with comparator)', ->
      comparator = (a, b) -> a.answer is b.answer
      node = node$ { answer: 42 }, comparator
      propagated = no
      link$ node, (value) -> propagated = yes
      strictEqual propagated, no
      node answer: 42
      strictEqual propagated, no

    it 'should propagate when value is changed (with comparator)', ->
      comparator = (a, b) -> a.answer is b.answer
      node = node$ { answer: 42 }, comparator
      propagated = no
      propagatedValue = null
      link$ node, (value) ->
        propagated = yes
        propagatedValue = value
      strictEqual propagated, no

      newValue = answer: 43
      node newValue
      strictEqual propagated, yes
      strictEqual propagatedValue, newValue

    it 'should allow multicasting', ->
      node = node$ 42
      propagated1 = no
      propagated2 = no
      target1 = (value) -> propagated1 = yes
      target2 = (value) -> propagated2 = yes
      link$ node, target1
      link$ node, target2
      strictEqual propagated1, no
      strictEqual propagated2, no

      node 43
      strictEqual propagated1, yes
      strictEqual propagated2, yes

    it 'should stop propagating when unlinked', ->
      node = node$ 42
      propagated1 = no
      propagated2 = no
      target1 = (value) -> propagated1 = yes
      target2 = (value) -> propagated2 = yes
      arrow1 = link$ node, target1
      arrow2 = link$ node, target2
      strictEqual propagated1, no
      strictEqual propagated2, no

      node 43
      strictEqual propagated1, yes
      strictEqual propagated2, yes

      propagated1 = no
      propagated2 = no
      unlink$ arrow2
      node 44
      strictEqual propagated1, yes
      strictEqual propagated2, no

      propagated1 = no
      propagated2 = no
      unlink$ arrow1
      node 45
      strictEqual propagated1, no
      strictEqual propagated2, no

    it 'empty nodes should always propagate', ->
      event = do node$
      propagated = no
      link$ event, -> propagated = yes
      strictEqual propagated, no
      event yes
      strictEqual propagated, yes

  describe 'Context', ->
    it 'should enclose deeply nested subgraphs correctly', ->
      foo1 = ->
      bar1 = ->
      baz1 = ->
      foo2 = ->
      bar2 = ->
      baz2 = ->
      foo3 = ->
      bar3 = ->
      baz3 = ->

      grandparent = context$ foo1: foo1, bar1: bar1, baz1: baz1
      parent = context$ { foo2: foo2, bar2: bar2, baz2: baz2 }, grandparent
      child = context$ { foo3: foo3, bar3: bar3, baz3: baz3 }, parent

      strictEqual grandparent.root, grandparent
      strictEqual grandparent.parent, grandparent
      strictEqual grandparent.foo1, foo1
      strictEqual grandparent.bar1, bar1
      strictEqual grandparent.baz1, baz1

      strictEqual parent.root, grandparent
      strictEqual parent.parent, grandparent
      strictEqual parent.foo2, foo2
      strictEqual parent.bar2, bar2
      strictEqual parent.baz2, baz2

      strictEqual child.root, grandparent
      strictEqual child.parent, parent
      strictEqual child.foo3, foo3
      strictEqual child.bar3, bar3
      strictEqual child.baz3, baz3


  describe 'Link/Unlink', ->
    it 'should unlink multiple arrows at once', ->
      node = node$ 42
      propagated1 = no
      propagated2 = no
      target1 = (value) -> propagated1 = yes
      target2 = (value) -> propagated2 = yes
      arrow1 = link$ node, target1
      arrow2 = link$ node, target2
      strictEqual propagated1, no
      strictEqual propagated2, no

      node 43
      strictEqual propagated1, yes
      strictEqual propagated2, yes

      propagated1 = no
      propagated2 = no
      unlink$ [ arrow1, arrow2 ]
      node 44
      strictEqual propagated1, no
      strictEqual propagated2, no

  describe 'Combinators', ->
    it 'call$', ->
      width = node$ 2
      height = node$ 6
      area = 0
      arrow = call$ width, height, -> area = width() * height()
      strictEqual area, 12

      width 7
      strictEqual area, 42

      unlink$ arrow
      width 2
      strictEqual area, 42

    it 'apply$', ->
      width = node$ 2
      height = node$ 6
      area = 0
      arrow = apply$ width, height, (w, h) -> area = w * h
      strictEqual area, 12

      width 7
      strictEqual area, 42

      unlink$ arrow
      width 2
      strictEqual area, 42

    it 'join$', ->
      width = node$ 2
      height = node$ 6
      area = node$ 0
      arrow = join$ width, height, area, (w, h) -> w * h
      strictEqual area(), 12

      width 7
      strictEqual area(), 42

      unlink$ arrow
      width 2
      strictEqual area(), 42

    it 'zip$', ->
      width = node$ 2
      height = node$ 6
      area = zip$ [width, height], (w, h) -> w * h
      strictEqual area(), 12

      width 7
      strictEqual area(), 42

    it 'lift$', ->
      width = node$ 2
      height = node$ 6
      area = lift$ width, height, (w, h) -> w * h
      strictEqual area(), 12

      width 7
      strictEqual area(), 42


    it 'filter$', ->
      integers = node$ 10
      evens = filter$ integers, (n) -> n % 2 is 0
      strictEqual evens(), 10
      integers 9
      strictEqual evens(), 10
      integers 8
      strictEqual evens(), 8

    it 'switch$', ->
      defaultValue = {}
      someValue = {}
      [choice1, choice2, choice3] = switch$ defaultValue, 3
      
      strictEqual choice1(), defaultValue
      strictEqual choice2(), defaultValue
      strictEqual choice3(), defaultValue

      choice1 someValue
      strictEqual choice1(), someValue
      strictEqual choice2(), defaultValue
      strictEqual choice3(), defaultValue

      choice2 someValue
      strictEqual choice1(), defaultValue
      strictEqual choice2(), someValue
      strictEqual choice3(), defaultValue

      choice3 someValue
      strictEqual choice1(), defaultValue
      strictEqual choice2(), defaultValue
      strictEqual choice3(), someValue

    it 'if$', ->
      english = node$ 'thank you'
      spanish = node$ 'gracias'
      language = node$ 'english'
      isEnglish = lift$ language, (language) -> language is 'english'
      greeting = if$ isEnglish, english, spanish
      equal greeting(), 'thank you'
      language 'spanish'
      equal greeting(), 'gracias'


    it 'and$', ->
      hasRed = node$ yes
      hasGreen = node$ yes
      hasBlue = node$ yes
      hasColor = and$ hasRed, hasGreen, hasBlue
      strictEqual hasColor(), yes
      hasRed no
      strictEqual hasColor(), no
      hasRed yes
      strictEqual hasColor(), yes

    it 'or$', ->
      hasRed = node$ no
      hasGreen = node$ no
      hasBlue = node$ no
      hasComponent = or$ hasRed, hasGreen, hasBlue
      strictEqual hasComponent(), no
      hasRed yes
      strictEqual hasComponent(), yes
      hasRed no
      strictEqual hasComponent(), no

    it 'not$', ->
      isTransparent = node$ yes
      isOpaque = not$ isTransparent
      strictEqual isOpaque(), no
      isTransparent no
      strictEqual isOpaque(), yes


