describe 'Geyser', ->
  describe 'generate', ->
    it "div", ->
      [div] = geyser2.generate [ 'div' ]
      dom = div 'hello'
      deepEqual dom,
        tag:
          name: 'div'
          classes: null
          attrs: null
        content: 'hello'
      strictEqual (geyser2.render dom), "<div>hello</div>"

    it ".foo", ->
      [div] = geyser2.generate [ '.foo' ]
      dom = div 'hello'
      deepEqual dom,
        tag:
          name: 'div'
          classes: 'foo'
          attrs: null
        content: 'hello'
      strictEqual (geyser2.render dom), "<div class='foo'>hello</div>"

    it ".foo.bar", ->
      [div] = geyser2.generate [ '.foo.bar' ]
      dom = div 'hello'
      deepEqual dom,
        tag:
          name: 'div'
          classes: 'foo bar'
          attrs: null
        content: 'hello'
      strictEqual (geyser2.render dom), "<div class='foo bar'>hello</div>"

    it "span.foo.bar", ->
      [span] = geyser2.generate [ 'span.foo.bar' ]
      dom = span 'hello'
      deepEqual dom,
        tag:
          name: 'span'
          classes: 'foo bar'
          attrs: null
        content: 'hello'
      strictEqual (geyser2.render dom), "<span class='foo bar'>hello</span>"

    it "a.foo href='http://localhost/'", ->
      [a] = geyser2.generate [ "a.foo href='http://localhost/'" ]
      dom = a 'hello'
      deepEqual dom,
        tag:
          name: 'a'
          classes: 'foo'
          attrs: "href='http://localhost/'"
        content: 'hello'
      strictEqual (geyser2.render dom), "<a class='foo' href='http://localhost/'>hello</a>"

    it ".foo data-id='bar'", ->
      [div] = geyser2.generate [ ".foo data-id='bar'" ]
      dom = div 'hello'
      deepEqual dom,
        tag:
          name: 'div'
          classes: 'foo'
          attrs: "data-id='bar'"
        content: 'hello'
      strictEqual (geyser2.render dom), "<div class='foo' data-id='bar'>hello</div>"

    it "input type='checkbox' data-id='bar' checked", ->
      [input] = geyser2.generate [ "input type='checkbox' data-id='bar' checked" ]
      dom = input 'hello'
      deepEqual dom,
        tag:
          name: 'input'
          classes: null
          attrs: "type='checkbox' data-id='bar' checked"
        content: 'hello'
      strictEqual (geyser2.render dom), "<input type='checkbox' data-id='bar' checked>hello</input>"

    it "input.foo type='checkbox' data-id='bar' checked", ->
      [input] = geyser2.generate [ "input.foo type='checkbox' data-id='bar' checked" ]
      dom = input 'hello'
      deepEqual dom,
        tag:
          name: 'input'
          classes: 'foo'
          attrs: "type='checkbox' data-id='bar' checked"
        content: 'hello'
      strictEqual (geyser2.render dom), "<input class='foo' type='checkbox' data-id='bar' checked>hello</input>"

    it "1 nested element", ->
      [div] = geyser2.generate [ '.foo' ]
      dom = div div 'hello'
      deepEqual dom,
        tag:
          name: 'div'
          classes: 'foo'
          attrs: null
        content: [
          tag:
            name: 'div'
            classes: 'foo'
            attrs: null
          content: 'hello'
        ]
      strictEqual (geyser2.render dom), "<div class='foo'><div class='foo'>hello</div></div>"

    it "2 levels of nested elements", ->
      [div] = geyser2.generate [ '.foo' ]
      dom = div div div 'hello'
      deepEqual dom,
        tag:
          name: 'div'
          classes: 'foo'
          attrs: null
        content: [
          tag:
            name: 'div'
            classes: 'foo'
            attrs: null
          content: [
            tag:
              name: 'div'
              classes: 'foo'
              attrs: null
            content: 'hello'
          ]
        ]
      strictEqual (geyser2.render dom), "<div class='foo'><div class='foo'><div class='foo'>hello</div></div></div>"

    it "1+ nested elements", ->
      [div] = geyser2.generate [ '.foo' ]
      dom = div [
        div 'hello'
        div 'world'
      ]
      deepEqual dom,
        tag:
          name: 'div'
          classes: 'foo'
          attrs: null
        content: [
          tag:
            name: 'div'
            classes: 'foo'
            attrs: null
          content: 'hello'
        ,
          tag:
            name: 'div'
            classes: 'foo'
            attrs: null
          content: 'world'
        ]
      strictEqual (geyser2.render dom), "<div class='foo'><div class='foo'>hello</div><div class='foo'>world</div></div>"
