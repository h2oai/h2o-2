geyser = do ->
  
  _cache = {}

  parse = (spec) ->
    dotIndex = spec.indexOf '.'
    switch dotIndex
      when -1
        tag: spec
      when 0
        tag: 'div'
        classes: split (spec.substr 1), '.'
      else
        tokens = split spec, '.'
        tag: tokens.shift()
        classes: tokens

  getElement = (spec) ->
    if el = _cache[spec]
      el
    else
      _cache[spec] = parse spec

  generate = (arg) ->
    console.assert (isString arg) or (isArray arg)
    specs = if isString arg then words arg else arg
    map specs, (spec) ->
      el = getElement spec
      (content) ->
        el: el
        content: content

  renderOne = (el, content) ->
    classes = if el.classes then ' class="' + (join el.classes, ' ') + '"' else ''
    "<#{el.tag}#{classes}>#{content}</#{el.tag}>"

  render = (html) ->
    if isArray html.content
      renderOne html.el, (join (map html.content, render), '')
    else
      renderOne html.el, html.content

  generate: generate
  render: render




