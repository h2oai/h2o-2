geyser = do ->

  class GeyserTag
    constructor: (@tag, @classes) ->

  class GeyserElement
    constructor: (@el, @content) ->
  
  _cache = {}

  parse = (spec) ->
    switch spec.indexOf '.'
      when -1
        new GeyserTag spec, null
      when 0
        new GeyserTag 'div', split (spec.substr 1), '.'
      else
        [tag, classes...] = split spec, '.'
        new GeyserTag tag, classes

  getElement = (spec) ->
    if el = _cache[spec]
      el
    else
      _cache[spec] = parse spec

  isGeyserElement = (arg) -> arg instanceof GeyserElement

  areGeyserElements = (args) ->
    every args, (arg) ->
      if isArray arg
        areGeyserElements arg
      else
        isGeyserElement arg

  project = (arg) ->
    if isArray arg
      if areGeyserElements arg
        arg
      else
        JSON.stringify arg # garbage in, json out.
    else if isGeyserElement arg
      [ arg ]
    else
      arg

  generate = (arg) ->
    console.assert (isString arg) or (isArray arg)
    specs = if isString arg then words arg else arg
    map specs, (spec) ->
      el = getElement spec
      (arg) -> new GeyserElement el, project arg

  renderOne = (el, content) ->
    classes = if el.classes then ' class="' + (join el.classes, ' ') + '"' else ''
    "<#{el.tag}#{classes}>#{content}</#{el.tag}>"

  renderOne = (node) ->
    element = document.createElement node.tag
    if node.classes
      element.className = join node.classes, ' '
    element

  render = (node) ->
    element = renderOne node.el
    if isElement node.content
      element.appendChild node.content
    else if isArray node.content
      for child in node.content
        element.appendChild render child
    else
      element.appendChild document.createTextNode node.content
    element

  generate: generate
  render: render




