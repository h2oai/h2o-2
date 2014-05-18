geyser = do ->
  _tagCache = {}

  class GeyserTag
    constructor: (@name, @classes, @attrs) ->

  class GeyserNode
    constructor: (@id, @tag, @content) ->

  createTag = (tagSpec, attrSpec) ->
    switch tagSpec.indexOf '.'
      when -1
        # no classes
        new GeyserTag tagSpec, null, attrSpec
      when 0
        # no name
        new GeyserTag 'div', (((tagSpec.substr 1).split '.').join ' '), attrSpec
      else
        # both name and classes
        [tag, classes...] = tagSpec.split '.'
        new GeyserTag tag, classes.join(' '), attrSpec

  parseTagSpec = (spec) ->
    if -1 is i = spec.indexOf ' '
      # no attrs
      createTag spec, null
    else
      # has attrs
      createTag (spec.substr 0, i), (spec.substr i + 1)

  getOrCreateTag = (spec) ->
    if tag = _tagCache[spec]
      tag
    else
      _tagCache[spec] = parseTagSpec spec

  generate = (specs) ->
    console.assert isArray specs

    map specs, (spec) ->
      tag = getOrCreateTag spec
      (arg, id) ->
        new GeyserNode id, tag,
          if isArray arg
            arg
          else if arg instanceof GeyserNode
            # this makes it convenient to say
            #   div div 'foo'
            # instead of
            #   div [ div 'foo' ]
            [ arg ]
          else
            arg

  render = (arg) ->
    if arg instanceof GeyserNode
      id = if arg.id then " id='#{arg.id}'" else ''
      tag = arg.tag
      name = tag.name
      classes = if tag.classes then " class='#{tag.classes}'" else ''
      attrs = if tag.attrs then " #{tag.attrs}" else ''
      content = render arg.content
      "<#{name}#{id}#{classes}#{attrs}>#{content}</#{name}>"
    else if isArray arg
      join (map arg, render), ''
    else
      if arg then arg else ''

  generate: generate
  render: render

