if window
  dumpTypecheckErrors = (arg) ->
    tab = '  '
    dump = (lines, offset, arg) ->
      if isArray arg
        indent = offset + tab
        for item in arg
          dump lines, indent, item
      else if isString arg
        lines.push offset + arg
      return

    dump lines=[], '', arg
    lines

  typecheck = (value, type) ->
    if error = T.check value, type
      lines = dumpTypecheckErrors error
      message = "Typecheck failed for #{type.name}"
      window.steam.context.fatal message, errors: lines
      console.error message, lines
      no
    else
      yes

do ->
  setAttributes = (el, attributes) ->
    if attributes
      for key, value of attributes
        el.setAttribute key, value
    el

  makeElement = (name, content, attributes) ->
    el = document.createElement name
    if content
      if isString content
        el.appendChild document.createTextNode content
      else
        el.appendChild content
    setAttributes el, attributes

  makeHtmlElement = (name, content, attributes) ->
    el = document.createElement name
    el.innerHTML = content
    setAttributes content

