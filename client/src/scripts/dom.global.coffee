
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

