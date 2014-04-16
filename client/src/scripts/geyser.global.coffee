geyser = do ->
  generate = (tags) ->
    map tags, (tag) ->
      (content) ->
        tag: tag, content: content

  renderOne = (tag, content) ->
    "<#{tag}>#{content}</#{tag}>"

  render = (html) ->
    if isArray html.content
      renderOne html.tag, (join (map html.content, render), '')
    else
      renderOne html.tag, html.content

  generate: generate
  render: render




