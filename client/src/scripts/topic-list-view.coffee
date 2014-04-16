Steam.TopicListView = (_, _topics) ->
  activate = (item) ->
    _.switchTopic item.data

  createItem = (topic) ->
    self =
      data: topic
      title: topic.title
      display: -> activate self

  _items = nodes$ map _topics, createItem

  items: _items

