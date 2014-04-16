Steam.MainView = (_) ->

  _topics = [
    key: 'frames'
    title: 'Datasets'
  ,
    key: 'models'
    title: 'Models'
  ]

  _isDisplayingTopics = node$ no
  _topic = node$ ''

  _topicTitle = lift$ _topic, _isDisplayingTopics, (topic, isDisplayingTopics) ->
    if isDisplayingTopics then 'Menu' else if topic then topic.title else ''

  toggleTopics = -> _isDisplayingTopics not _isDisplayingTopics()

  apply$ _topic, (topic) ->
    switch topic.key
      when 'frames'
        unless _frameListView()
          switchView _allTopicViews, _frameListView, Steam.FrameListView _
          _.displayFrames()
      when 'models'
        unless _modelListView()
          switchView _allTopicViews, _modelListView, Steam.ModelListView _
          _.displayModels()

    _isDisplayingTopics no

  _topicListView = Steam.TopicListView _, _topics
  [_frameListView, _modelListView] = _allTopicViews = times 3, -> node$ null
  [ _emptyView, _frameView, _modelView ] = _allDetailViews = times 3, -> node$ null

  switchView = (sourceNodes, targetNode, view) ->
    for sourceNode in sourceNodes when oldView = sourceNode()
      oldView.dispose()
      sourceNode null
    targetNode view
    return

  link$ _.displayFrame, (key, frame) ->
    switchView _allDetailViews, _frameView, Steam.FrameView _, key, frame

  link$ _.displayModel, (key, model) ->
    switchView _allDetailViews, _modelView, Steam.ModelView _, key, model

  link$ _.switchTopic, _topic

  #TODO do this through hash uris
  _.switchTopic head _topics

  isDisplayingTopics: _isDisplayingTopics
  topicTitle: _topicTitle
  toggleTopics: toggleTopics
  topicListView: _topicListView
  
  frameListView: _frameListView
  modelListView: _modelListView
  
  emptyView: _emptyView
  frameView: _frameView
  modelView: _modelView

