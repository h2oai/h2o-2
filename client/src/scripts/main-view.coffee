Steam.MainView = (_) ->
  createTopic = (title, handle, isEnabled) ->
    self =
      title: title
      isDisabled: not isEnabled
      display: -> handle() if handle

  switchTopic = (topic) ->
    _topic topic
    switch topic
      when _frameTopic
        unless _frameListView()
          switchView _allTopicViews, _frameListView, frameListView, no
      when _modelTopic
        unless _modelListView()
          switchView _allTopicViews, _modelListView, modelListView, no
    _isDisplayingTopics no
    return
  
  switchToFrames = (opts) ->
    switchTopic _frameTopic
    _.loadFrames if opts then opts else type: 'all'

  switchToModels = (opts) ->
    switchTopic _modelTopic
    _.loadModels if opts then opts else type: 'all'

  _topic = node$ null
  _isDisplayingTopics = node$ no
  _topicTitle = lift$ _topic, _isDisplayingTopics, (topic, isDisplayingTopics) ->
    if isDisplayingTopics then 'Menu' else if topic then topic.title else ''
  toggleTopics = -> _isDisplayingTopics not _isDisplayingTopics()
  _topics = node$ [
    _frameTopic = createTopic 'Datasets', switchToFrames, yes
    _modelTopic = createTopic 'Models', switchToModels, yes
    _timelineTopic = createTopic 'Timeline', null, no
    _notificationsTopic = createTopic 'Notifications', null, no
    _jobsTopic = createTopic 'Jobs', null, no
    _clusterTopic = createTopic 'Cluster', null, no
    _administrationTopic = createTopic 'Administration', null, no
  ]

  frameListView = Steam.FrameListView _
  modelListView = Steam.ModelListView _

  [_frameListView, _modelListView] = _allTopicViews = times 3, -> node$ null
  [ _emptyView, _frameView, _modelView ] = _allDetailViews = times 3, -> node$ null

  switchView = (sourceNodes, targetNode, view, dispose=yes) ->
    for sourceNode in sourceNodes when oldView = sourceNode()
      oldView.dispose() if dispose
      sourceNode null
    targetNode view
    return

  link$ _.displayFrame, (key, frame) ->
    switchView _allDetailViews, _frameView, Steam.FrameView _, key, frame

  link$ _.displayModel, (key, model) ->
    switchView _allDetailViews, _modelView, Steam.ModelView _, key, model

  link$ _.switchToFrames, switchToFrames

  link$ _.switchToModels, switchToModels

  #TODO do this through hash uris
  switchToFrames type: 'all'

  isDisplayingTopics: _isDisplayingTopics
  topicTitle: _topicTitle
  toggleTopics: toggleTopics
  topics: _topics
  
  frameListView: _frameListView
  modelListView: _modelListView
  
  emptyView: _emptyView
  frameView: _frameView
  modelView: _modelView

