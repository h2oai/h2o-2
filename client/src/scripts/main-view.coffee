Steam.MainView = (_) ->
  _listViews = nodes$ []
  _pageViews = nodes$ []
  _modalViews = nodes$ []
  _isModal = lift$ _modalViews, (modalViews) -> modalViews.length > 0
  _topic = node$ null
  _isDisplayingTopics = node$ no
  _isListMasked = node$ no
  _isPageMasked = lift$ _isDisplayingTopics, identity
  _topicTitle = lift$ _topic, _isDisplayingTopics, (topic, isDisplayingTopics) ->
    if isDisplayingTopics then 'Menu' else if topic then topic.title else ''
  toggleTopics = -> _isDisplayingTopics not _isDisplayingTopics()
  apply$ _isDisplayingTopics, (isDisplayingTopics) ->
    if isDisplayingTopics
      _listViews.push topicListView
    else
      _listViews.remove topicListView

  createTopic = (title, handle, isEnabled) ->
    self =
      title: title
      isDisabled: not isEnabled
      display: -> handle() if handle

  switchTopic = (topic) ->
    switch topic
      when _frameTopic
        unless _topic() is topic
          _topic topic
          switchList frameListView
      when _modelTopic
        unless _topic() is topic
          _topic topic
          switchList modelListView
      when _scoringTopic
        unless _topic() is topic
          _topic topic
          switchList scoringListView
    _isDisplayingTopics no
    return
  
  switchToFrames = (opts) ->
    switchTopic _frameTopic
    _.loadFrames if opts then opts else type: 'all'

  switchToModels = (opts) ->
    switchTopic _modelTopic
    _.loadModels if opts then opts else type: 'all'

  switchToScoring = (opts) ->
    switchTopic _scoringTopic
    _.loadScorings if opts then opts else type: 'all'

  _topics = node$ [
    _frameTopic = createTopic 'Datasets', switchToFrames, yes
    _modelTopic = createTopic 'Models', switchToModels, yes
    _scoringTopic = createTopic 'Scoring', switchToScoring, yes
    _timelineTopic = createTopic 'Timeline', null, no
    _notificationsTopic = createTopic 'Notifications', null, no
    _jobsTopic = createTopic 'Jobs', null, no
    _clusterTopic = createTopic 'Cluster', null, no
    _administrationTopic = createTopic 'Administration', null, no
  ]

  topicListView = Steam.TopicListView _, _topics
  frameListView = Steam.FrameListView _
  modelListView = Steam.ModelListView _
  scoringListView = Steam.ScoringListView _
  modelSelectionView = Steam.ModelSelectionView _

  switchView = (views, view) ->
    for oldView in views()
      oldView.dispose() if isFunction oldView.dispose
    if view
      views [ view ]
    else
      views.removeAll()

  switchList = (view) -> switchView _listViews, view
  switchPage = (view) -> switchView _pageViews, view
  switchModal = (view) -> switchView _modalViews, view
 
  _template = (view) -> view.template

  link$ _.displayFrame, (frame) ->
    switchPage Steam.FrameView _, frame

  link$ _.displayModel, (model) ->
    switchPage Steam.ModelView _, model

  link$ _.displayScoring, (scoring) ->
    switchPage Steam.ScoringView _, scoring

  link$ _.switchToFrames, switchToFrames

  link$ _.switchToModels, switchToModels

  link$ _.switchToScoring, switchToScoring

  link$ _.modelsSelected, -> switchModal modelSelectionView

  link$ _.modelsDeselected, -> _modalViews.remove modelSelectionView

  #TODO do this through hash uris
  switchToFrames type: 'all'

  topicTitle: _topicTitle
  toggleTopics: toggleTopics
  listViews: _listViews
  pageViews: _pageViews
  modalViews: _modalViews
  isListMasked: _isListMasked
  isPageMasked: _isPageMasked
  isModal: _isModal
  template: _template
