Steam.MainView = (_) ->
  _listViews = nodes$ []
  _selectionViews = node$ []
  _pageViews = nodes$ []
  _modalViews = nodes$ []
  _isModal = lift$ _modalViews, (modalViews) -> modalViews.length > 0
  _isHelpHidden = node$ no
  _topic = node$ null
  _isDisplayingTopics = node$ no
  _isListMasked = node$ no
  _isPageMasked = lift$ _isDisplayingTopics, identity
  _topicTitle = lift$ _topic, _isDisplayingTopics, (topic, isDisplayingTopics) ->
    if isDisplayingTopics then 'Menu' else if topic then topic.title else ''
  toggleTopics = -> _isDisplayingTopics not _isDisplayingTopics()
  toggleHelp = -> _isHelpHidden not _isHelpHidden()
  apply$ _isDisplayingTopics, (isDisplayingTopics) ->
    if isDisplayingTopics
      _listViews.push _topicListView
    else
      _listViews.remove _topicListView

  createTopic = (title, handle) ->
    self =
      title: title
      isDisabled: not isFunction handle
      display: -> handle() if handle

  switchTopic = (topic) ->
    switch topic
      when _frameTopic
        unless _topic() is topic
          _topic topic
          switchListView _frameListView
          switchSelectionView null
      when _modelTopic
        unless _topic() is topic
          _topic topic
          switchListView _modelListView
          switchSelectionView null
      when _scoringTopic
        unless _topic() is topic
          _topic topic
          switchListView _scoringListView
          switchSelectionView _scoringSelectionView
      when _notificationTopic
        unless _topic() is topic
          _topic topic
          switchListView _notificationListView
          switchSelectionView null
    _isDisplayingTopics no
    return
  
  switchToFrames = (predicate) ->
    switchTopic _frameTopic
    _.loadFrames predicate

  switchToModels = (predicate) ->
    switchTopic _modelTopic
    _.loadModels predicate

  switchToScoring = (predicate) ->
    switchTopic _scoringTopic
    _.loadScorings predicate

  switchToNotifications = (predicate) ->
    switchTopic _notificationTopic
    _.loadNotifications predicate

  _topics = node$ [
    _frameTopic = createTopic 'Datasets', switchToFrames
    _modelTopic = createTopic 'Models', switchToModels
    _scoringTopic = createTopic 'Scoring', switchToScoring
    _timelineTopic = createTopic 'Timeline', null
    _notificationTopic = createTopic 'Notifications', switchToNotifications
    _jobTopic = createTopic 'Jobs', null
    _clusterTopic = createTopic 'Cluster', null
    _administrationTopic = createTopic 'Administration', null
  ]

  # List views
  _topicListView = Steam.TopicListView _, _topics
  _frameListView = Steam.FrameListView _
  _modelListView = Steam.ModelListView _
  _scoringListView = Steam.ScoringListView _
  _notificationListView = Steam.NotificationListView _

  # Selection views
  _modelSelectionView = Steam.ModelSelectionView _
  _scoringSelectionView = Steam.ScoringSelectionView _

  switchView = (views, view) ->
    for oldView in views()
      oldView.dispose() if isFunction oldView.dispose
    if view
      views [ view ]
    else
      views []

  switchListView = (view) -> switchView _listViews, view
  switchSelectionView = (view) -> switchView _selectionViews, view
  switchPageView = (view) -> switchView _pageViews, view
  switchModalView = (view) -> switchView _modalViews, view
 
  _template = (view) -> view.template

  link$ _.displayEmpty, ->
    switchPageView template: 'empty-view'

  link$ _.displayFrame, (frame) ->
    switchPageView Steam.FrameView _, frame if _topic() is _frameTopic

  link$ _.displayModel, (model) ->
    switchPageView Steam.ModelView _, model if _topic() is _modelTopic

  link$ _.displayScoring, (scoring) ->
    switchPageView Steam.ScoringView _, scoring if _topic() is _scoringTopic

  link$ _.displayNotification, (notification) ->
    switchPageView Steam.NotificationView _, notification if _topic() is _notificationTopic

  link$ _.switchToFrames, switchToFrames

  link$ _.switchToModels, switchToModels

  link$ _.switchToScoring, switchToScoring

  link$ _.switchToNotifications, switchToNotifications

  link$ _.modelsSelected, -> switchModalView _modelSelectionView

  link$ _.modelsDeselected, -> _modalViews.remove _modelSelectionView

  #TODO do this through hash uris
  switchToFrames type: 'all'

  topicTitle: _topicTitle
  toggleTopics: toggleTopics
  toggleHelp: toggleHelp
  listViews: _listViews
  selectionViews: _selectionViews
  pageViews: _pageViews
  modalViews: _modalViews
  isListMasked: _isListMasked
  isPageMasked: _isPageMasked
  isModal: _isModal
  isHelpHidden: _isHelpHidden
  template: _template
