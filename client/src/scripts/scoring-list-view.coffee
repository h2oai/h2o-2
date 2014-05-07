# TODO automatically batch scorings into colored groups
# HSL( (((i % 3) * n / 3) + (i / 3)) * 255.0 / n, 255, 128)

Steam.ScoringListView = (_) ->
  _predicate = node$ type: 'all'
  _items = do nodes$
  _hasItems = lift$ _items, (items) -> items.length > 0

  #TODO ugly
  _isLive = node$ yes

  displayItem = (item) ->
    if item
      _.displayScoring item
    else
      _.displayEmpty()

  findActiveItem = ->
    find _items(), (item) -> item.isActive()

  activateAndDisplayItem = (item) ->
    for other in _items()
      if other is item
        other.isActive yes
      else
        other.isActive no

    displayItem item

  createScoringItem = (scoring) ->
    #TODO replace with type checking
    #TODO dispose isSelected properly
    self =
      type: 'scoring'
      data:
        input: scoring
        output: null
      title: scoring.frameKey
      caption: "#{scoring.model.model_algorithm} (#{scoring.model.response_column_name})"
      cutline: new Date(scoring.timestamp).toString()
      display: -> activateAndDisplayItem self
      isActive: node$ no
      isSelected: node$ no
      state: node$ 'waiting'
      isReady: node$ no
      hasFailed: node$ no

    apply$ _isLive, self.isSelected, (isLive, isSelected) ->
      _.scoringSelectionChanged isSelected, self if isLive

    self

  createComparisonItem = (comparison) ->
    self =
      type: 'comparison'
      data: comparison
      title: 'Comparison' #TODO needs a better caption
      caption: describeCount comparison.scorings.length, 'scoring'
      cutline: new Date(comparison.timestamp).toString()
      display: -> activateAndDisplayItem self
      isActive: node$ no
      isSelected: node$ no
      isReady: node$ yes
      hasFailed: node$ no

    apply$ _isLive, self.isSelected, (isLive, isSelected) ->
      _.scoringSelectionChanged isSelected, self if isLive

    self

  runScoringJobs = (jobs, go) ->
    queue = copy jobs
    runNext = ->
      if job = shift queue
        job.run -> defer runNext
      else
        go()
    defer runNext

  createScoringJobs = (items) ->
    map items, (item) ->
      frameKey = item.data.input.frameKey
      modelKey = item.data.input.model.key
      run: (go) ->
        item.state 'running'
        _.requestScoringOnFrame frameKey, modelKey, (error, result) ->
          item.state if error then 'error' else 'success'
          item.isReady yes
          if error
            _.error 'Scoring failed', { frameKey: frameKey, modelKey: modelKey }, error
            #item.time if error.response then error.response.time else 0
            item.hasFailed yes
            item.data.output = error
          else
            #TODO what does it mean to have > 1 metrics
            #TODO put this in the comparison table
            #item.time if result.metrics and result.metrics.length > 0 then (head result.metrics).duration_in_ms else 0
            item.data.output = result

          do go

  loadScorings = (predicate) ->
    console.assert isDefined predicate
    #pastScorings = (_.getFromCache 'scoring') or _.putIntoCache 'scoring', []

    switch predicate.type
      when 'scoring'
        items = map predicate.scorings, createScoringItem
        _items.splice.apply _items, [0, 0].concat items
        jobs = createScoringJobs items
        runScoringJobs jobs, ->
        activateAndDisplayItem head items
        _.scoringsLoaded()
      when 'comparison'
        item = createComparisonItem
          scorings: predicate.scorings
          timestamp: predicate.timestamp
        _items.unshift item
        activateAndDisplayItem item

    _predicate predicate
    return

  link$ _.loadScorings, (predicate) ->
    if predicate
      loadScorings predicate
    else
      displayItem findActiveItem()

  deselectAllScorings = ->
    #TODO ugly
    _isLive no
    for item in _items()
      item.isSelected no
    _isLive yes
    _.scoringSelectionCleared()

  deleteActiveScoring = () ->
    _items.remove findActiveItem()
    _.displayEmpty()

  deleteScorings = (scorings) ->
    deselectAllScorings()
    _items.removeAll scorings 
    unless findActiveItem()
      _.displayEmpty() 

  link$ _.deselectAllScorings, deselectAllScorings
  link$ _.deleteScorings, deleteScorings
  link$ _.deleteActiveScoring, deleteActiveScoring

  items: _items
  hasItems: _hasItems
  template: 'scoring-list-view'

