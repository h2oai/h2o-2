Steam.ScoringListView = (_) ->
  _items = do nodes$
  _hasItems = lift$ _items, (items) -> items.length > 0

  #TODO ugly
  _isLive = node$ yes

  activateItem = (item) ->
    for other in _items()
      if other is item
        other.isActive yes
      else
        other.isActive no

    _.displayScoring item.data

  createItem = (scoring) ->
    #TODO replace with type checking
    self =
      data: scoring
      title: scoring.frameKey
      caption: describeCount scoring.scores.length, 'model'
      cutline: new Date(scoring.timestamp).toString()
      display: -> activateItem self
      isActive: node$ no
      isSelected: node$ no

    apply$ _isLive, self.isSelected, (isLive, isSelected) ->
      _.scoringSelectionChanged isSelected, _predicate, scoring if isLive
    self

  displayScorings = (scorings) ->
    _items items = map scorings, createItem
    if isEmpty items
      _.displayEmpty()
    else
      activateItem head items

  _predicate = null
  loadScorings = (predicate) ->
    console.assert isDefined predicate
    _predicate = predicate
    
    pastScorings = (_.getFromCache 'scoring') or _.putIntoCache 'scoring', []
    if predicate.type is 'scoring'
      pastScorings.unshift predicate.scoring

    displayScorings pastScorings

    return

  link$ _.loadScorings, loadScorings
  link$ _.deselectAllScorings, ->
    #TODO ugly
    _isLive no
    for item in _items()
      item.isSelected no
    _isLive yes

  items: _items
  hasItems: _hasItems
  template: 'scoring-list-view'

