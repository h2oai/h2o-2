Steam.ScoringListView = (_) ->
  _predicate = node$ type: 'all'
  _items = do nodes$
  _hasItems = lift$ _items, (items) -> items.length > 0

  #TODO ugly
  _isLive = node$ yes

  displayItem = (item) ->
    if item
      _.displayScoring item.data
    else
      _.displayEmpty()

  displayActiveItem = ->
    displayItem find _items(), (item) -> item.isActive()

  activateAndDisplayItem = (item) ->
    for other in _items()
      if other is item
        other.isActive yes
      else
        other.isActive no

    displayItem item

  createItem = (scoring) ->
    #TODO replace with type checking
    self =
      data: scoring
      title: scoring.frameKey
      caption: describeCount scoring.scores.length, 'model'
      cutline: new Date(scoring.timestamp).toString()
      display: -> activateAndDisplayItem self
      isActive: node$ no
      isSelected: node$ no

    apply$ _isLive, self.isSelected, (isLive, isSelected) ->
      _.scoringSelectionChanged isSelected, _predicate(), scoring if isLive
    self

  displayScorings = (scorings) ->
    _items items = map scorings, createItem
    activateAndDisplayItem head items


  apply$ _predicate, (predicate) ->
    pastScorings = (_.getFromCache 'scoring') or _.putIntoCache 'scoring', []
    if predicate.type is 'scoring'
      pastScorings.unshift predicate.scoring

    displayScorings pastScorings

    return

  link$ _.loadScorings, (predicate) ->
    if predicate
      _predicate predicate
    else
      displayActiveItem()

  link$ _.deselectAllScorings, ->
    #TODO ugly
    _isLive no
    for item in _items()
      item.isSelected no
    _isLive yes

  items: _items
  hasItems: _hasItems
  template: 'scoring-list-view'

