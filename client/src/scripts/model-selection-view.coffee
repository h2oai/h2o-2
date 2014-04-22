Steam.ModelSelectionView = (_) ->
  _selections = nodes$ []
  _caption = lift$ _selections, (selections) ->
    "#{describeCount selections.length, 'model'} selected."
  _hasSelection = lift$ _selections, (selections) ->
    selections.length > 0
  _predicate = node$ null
  _frameKey = lift$ _predicate, (predicate) -> if predicate then predicate.frameKey else ''
  _canScore = lift$ _predicate, _selections, (predicate, selections) ->
    if predicate isnt null
      if predicate.type is 'compatibleWithFrame'
        if selections.length > 0
          responseColumn = (head selections).responseColumn
          if (every selections, ((selection) -> selection.responseColumn is responseColumn))
            return yes
    no


  apply$ _hasSelection, (hasSelection) ->
    if hasSelection
      _.modelsSelected()
    else
      _.modelsDeselected()

  createSelection = (model) ->
    data: model
    algorithm: model.model_algorithm
    category: model.model_category
    responseColumn: model.response_column_name

  link$ _.modelSelectionChanged, (isSelected, predicate, model) ->
    if isSelected
      _selections.push createSelection model
    else
      _selections.remove (selection) -> selection.data is model

    _predicate predicate unless _predicate() is predicate

  link$ _.modelSelectionCleared, ->
    _selections.removeAll()
    _predicate null

  scoreModels = ->
    frameKey = _frameKey()
    scores = map _selections(), (selection) ->
      model: selection.data
      status: null
      time: null
      result: null
    scoring = frameKey: frameKey, scores: scores, timestamp: Date.now()

    do cancel
    _.switchToScoring type: 'scoring', scoring: scoring

  cancel = ->
    _selections.removeAll()
    _.deselectAllModels()


  frameKey: _frameKey
  caption: _caption
  hasSelection: _hasSelection
  selections: _selections
  canScore: _canScore
  scoreModels: scoreModels
  cancel: cancel
  template: 'model-selection-view'
