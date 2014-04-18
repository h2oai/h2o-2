Steam.ModelSelectionView = (_) ->
  _selections = nodes$ []
  _caption = lift$ _selections, (selections) ->
    "#{describeCount selections.length, 'model'} selected."
  _hasSelection = lift$ _selections, (selections) ->
    selections.length > 0
  _predicate = node$ null
  _canScore = lift$ _hasSelection, _predicate, (hasSelection, predicate) ->
    hasSelection and predicate isnt null and predicate.type is 'compatibleWithFrame'

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
    status: node$ '-'
    time: node$ '-'
    result: node$ null

  link$ _.modelSelectionChanged, (isSelected, predicate, model) ->
    if isSelected
      _selections.push createSelection model
    else
      _selections.remove (selection) -> selection.data is model

    _predicate predicate unless _predicate() is predicate

  scoreModels = ->
    frameKey = _predicate().frameKey
    forEach _selections(), (selection) ->
      modelKey = selection.data.key
      selection.status 'running'
      _.requestScoringOnFrame frameKey, modelKey, (error, result) ->
        data = if error then error.data else result
        selection.status data.response.status
        selection.time data.response.time
        selection.result error or result

  clearSelected = ->
    _selections []
    _.deselectAllModels()


  caption: _caption
  hasSelection: _hasSelection
  selections: _selections
  canScore: _canScore
  scoreModels: scoreModels
  clearSelected: clearSelected
