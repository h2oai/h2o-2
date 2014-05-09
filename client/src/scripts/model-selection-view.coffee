Steam.ModelSelectionView = (_) ->
  _selections = nodes$ []
  _hasSelection = lift$ _selections, (selections) -> selections.length > 0
  _caption = lift$ _selections, (selections) ->
    "#{describeCount selections.length, 'model'} selected."

  scoreSelections = ->
    _.promptForFrame (action, frameKey) ->
      switch action
        when 'confirm'
          scorings = map _selections(), (selection) ->
            frameKey: frameKey
            model: selection.data
            status: null
            time: null
            result: null
            timestamp: Date.now()

          _.switchToScoring type: 'scoring', scorings: scorings
          _.deselectAllModels()
        when 'error'
          _.fail 'Error', 'An error occured while fetching the list of datasets.', error, noop

  clearSelections = ->
    _.deselectAllModels()

  link$ _.modelSelectionChanged, (isSelected, model) ->
    if isSelected
      _selections.push model
    else
      _selections.remove model

  link$ _.modelSelectionCleared, ->
    _selections.removeAll()

  caption: _caption
  hasSelection: _hasSelection
  clearSelections: clearSelections
  scoreSelections: scoreSelections
  template: 'model-selection-view'
  
