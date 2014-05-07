Steam.ScoringSelectionView = (_) ->
  _selections = do nodes$
  _hasSelection = lift$ _selections, (selections) -> selections.length > 0
  _caption = lift$ _selections, (selections) ->
    "#{describeCount selections.length, 'item'} selected"
  _canCompareScorings = lift$ _selections, (selections) ->
    console.log selections

    if selections.length < 2
      return no

    for selection in selections
      if selection.type is 'comparison'
        return no

    yes

  compareScorings = ->
    _.loadScorings
      type: 'comparison'
      # Send a clone of selections because the selections gets cleared soon after.
      scorings: clone _selections()
      timestamp: Date.now()
    _.deselectAllScorings()

  deleteActiveScoring = ->
    #TODO confirm dialog
    _.deleteActiveScoring()

  deleteScorings = ->
    # Send a clone of selections because the selections gets cleared
    #  when deleted from the selection list.
    #TODO confirm dialog
    _.deleteScorings clone _selections()

  clearSelections = ->
    _.deselectAllScorings()

  link$ _.scoringSelectionChanged, (isSelected, scoring) ->
    if isSelected
      _selections.push scoring
    else
      _selections.remove scoring

  link$ _.scoringSelectionCleared, ->
    _selections.removeAll()

  caption: _caption
  selections: _selections
  hasSelection: _hasSelection
  clearSelections: clearSelections
  canCompareScorings: _canCompareScorings
  compareScorings: compareScorings
  deleteScorings: deleteScorings
  deleteActiveScoring: deleteActiveScoring
  template: 'scoring-selection-view'

