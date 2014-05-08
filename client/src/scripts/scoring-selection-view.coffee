Steam.ScoringSelectionView = (_) ->
  _selections = do nodes$
  _hasSelection = lift$ _selections, (selections) -> selections.length > 0
  _caption = lift$ _selections, (selections) ->
    "#{describeCount selections.length, 'item'} selected"

  defaultScoringComparisonMessage = 'Compare selected scorings.'
  _scoringComparisonMessage = lift$ _selections, (selections) ->
    return 'Select two or more scorings to compare.' if selections.length < 2
    return 'Remove comparison tables from your selection.' if some selections, (selection) -> selection.type is 'comparison'
    return 'Ensure that all selected scorings refer to conforming datasets.' unless valuesAreEqual selections, (selection) -> selection.data.input.frameKey
    return 'Ensure that all selected scorings belong to the same model category.' unless valuesAreEqual selections, (selection) -> selection.data.input.model.model_category
    return 'Ensure that all selected scorings refer to the same response column.' unless valuesAreEqual selections, (selection) -> selection.data.input.model.response_column_name

    # TODO is the following rule valid?
    # return 'Ensure that all selected scorings refer to the same input columns' unless valuesAreEqual selections, (selection) -> selection.data.input.model.input_column_names.join '\0'
    defaultScoringComparisonMessage

  _canCompareScorings = lift$ _scoringComparisonMessage, (message) -> message is defaultScoringComparisonMessage

  compareScorings = ->
    _.loadScorings
      type: 'comparison'
      # Send a clone of selections because the selections gets cleared soon after.
      scorings: clone _selections()
      timestamp: Date.now()
    _.deselectAllScorings()

  tryCompareScorings = (hover) ->
    _.status if hover then _scoringComparisonMessage() else null

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
  tryCompareScorings: tryCompareScorings
  compareScorings: compareScorings
  deleteScorings: deleteScorings
  deleteActiveScoring: deleteActiveScoring
  template: 'scoring-selection-view'

