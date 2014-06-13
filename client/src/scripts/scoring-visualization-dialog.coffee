Steam.ScoringVisualizationDialog = (_, _title, _parameters,  _go) ->
  _visualizationType = node$ _parameters.visualizationType
  _axisXVariable = node$ _parameters.variableX
  _axisYVariable = node$ _parameters.variableY
  _canConfirm = lift$ _axisXVariable, _axisYVariable, (x, y) -> x and y 
  _variables = lift$ _visualizationType, (visualizationType) -> visualizationType.variables

  confirm = ->
    _go 'confirm',
      visualizationType: _visualizationType()
      variableX: _axisXVariable()
      variableY: _axisYVariable()

  cancel = -> _go 'cancel'

  title: _title
  visualizationTypes: _parameters.visualizationTypes
  visualizationType: _visualizationType
  variables: _variables
  variableX: _axisXVariable
  variableY: _axisYVariable
  canConfirm: _canConfirm
  confirm: confirm
  cancel: cancel
  template: 'scoring-visualization-dialog'
