createVariableImportanceChart = (varImp, measureTextWidth, showPareto) ->
  [ div ] = geyser.generate [ 'div' ]
  render = ($element) ->
    $element.empty().append renderVariableImportanceChart varImp, measureTextWidth, showPareto
  markup: div()
  behavior: render

renderVariableImportanceChart = (varImp, measureTextWidth, showPareto) ->
  tickCount = 10
  axisPadding = 15
  barHeight = 20
  columnNames = map varImp.data, (datum) -> datum.columnName

  scaleX = d3.scale.linear()
    .domain [0, 1.1 * d3.max varImp.data, (datum) -> datum.value ] # add 10% to fit in labels properly
    .nice tickCount

  scaleY = d3.scale.ordinal()
    .domain columnNames

  tickFormatX = scaleX.tickFormat tickCount
  axisXHeight = axisPadding + measureTextWidth max (map (scaleX.ticks tickCount), tickFormatX), (tick) -> tick.length

  datumWithLongestLabel = max varImp.data, (datum) -> datum.columnName.length
  axisYWidth = axisPadding + measureTextWidth datumWithLongestLabel.columnName


  margin = top: 20, right: 20, bottom: axisXHeight, left: axisYWidth
  width = 500 - margin.left - margin.right
  height = varImp.data.length * barHeight

  scaleX.range [ 0, width ]
  scaleXPercent = d3.scale.linear()
    .domain [ 0, 110 ] # add 10% to fit in labels properly
    .range [ 0, width ]
  scaleY.rangeRoundBands [ 0, height ], .1
 
  axisX = d3.svg.axis()
    .scale scaleX
    .orient 'bottom'
    .ticks tickCount

  axisY = d3.svg.axis()
    .scale scaleY
    .orient 'left'

  el = document.createElementNS 'http://www.w3.org/2000/svg', 'svg'

  svg = (d3.select el)
    .attr 'width', width + margin.left + margin.right
    .attr 'height', height + margin.top + margin.bottom
    .append 'g'
    .attr 'transform', "translate(#{margin.left},#{margin.top})"

  axis = svg.append 'g'
    .attr 'class', 'x axis'
    .attr 'transform', "translate(0,#{height})"
    .call axisX

    # Rotate labels
  axis.selectAll 'text'
    .style 'text-anchor', 'end'
    .attr 'dx', '-.8em'
    .attr 'dy', '-.5em'
    .attr 'transform', 'rotate(-90)'

    # Append axis title
  axis.append 'text'
    .attr 'x', width
    .attr 'dy', '-.7em'
    .style 'text-anchor', 'end'
    .text varImp.method

  svg.append 'g'
    .attr 'class', 'y axis'
    .call axisY

  svg.selectAll '.bar'
    .data varImp.data
    .enter()
    .append 'rect'
    .attr 'class', 'bar'
    .attr 'x', (d) -> scaleX 0
    .attr 'width', (d) -> scaleX d.value
    .attr 'y', (d) -> scaleY d.columnName
    .attr 'height', scaleY.rangeBand()

  svg.selectAll '.bar-label'
    .data varImp.data
    .enter()
    .append 'text'
    .attr 'class', 'bar-label'
    .attr 'x', (d) -> scaleX d.value
    .attr 'y', (d) -> scaleY d.columnName
    .attr 'dx', '0.2em'
    .attr 'dy', '1.2em'
    .text (d) -> tickFormatX d.value

  if showPareto
    pointX = (d) -> scaleXPercent d.cumulativeScaledValue
    pointY = (d) -> barHeight/2 + scaleY d.columnName

    line = d3.svg.line()
      .x pointX
      .y pointY

    svg.selectAll '.dot'
      .data varImp.data
      .enter()
      .append 'circle'
      .attr 'class', 'dot'
      .attr 'r', 2 
      .attr 'cx', pointX
      .attr 'cy', pointY

    svg.append 'path'
      .datum varImp.data
      .attr 'class', 'curve'
      .attr 'd', line

    svg.selectAll '.dot-label'
      .data varImp.data
      .enter()
      .append 'text'
      .attr 'class', 'dot-label'
      .attr 'x', pointX
      .attr 'y', pointY
      .attr 'dx', '0.25em'
      .attr 'dy', '-0.25em'
      .text (d) -> (Math.round d.cumulativeScaledValue) + '%'

  el

# WARNING: mutates arg
computeCumulativeScaledValues = (varImp) ->
  sum = 0
  for datum in varImp.data
    sum += datum.value 

  cumulativeSum = 0
  for datum in varImp.data
    datum.cumulativeScaledValue = cumulativeSum += (datum.value / sum * 100)

  varImp.hasCumulativeScaledValues = yes

  return

createVariableImportanceModel = (inputColumnNames, variableImportances) ->
  #TODO always refer to .variables when variables are available in variable_importances.
  variables = variableImportances.variables or inputColumnNames

  data = times variableImportances.varimp.length, (index) ->
    columnName: variables[index] or '' #TODO remove or... when variables are available in variable_importances
    value: variableImportances.varimp[index]
    cumulativeScaledValue: 0
    hasCumulativeScaledValues: no
  
  sortedData = sortBy data, (datum) -> -datum.value

  method: variableImportances.method
  data: if sortedData.length < variableImportances.max_var then sortedData else sortedData.slice 0, variableImportances.max_var

Steam.ModelView = (_, _model) ->
  stringify = (value) ->
    if isArray value
      join value, ', '
    else
      value

  kv = (key, value) -> key: key, value: stringify value

  collateSummary = (model) ->
    [
      kv 'Response Column', model.response_column_name
      kv 'Model Category', model.model_category
      #TODO uncomment when this is functional
      # kv 'State', model.state
    ]

  collateParameters = (model) ->
    parameters = [
      pairs model.critical_parameters
      pairs model.secondary_parameters
      pairs model.expert_parameters
    ]
    map (flatten parameters, yes), ([key, value]) -> kv key, value
  
  collateCompatibleFrames = (frames) ->
    map frames, (frame) ->
      frameKey: frame.key
      columns: join frame.column_names, ', '
      inspect: -> _.inspect Steam.FrameInspectionView _, frame

  # PP-74 hide raw frames from list
  _nonRawFrames = filter _model.compatible_frames, (frame) -> not frame.is_raw_frame
  _compatibleFrames = collateCompatibleFrames _nonRawFrames
  _compatibleFramesCount = "(#{_nonRawFrames.length})"

  _hasVariableImportance = isTruthy _model.variable_importances
  if _hasVariableImportance
    _variableImportances = createVariableImportanceModel _model.input_column_names, _model.variable_importances
    _showParetoCurve = node$ no
    _variableImportanceChart = lift$ _showParetoCurve, (showParetoCurve) ->
      if showParetoCurve
        computeCumulativeScaledValues _variableImportances unless _variableImportances.hasCumulativeScaledValues
        createVariableImportanceChart _variableImportances, _.measureTextWidth, yes
      else
        createVariableImportanceChart _variableImportances, _.measureTextWidth, no

  loadCompatibleFrames = ->
    _.switchToFrames type: 'compatibleWithModel', modelKey: _model.key
  
  data: _model
  key: _model.key
  timestamp: _model.creation_epoch_time_millis
  summary: collateSummary _model
  parameters: collateParameters _model
  inputColumns: _model.input_column_names
  inputColumnsCount: "(#{_model.input_column_names.length})"
  compatibleFrames: _compatibleFrames
  compatibleFramesCount: _compatibleFramesCount
  loadCompatibleFrames: loadCompatibleFrames
  hasVariableImportance: _hasVariableImportance
  variableImportanceChart: _variableImportanceChart
  showParetoCurve: _showParetoCurve
  template: 'model-view'

