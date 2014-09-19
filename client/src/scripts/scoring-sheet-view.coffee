unless exports?
  format4f = d3.format '.4f' 
  formatInteger = d3.format 'd'
else
  format4f = null
  formatInteger = null

computeTPR = (cm) ->
  [[tn, fp], [fn, tp]] = cm
  tp / (tp + fn)

computeFPR = (cm) ->
  [[tn, fp], [fn, tp]] = cm
  fp / (fp + tn)

isNumericVariable = (variable) -> variable.type is 'float' or variable.type is 'integer'

getSortedNumericVariables = (variables) ->
  sortBy (filter variables, isNumericVariable), (variable) -> variable.caption


metricCriteriaVariable =
  id: uniqueId()
  name: 'metricCriteria'
  caption: 'Metric Criteria'
  type: 'string'
  format: identity
  domain: [
    value: 'maximum F1'
    caption: 'Max F1'
    isImportant: yes
  ,
    value: 'maximum F2'
    caption: 'Max F2'
    isImportant: no
  ,
    value: 'maximum F0point5'
    caption: 'Max F0.5'
    isImportant: no
  ,
    value: 'maximum Accuracy'
    caption: 'Max Accuracy'
    isImportant: no
  ,
    value: 'maximum Precision'
    caption: 'Max Precision'
    isImportant: no
  ,
    value: 'maximum Recall'
    caption: 'Max Recall'
    isImportant: no
  ,
    value: 'maximum Specificity'
    caption: 'Max Specificity'
    isImportant: no
  ,
    value: 'maximum absolute MCC'
    caption: 'Max Absolute MCC'
    isImportant: no
  ,
    value: 'minimizing max per class Error'
    caption: 'Min MPCE'
    isImportant: no
  ]

metricTypeVariable =
  id: uniqueId()
  name: 'metricType'
  caption: 'Metric Type'
  type: 'string'
  format: identity
  domain: [
    value: 'threshold_for_criteria'
    caption: 'Threshold'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'error_for_criteria'
    caption: 'Error'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'F0point5_for_criteria'
    caption: 'F0.5'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'F1_for_criteria'
    caption: 'F1'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'F2_for_criteria'
    caption: 'F2'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'accuracy_for_criteria'
    caption: 'Accuracy'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'precision_for_criteria'
    caption: 'Precision'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'recall_for_criteria'
    caption: 'Recall'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'specificity_for_criteria'
    caption: 'Specificity'
    domain: [0, 1]
    isImportant: yes
  ,
    value: 'mcc_for_criteria'
    caption: 'MCC'
    domain: [-1, 1]
    isImportant: yes
  ,
    value: 'max_per_class_error_for_criteria'
    caption: 'MPCE'
    domain: [0, 1]
    isImportant: yes
  ]

scoringVariable = 
  id: uniqueId()
  name: 'scoring'
  caption: 'Scoring'
  type: 'string'
  read: (metric) -> metric.caption
  domain: []
  format: identity

inputsVariable =
  id: uniqueId()
  name: 'inputs'
  caption: 'Inputs'
  type: 'string'
  read: (metric) -> 'NA'
  domain: []
  format: identity

metricVariables = []

metricVariables.push
  id: uniqueId()
  name: 'method'
  caption: 'Method'
  type: 'string'
  read: (metric) -> metric.model.model_algorithm
  format: identity
  domain: [ 0, 1 ]
  extent: []

metricVariables.push
  id: uniqueId()
  name: 'auc'
  caption: 'AUC'
  type: 'float'
  read: (metric) -> +metric.data.auc.AUC
  format: format4f
  domain: [ 0, 1 ]
  extent: []

metricVariables.push
  id: uniqueId()
  name: 'gini'
  caption: 'Gini'
  type: 'float'
  read: (metric) -> +metric.data.auc.Gini
  format: format4f
  domain: [ 0, 1 ]
  extent: []

metricVariables.push
  id: uniqueId()
  name: 'trainingTime'
  caption: 'Training Time (ms)'
  type: 'integer'
  read: (metric) -> metric.model.training_duration_in_ms
  format: formatInteger
  domain: [ 0, 1 ]
  extent: []

metricVariables.push
  id: uniqueId()
  name: 'scoringTime'
  caption: 'Scoring Time (ms)'
  type: 'integer'
  read: (metric) -> metric.data.duration_in_ms
  format: formatInteger
  domain: [ 0, 1 ]
  extent: []


forEach metricCriteriaVariable.domain, (metricCriterion, metricCriterionIndex) ->
  forEach metricTypeVariable.domain, (metricType) -> 
    metricVariables.push
      id: uniqueId()
      name: "#{metricCriterion.value}-#{metricType.value}"
      caption: "#{metricType.caption} (#{metricCriterion.caption})"
      type: 'float'
      read: (metric) -> +metric.data.auc[metricType.value][metricCriterionIndex]
      format: format4f
      domain: [ 0, 1 ]
      extent: []
      meta:
        metricType: metricType
        metricCriterion: metricCriterion
        metricCriterionIndex: metricCriterionIndex

metricVariables.push scoringVariable

metricVariables.push
  id: uniqueId()
  name: 'frameKey'
  caption: 'Frame'
  type: 'string'
  read: (metric) -> metric.data.frame.key
  format: identity
  domain: [ 0, 1 ]
  extent: []

metricVariables.push
  id: uniqueId()
  name: 'modelKey'
  caption: 'Model'
  type: 'string'
  read: (metric) -> metric.model.key
  format: identity
  domain: [ 0, 1 ]
  extent: []

metricVariablesIndex = indexBy metricVariables, (variable) -> variable.name

thresholdVariables = [
  name: 'threshold'
  caption: 'Threshold'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.thresholds[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'error'
  caption: 'Error'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.errorr[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'f0.5'
  caption: 'F0.5'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.F0point5[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'f1'
  caption: 'F1'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.F1[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'f2'
  caption: 'F2'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.F2[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'accuracy'
  caption: 'Accuracy'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.accuracy[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'precision'
  caption: 'Precision'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.precision[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'recall'
  caption: 'Recall'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.recall[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'specificity'
  caption: 'Specificity'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.specificity[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'mcc'
  caption: 'MCC'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.mcc[index]
  format: format4f
  domain: [ -1, 1 ]
  extent: []
,
  name: 'mpce'
  caption: 'MPCE'
  type: 'float'
  read: (metric, index) -> +metric.data.auc.max_per_class_error[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'cm'
  caption: 'Confusion Matrix'
  type: 'blob'
  read: (metric, index) -> 
    domain: metric.data.auc.actual_domain
    cm: metric.data.auc.confusion_matrices[index]
  format: (blob) ->
    [ grid, tr, th, td ] = geyser.generate words 'table.table.table-bordered tr th td'
    [ d1, d2 ] = blob.domain
    [[tn, fp], [fn, tp]] = blob.cm
    grid [
      tr [
        th ''
        th d1
        th d2
      ]
      tr [
        th d1
        td tn
        td fp
      ]
      tr [
        th d2
        td fn
        td tp
      ]
    ]
  domain: null
  extent: []
,
  name: 'tpr'
  caption: 'TPR'
  type: 'float'
  read: (metric, index) -> computeTPR metric.data.auc.confusion_matrices[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
,
  name: 'fpr'
  caption: 'FPR'
  type: 'float'
  read: (metric, index) -> computeFPR metric.data.auc.confusion_matrices[index]
  format: format4f
  domain: [ 0, 1 ]
  extent: []
]

thresholdVariablesIndex = indexBy thresholdVariables, (variable) -> variable.name

createInputVariable = (algorithm, key, caption, type, inputType, domain, hasDifferences) ->
  read = (metric) ->
    if metric.model.model_algorithm is algorithm
      switch inputType
        when 'critical'
          metric.model.critical_parameters[key]
        when 'secondary'
          metric.model.secondary_parameters[key]
        when 'expert'
          metric.model.expert_parameters[key]
    else
      undefined

  extent =
    if isArray domain
      if domain.length is 2
        if domain[0] is domain[1]
          [ domain[0] - 1, domain[1] + 1 ]
        else
          domain
      else
        [0, 1]
    else
      [0, 1]

  format = (value) ->
    if isUndefined value
      '-'
    else
      switch type
        when 'integer'
          formatInteger value
        when 'float'
          format4f value
        when 'string'
          value
        when 'array'
          value.join ', '
        else
          JSON.stringify value
      
  id: uniqueId()
  name: caption
  caption: caption
  type: type
  read: read
  format: format
  domain: domain
  extent: extent
  meta:
    inputType: inputType
    hasDifferences: hasDifferences

createInputParameterAndValue2 = (algorithm, type, key, value) ->
  key: key
  algorithm: algorithm
  type: type
  value: value

createInputParameterAndValue = (value, key) ->
  # DL, DRF have array-valued params, so handle that case properly
  scalarValue = if isArray value then value.join ', ' else value
  key: key
  value: scalarValue
  isNumber: isNumber value

determineType = (value) ->
  if isNumber value
    'float'
  else if isString value
    'string'
  else if isArray value
    'array'
  else
    'blob'

createInputVariables = (inputParametersByCaption) ->
  variables = []
  for caption, inputs of inputParametersByCaption
    type = null
    key = null
    algorithm = null
    inputType = null
    valid = no
    for input in inputs
      if type is null
        type = determineType input.value
        key = input.key
        algorithm = input.algorithm
        inputType = input.type
        valid = yes
      else
        unless type is determineType input.value
          valid = no
          continue
    if valid
      switch type
        when 'float'
          domain = d3.extent inputs, (input) -> input.value
          type = 'integer' if (every inputs, (input) -> isInteger input.value)
        when 'string'
          domain = unique map inputs, (input) -> input.value
        else
          domain = null
      hasDifferences = domain is null or (domain isnt null and not same domain)
      variables.push createInputVariable algorithm, key, caption, type, inputType, domain, hasDifferences

  sortBy variables, (variable) -> variable.caption

collateInputVariables = (metrics) ->
  # Collate critcal, secondary, expert parameters for all models
  inputParameters = map metrics, (metric) ->
    model = metric.model
    flatten [
      mapWithKey model.critical_parameters, (value, key) -> createInputParameterAndValue2 model.model_algorithm, 'critical', key, value
      mapWithKey model.secondary_parameters, (value, key) -> createInputParameterAndValue2 model.model_algorithm, 'secondary', key, value
      mapWithKey model.expert_parameters, (value, key) -> createInputParameterAndValue2 model.model_algorithm, 'expert', key, value
    ]
  inputParametersByKey = groupBy (flatten inputParameters), (parameter) -> "#{parameter.algorithm}.#{parameter.key}"
  createInputVariables inputParametersByKey

collateInputParameters = (metrics) ->
  models = map metrics, (metric) -> metric.model

  # Collate critcal, secondary, expert parameters for all models
  parametersByModel = map models, (model) ->
    flatten [
      mapWithKey model.critical_parameters, createInputParameterAndValue
      mapWithKey model.secondary_parameters, createInputParameterAndValue
      mapWithKey model.expert_parameters, createInputParameterAndValue
    ]

  # Filter only numeric parameters
  numericParametersByModel = map parametersByModel, (parameters) -> filter parameters, (parameter) -> parameter.isNumber

  # Find parameters that have numeric values defined for all models.
  # This is to reject any parameters that have numeric values for some models and non-numeric ones for the others (in which case the number of parameter values will not match the number of models).
  parametersByKey = groupBy (flatten numericParametersByModel), (parameter) -> parameter.key
  plottableParameterKeys = []
  for key, parameters of parametersByKey
    if parameters.length is models.length
      plottableParameterKeys.push key

  # Create a dictionary of input parameters per metrics
  inputParameters = times models.length, -> {}
  for key in plottableParameterKeys
    for parameter, i in parametersByKey[key]
      inputParameters[i][key] = parameter.value

  forEach metrics, (metric, i) -> metric.inputs = inputParameters[i]

  [plottableParameterKeys, inputParameters]

createMetricFrameFromScorings = (scores) ->
  uniqueScoringNames = {}
  createUniqueScoringName = (frameKey, modelKey) ->
    name = "#{modelKey} on #{frameKey}"
    if index = uniqueScoringNames[name]
      uniqueScoringNames[name] = index++
      name += ' #' + index
    else
      uniqueScoringNames[name] = 1
    name

  # Go for higher contrast when comparing fewer scorings.
  palette = if scores.length > 10 then d3.scale.category20 else d3.scale.category10
  colorScale = palette().domain d3.range scores.length

  metrics = map scores, (score, index) ->
    model = score.data.input.model
    metric = head score.data.output.metrics

    id: index
    caption: createUniqueScoringName metric.frame.key, metric.model.key
    model: model
    data: metric
    color: colorScale index

  for variable in metricVariables when isNumericVariable variable
    extent = d3.extent metrics, variable.read
    [ l, u ] = extent
    variable.extent = if (isNumber l) and (isNumber u) and l < u then extent else variable.domain

  for variable in thresholdVariables when isNumericVariable variable
    extents = map metrics, (metric) -> d3.extent times metric.data.auc.thresholds.length, (index) -> variable.read metric, index
    l = d3.min extents, (extent) -> head extent
    u = d3.max extents, (extent) -> last extent
    variable.extent = if (isNumber l) and (isNumber u) and l < u then [l, u] else variable.domain

  inputVariables = collateInputVariables metrics

  metrics: metrics
  inputVariables: inputVariables
  metricVariables: metricVariables
  thresholdVariables: thresholdVariables

createMetricsVisualization = (metrics, variableX, variableY, inspect) ->
  [ div ] = geyser.generate [ 'div' ]
  render = ($element) ->
    plot = renderMetricsVisualization metrics, variableX, variableY, inspect
    $element.empty().append plot
  markup: div()
  behavior: render

createThresholdVisualization = (metrics, variableX, variableY, showReferenceLine, inspect) ->
  [ div ] = geyser.generate [ 'div' ]
  render = ($element) ->
    plot = renderThresholdVisualization metrics, variableX, variableY, showReferenceLine, inspect
    $element.empty().append plot
  markup: div()
  behavior: render

createInspectionColorSwatch = ->
  pop geyser.generate [ "div style='display:inline-block;width:12px;height:12px;margin-right:7px;background-color:$color'" ]

createMetricInspection = (variables, metric) ->
  [ div, h1, h2, table, tbody, tr, th, td, span ] = geyser.generate words 'div h1 h2 table.table.table-condensed tbody tr th td span'
  swatch = createInspectionColorSwatch()

  div [
    h1  [ 
      swatch '', $color:metric.color
      span metric.caption
    ]
    table tbody map variables, (variable) ->
      value = variable.read metric
      tr [
        th variable.caption
        td if isNaN value then 'NaN' else variable.format value
      ]
  ]

createThresholdInspection = (variables, metric, index) ->
  [ div, h1, h2, table, grid, tbody, tr, th, td, span ] = geyser.generate words 'div h1 h2 table.table.table-condensed table.table.table-bordered tbody tr th td span'
  swatch = createInspectionColorSwatch()

  tabulateProperties = (index) ->
    table tbody map variables, (variable) ->
      tr [
        th variable.caption
        td variable.format variable.read metric, index
      ]

  div [
    h1  [ 
      swatch '', $color:metric.color
      span metric.caption
    ]
    h2 'Outputs'
    tabulateProperties index
  ]

renderMetricsVisualization = (metrics, variableX, variableY, inspect) ->
  margin = top: 20, right: 20, bottom: 30, left: 30
  width = 200
  height = 200

  readX = (metric) ->
    variableX.read metric

  readY = (metric) ->
    variableY.read metric

  scaleX = d3.scale.linear()
    .domain variableX.extent
    .range [ 0, width ]
    .nice()

  scaleY = d3.scale.linear()
    .domain variableY.extent
    .range [ height, 0 ]
    .nice()

  axisX = d3.svg.axis()
    .scale scaleX
    .orient 'bottom'
    .ticks 5

  axisY = d3.svg.axis()
    .scale scaleY
    .orient 'left'
    .ticks 5

  line = d3.svg.line()
    .x (d) -> scaleX readX d
    .y (d) -> scaleY readY d

  el = document.createElementNS 'http://www.w3.org/2000/svg', 'svg'

  svg = (d3.select el)
    .attr 'class', 'y-metrics-plot'
    .attr 'width', width + margin.left + margin.right
    .attr 'height', height + margin.top + margin.bottom
    .append 'g'
    .attr 'transform', "translate(#{margin.left},#{margin.top})"

  svg.append 'g'
    .attr 'class', 'x axis'
    .attr 'transform', "translate(0, #{height})"
    .call axisX
    .append 'text'
    .attr 'x', width
    .attr 'y', -6
    .style 'text-anchor', 'end'
    .text variableX.caption

  svg.append 'g'
    .attr 'class', 'y axis'
    .call axisY
    .append 'text'
    .attr 'transform', 'rotate(-90)'
    .attr 'y', 6
    .attr 'dy', '.71em'
    .style 'text-anchor', 'end'
    .text variableY.caption

  svg.selectAll '.dot'
    .data metrics
    .enter()
    .append 'circle'
    .attr 'class', 'dot'
    .attr 'r', 5
    .attr 'cx', (d) -> scaleX readX d
    .attr 'cy', (d) -> scaleY readY d
    .attr 'stroke', (d) -> d.color
    .on 'click', (d) -> inspect d
    .append 'title'
    .text (d) -> d.caption

  el

renderThresholdVisualization = (metrics, variableX, variableY, showReferenceLine, inspect) ->
  margin = top: 20, right: 20, bottom: 20, left: 30
  width = 200
  height = 200

  validIndices = zipObject map metrics, (metric) ->
    indices = filter (range metric.data.auc.thresholds.length), (index) ->
      x = variableX.read metric, index
      y = variableY.read metric, index
      (not isNaN x) and (not isNaN y)
    [ metric.id, indices ]

  scaleX = d3.scale.linear()
    .domain variableX.extent
    .range [ 0, width ]

  scaleY = d3.scale.linear()
    .domain variableY.extent
    .range [ height, 0 ]

  axisX = d3.svg.axis()
    .scale scaleX
    .orient 'bottom'
    .ticks 5

  axisY = d3.svg.axis()
    .scale scaleY
    .orient 'left'
    .ticks 5

  line = (metric) -> 
    d3.svg.line()
      .x (index) -> scaleX variableX.read metric, index
      .y (index) -> scaleY variableY.read metric, index

  el = document.createElementNS 'http://www.w3.org/2000/svg', 'svg'

  svg = (d3.select el)
    .attr 'class', 'y-custom-plot'
    .attr 'width', width + margin.left + margin.right
    .attr 'height', height + margin.top + margin.bottom
    .append 'g'
    .attr 'transform', "translate(#{margin.left},#{margin.top})"

  svg.append 'g'
    .attr 'class', 'x axis'
    .attr 'transform', "translate(0, #{height})"
    .call axisX
    .append 'text'
    .attr 'x', width
    .attr 'y', -6
    .style 'text-anchor', 'end'
    .text variableX.caption

  svg.append 'g'
    .attr 'class', 'y axis'
    .call axisY
    .append 'text'
    .attr 'transform', 'rotate(-90)'
    .attr 'y', 6
    .attr 'dy', '.71em'
    .style 'text-anchor', 'end'
    .text variableY.caption

  if showReferenceLine
    svg.append 'line'
      .attr 'class', 'guide'
      .attr 'stroke-dasharray', '3,3'
      .attr
        x1: scaleX 0
        y1: scaleY 0
        x2: scaleX 1
        y2: scaleY 1

  curve = svg.selectAll '.y-curve'
    .data metrics
    .enter()
    .append 'g'
    .attr 'class', 'y-curve'

  curve.append 'path'
    .attr 'id', (metric) -> "curve#{metric.id}"
    .attr 'class', 'line'
    .attr 'd', (metric) -> line(metric) validIndices[metric.id]
    .style 'stroke', (metric) -> metric.color

  forEach metrics, (metric) ->
    svg.append 'g'
      .selectAll '.dot'
      .data validIndices[metric.id]
      .enter()
      .append 'circle'
      .attr 'class', 'dot'
      .attr 'r', 5
      .attr 'cx', (index) -> scaleX variableX.read metric, index
      .attr 'cy', (index) -> scaleY variableY.read metric, index
      .on 'click', (index) -> inspect metric, index
      .on 'mouseover', (index) ->
        d3.select(@).style 'stroke', metric.color
      .on 'mouseout', (index) ->
        d3.select(@).style 'stroke', 'none'
      .append 'title'
      .text metric.caption
  el

createFilter = (variable) ->
  items = map variable.domain, (factor) ->
    factor: factor
    isSelected: factor.isImportant
  predicate = {}
  for item in items
    predicate[item.factor.value] = item.isSelected

  variable: variable
  items: items
  predicate: predicate

Steam.ScoringSheetView = (_, _scorings) ->
  _metricFrame = null
  _metricTable = node$ null
  _sortByVariable = metricVariablesIndex.auc
  _sortAscending = no
  _visualizations = nodes$ []

  #_filtersView = null
  #_filtersIndex = null

  _metricsVisualizationType = null
  _thresholdVisualizationType = null
  _visualizationTypes = null

  _filteredMetricVariables = null
  _filteredInputVariables = null
  _filteredMetrics = null
  _scoringFilter = null
  _inputsFilter = null
  _metricTypeFilter = null
  _metricCriteriaFilter = null
  _allFilters = null

  initialize = (scorings) ->
    _metricFrame = createMetricFrameFromScorings scorings
    scoringVariable.domain = map _metricFrame.metrics, (metric) ->
      value: metric.id
      caption: metric.caption
      isImportant: yes

    inputsVariable.domain = map _metricFrame.inputVariables, (variable) ->
      value: variable.id
      caption: variable.caption
      isImportant: (variable.meta.inputType is 'critical') or (variable.meta.hasDifferences)

    areModelsComparable = same _metricFrame.metrics, (a, b) -> a.model.model_category is b.model.model_category and a.model.model_algorithm is b.model.model_algorithm
    _metricsVisualizationType =
      type: 'scoring'
      caption: 'Scoring'
      variables: getSortedNumericVariables if areModelsComparable then (_metricFrame.inputVariables.concat metricVariables) else metricVariables

    _thresholdVisualizationType =
      type: 'threshold'
      caption: 'Threshold'
      variables: getSortedNumericVariables thresholdVariables

    _visualizationTypes = [ _metricsVisualizationType, _thresholdVisualizationType ]

    _scoringFilter = createFilter scoringVariable
    _inputsFilter = createFilter inputsVariable
    _metricTypeFilter = createFilter metricTypeVariable
    _metricCriteriaFilter = createFilter metricCriteriaVariable
    _allFilters = [ _scoringFilter, _inputsFilter, _metricTypeFilter, _metricCriteriaFilter ]
    updateFiltering()
    _metricTable createMetricTable() 
    _visualizations.push createThresholdVisualizationPane thresholdVariablesIndex.fpr, thresholdVariablesIndex.tpr, yes

  updateFiltering = ->
    _filteredMetricVariables = filter _metricFrame.metricVariables, (variable) ->
      if variable.meta
        if _metricTypeFilter.predicate[variable.meta.metricType.value] and _metricCriteriaFilter.predicate[variable.meta.metricCriterion.value]
          yes
        else
          no
      else
        yes
    _filteredInputVariables = filter _metricFrame.inputVariables, (variable) -> _inputsFilter.predicate[variable.id] 
    _filteredMetrics = filter _metricFrame.metrics, (metric) -> _scoringFilter.predicate[metric.id]

  invalidate = ->
    _metricTable createMetricTable()
    newVisualizations = []
    forEach _visualizations(), (oldVisualization) ->
      { type, variableX, variableY } = oldVisualization.data
      createVisualizationPane type, variableX, variableY, (visualization) ->
        newVisualizations.push visualization
    _visualizations newVisualizations

  createMetricTable = ->
    [ div, table, thead, tbody, tr, th, thAsc, thDesc, td ] = geyser.generate words 'div table.table.table-condensed thead tbody tr th th.y-sorted-asc th.y-sorted-desc td'
    [ columnHeader, scoringLink, swatch, checkbox, selectAllCheckbox, filterButton, filterOutButton] = geyser.generate [ "a.y-header data-variable-id='$id'", "a.y-scoring-link data-scoring-id='$id'", ".y-legend-swatch style='background-color:$color'", "input.y-select-one-checkbox type='checkbox' data-scoring-id='$id'", "input.y-select-all-checkbox type='checkbox'", 'button.btn.y-filter-button', "button.btn.y-filter-out-button style='margin-left:7px'"]

    # Sort
    _filteredMetrics = sortBy _filteredMetrics, (metric) -> _sortByVariable.read metric
    _filteredMetrics.reverse() unless _sortAscending

    columnVariables = clone _filteredMetricVariables
    columnVariables.splice.apply columnVariables, flatten [ columnVariables.length - 3, 0, _filteredInputVariables ]
    
    headers = map columnVariables, (variable) ->
      tag = if variable isnt _sortByVariable then th else if _sortAscending then thAsc else thDesc
      tag columnHeader variable.caption, $id: variable.id

    # Additional column to house legend swatches
    headers.unshift th '&nbsp;'
    headers.unshift th selectAllCheckbox ''

    rows = map _filteredMetrics, (metric) ->
      cells = map columnVariables, (variable) ->
        if variable.name is 'method' or variable.name is 'scoring'
          td scoringLink (variable.read metric), $id: metric.id
        else
          td variable.format variable.read metric
      # Add legend swatch 
      cells.unshift td swatch '', $color:metric.color

      # Add checkbox
      cells.unshift td checkbox '', $id: metric.id
      cells

    markup = div [
      table [
        thead tr headers
        tbody map rows, tr
      ]
      div [
        filterButton 'Edit this table&hellip;'
        filterOutButton 'Remove selected'
      ]
    ]

    behavior = ($element) ->
      $('a.y-header', $element).each ->
        $anchor = $ @
        $anchor.click ->
          sortById = $anchor.attr 'data-variable-id'
          _sortByVariable = find columnVariables, (variable) -> variable.id is sortById
          _sortAscending = $anchor.parents('th').hasClass 'y-sorted-desc'
          invalidate()
      $('a.y-scoring-link', $element).each ->
        $anchor = $ @
        $anchor.click ->
          metricId = parseInt $anchor.attr 'data-scoring-id'
          metric = find _metricFrame.metrics, (metric) -> metric.id is metricId
          if metric
            _.inspect
              content: createMetricInspection _metricFrame.metricVariables, metric
              template: 'geyser'
      $('.y-select-all-checkbox', $element).change ->
        $checkbox = $ @
        $('.y-select-one-checkbox', $element).prop 'checked', $checkbox.is ':checked'

      $('.y-filter-button', $element).click -> displayFilters()

      $('.y-filter-out-button', $element).click ->
        scoringFilterPredicate = {}
        $('.y-select-one-checkbox', $element).each ->
          $checkbox = $ @
          metricId = parseInt $checkbox.attr 'data-scoring-id'
          scoringFilterPredicate[metricId] = not $checkbox.is ':checked'
          return
        applyFilter _scoringFilter, scoringFilterPredicate
        updateFiltering()
        invalidate()

    markup: markup
    behavior: behavior

  applyFilter = (filter, predicate) ->
    filter.predicate = predicate
    for item in filter.items
      item.isSelected = predicate[item.factor.value]
    return
  

  displayFilters = ->
    _.filterScorings _allFilters, (action, predicates) ->
      switch action
        when 'confirm'
          for predicate, i in predicates
            applyFilter _allFilters[i], predicate
          updateFiltering()
          invalidate()

  confirmVisualizationDeletion = (go) ->
    confirmDialogOpts =
      title: 'Delete Visualization?'
      confirmCaption: 'Delete'
      cancelCaption: 'Keep'
    _.confirm 'This visualization will be deleted. Are you sure?', confirmDialogOpts, (response) ->
      go() if response is 'confirm'

  replaceVisualization = (oldVisualization, newVisualization) ->
    index = _visualizations.indexOf oldVisualization
    if index >= 0
      _visualizations.splice index, 1, newVisualization
    return

  createMetricsVisualizationPane = (variableX, variableY) ->
    rendering =  createMetricsVisualization _filteredMetrics, variableX, variableY, (metric) ->
      _.inspect
        content: createMetricInspection _metricFrame.metricVariables, metric
        template: 'geyser'

    edit = ->
      configureVisualization 'Edit Visualization', self.data.type, self.data.variableX, self.data.variableY, (visualization) -> replaceVisualization self, visualization

    remove = ->
      confirmVisualizationDeletion -> _visualizations.remove self

    self =
      caption: "#{variableX.caption} vs #{variableY.caption}"
      data:
        type: _metricsVisualizationType
        variableX: variableX
        variableY: variableY
      rendering: rendering
      edit: edit
      remove: remove

    self

  createThresholdVisualizationPane = (variableX, variableY, showReferenceLine) ->
    rendering =  createThresholdVisualization _filteredMetrics, variableX, variableY, showReferenceLine, (metric, index) ->
      _.inspect
        content: createThresholdInspection _metricFrame.thresholdVariables, metric, index
        template: 'geyser'

    edit = ->
      configureVisualization 'Edit Visualization', self.data.type, self.data.variableX, self.data.variableY, (visualization) -> replaceVisualization self, visualization
    remove = ->
      confirmVisualizationDeletion -> _visualizations.remove self

    self =
      caption: if variableX is thresholdVariablesIndex.fpr and variableY is thresholdVariablesIndex.tpr then 'ROC Chart' else "#{variableX.caption} vs #{variableY.caption}"
      data:
        type: _thresholdVisualizationType
        variableX: variableX
        variableY: variableY
      rendering: rendering
      edit: edit
      remove: remove

    self

  createVisualizationPane = (visualizationType, variableX, variableY, go) ->
    switch visualizationType
      when _metricsVisualizationType
        go createMetricsVisualizationPane variableX, variableY
      when _thresholdVisualizationType
        go createThresholdVisualizationPane variableX, variableY, no

  configureVisualization = (caption, type, variableX, variableY, go) ->
    parameters =
      visualizationTypes: _visualizationTypes
      visualizationType: type
      variableX: variableX
      variableY: variableY

    _.configureScoringVisualization caption, parameters, (action, response) ->
      switch action
        when 'confirm'
          createVisualizationPane response.visualizationType, response.variableX, response.variableY, go

  addVisualization = ->
    configureVisualization 'Add Visualization', _metricsVisualizationType, null, null, (visualization) -> _visualizations.push visualization

  initialize _scorings

  metricTable: _metricTable
  visualizations: _visualizations
  displayFilters: displayFilters
  addVisualization: addVisualization



