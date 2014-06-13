format4f = unless exports? then d3.format '.4f' else null

computeTPR = (cm) ->
  [[tn, fp], [fn, tp]] = cm
  tp / (tp + fn)

computeFPR = (cm) ->
  [[tn, fp], [fn, tp]] = cm
  fp / (fp + tn)


metricCriteriaVariable =
  id: uniqueId()
  name: 'metricCriteria'
  caption: 'Metric Criteria'
  type: 'string'
  format: identity
  domain: [
    value: 'maximum F1'
    caption: 'Max F1'
  ,
    value: 'maximum F2'
    caption: 'Max F2'
  ,
    value: 'maximum F0point5'
    caption: 'Max F0.5'
  ,
    value: 'maximum Accuracy'
    caption: 'Max Accuracy'
  ,
    value: 'maximum Precision'
    caption: 'Max Precision'
  ,
    value: 'maximum Recall'
    caption: 'Max Recall'
  ,
    value: 'maximum Specificity'
    caption: 'Max Specificity'
  ,
    value: 'maximum absolute MCC'
    caption: 'Max Absolute MCC'
  ,
    value: 'minimizing max per class Error'
    caption: 'Min MPCE'
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
  ,
    value: 'error_for_criteria'
    caption: 'Error'
    domain: [0, 1]
  ,
    value: 'F0point5_for_criteria'
    caption: 'F0.5'
    domain: [0, 1]
  ,
    value: 'F1_for_criteria'
    caption: 'F1'
    domain: [0, 1]
  ,
    value: 'F2_for_criteria'
    caption: 'F2'
    domain: [0, 1]
  ,
    value: 'accuracy_for_criteria'
    caption: 'Accuracy'
    domain: [0, 1]
  ,
    value: 'precision_for_criteria'
    caption: 'Precision'
    domain: [0, 1]
  ,
    value: 'recall_for_criteria'
    caption: 'Recall'
    domain: [0, 1]
  ,
    value: 'specificity_for_criteria'
    caption: 'Specificity'
    domain: [0, 1]
  ,
    value: 'mcc_for_criteria'
    caption: 'MCC'
    domain: [-1, 1]
  ,
    value: 'max_per_class_error_for_criteria'
    caption: 'MPCE'
    domain: [0, 1]
  ]

metricVariables = []

scoringVariable = 
  id: uniqueId()
  name: 'scoring'
  caption: 'Scoring'
  type: 'string'
  read: (metric) -> metric.caption
  domain: []
  format: identity

metricVariables.push scoringVariable

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

  #TODO create variables for model inputs.
  modelVariables = null

  for variable in metricVariables when variable.type is 'float'
    extent = d3.extent metrics, variable.read
    [ l, u ] = extent
    variable.extent = if (isNumber l) and (isNumber u) and l < u then extent else variable.domain

  for variable in thresholdVariables when variable.type is 'float'
    extents = map metrics, (metric) -> d3.extent times metric.data.auc.thresholds.length, (index) -> variable.read metric, index
    l = d3.min extents, (extent) -> head extent
    u = d3.max extents, (extent) -> last extent
    variable.extent = if (isNumber l) and (isNumber u) and l < u then [l, u] else variable.domain

  metrics: metrics
  modelVariables: modelVariables
  metricVariables: metricVariables
  thresholdVariables: thresholdVariables

createFilterLookup = (filter) ->
  lookup = {}
  for item in filter.items
    lookup[item.factor.value] = yes if item.isChecked()
  lookup

createMetricsVisualization = (metrics, variableX, variableY) ->
  [ div ] = geyser.generate [ 'div' ]
  render = ($element) ->
    plot = renderMetricsVisualization metrics, variableX, variableY
    $element.empty().append plot
  markup: div()
  behavior: render

createThresholdVisualization = (metrics, variableX, variableY, showReferenceLine) ->
  [ div ] = geyser.generate [ 'div' ]
  render = ($element) ->
    plot = renderThresholdVisualization metrics, variableX, variableY, showReferenceLine
    $element.empty().append plot
  markup: div()
  behavior: render

renderMetricsVisualization = (metrics, variableX, variableY) ->
  margin = top: 20, right: 20, bottom: 20, left: 30
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
    .on 'click', (d) ->
      _.inspect
        content: createStripVisualizationValueInspection d
        template: 'geyser'
  el

renderThresholdVisualization = (metrics, variableX, variableY, showReferenceLine) ->
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
      .attr 'cx', (d) -> scaleX variableX.read metric, d
      .attr 'cy', (d) -> scaleY variableY.read metric, d
      .on 'click', (d) ->
        _.inspect
          content: createThresholdPlotInspection metric, d
          template: 'geyser'
      .on 'mouseover', (d) ->
        d3.select(@).style 'stroke', metric.color
      .on 'mouseout', (d) ->
        d3.select(@).style 'stroke', 'none'
  el

Steam.ScoringSheetView = (_, _scorings) ->
  _metricFrame = null
  _metricTable = node$ null
  _filtersView = null
  _filtersIndex = null
  _sortByVariable = metricVariablesIndex.auc
  _sortAscending = no
  _visualizations = nodes$ []
  _filteredMetricVariables = null
  _filteredMetrics = null

  initialize = (scorings) ->
    _metricFrame = createMetricFrameFromScorings scorings
    scoringVariable.domain = map _metricFrame.metrics, (metric) ->
      value: metric.id
      caption: metric.caption
    _filtersView = createFiltersView scoringVariable, metricTypeVariable, metricCriteriaVariable
    _filtersIndex = indexBy _filtersView.filters, (filter) -> filter.variable.name
    updateFiltering()
    _metricTable createMetricTable _metricFrame 

  updateFiltering = ->
    scoringFilterLookup = createFilterLookup _filtersIndex.scoring
    metricTypeFilterLookup = createFilterLookup  _filtersIndex.metricType
    metricCriteriaFilterLookup = createFilterLookup _filtersIndex.metricCriteria
    _filteredMetricVariables = filter _metricFrame.metricVariables, (variable) ->
      if variable.meta
        if metricTypeFilterLookup[variable.meta.metricType.value] and metricCriteriaFilterLookup[variable.meta.metricCriterion.value]
          yes
        else
          no
      else
        #TODO AUC, Gini
        yes
    _filteredMetrics = filter _metricFrame.metrics, (metric) -> scoringFilterLookup[metric.id]

  invalidate = ->
    _metricTable createMetricTable()

  createMetricTable = ->
    [ table, thead, tbody, tr, th, td ] = geyser.generate words 'table.table.table-condensed thead tbody tr th td'
    [ span ] = geyser.generate [ "a data-variable-id='$id'" ]

    # Sort
    _metricFrame.metrics.sort (a, b) ->
      diff = (_sortByVariable.read a) - (_sortByVariable.read b)
      if _sortAscending then diff else -diff
    
    header = tr map _filteredMetricVariables, (variable) ->
      th span variable.caption, $id: variable.id

    rows = map _filteredMetrics, (metric) ->
      tr map _filteredMetricVariables, (variable) ->
        td variable.format variable.read metric

    markup = table [
      thead header
      tbody rows
    ]

    behavior = ($element) ->
      $('a', $element).each ->
        $anchor = $ @
        $anchor.click ->
          sortById = $anchor.attr 'data-variable-id'
          _sortByVariable = find _metricFrame.metricVariables, (variable) -> variable.id is sortById
          #TODO toggle sort direction
          invalidate()

    markup: markup
    behavior: behavior

  createDiscreteFilterView = (variable) ->
    initialized = no
    items = map variable.domain, (factor) ->
      isChecked = node$ yes
      self =
        caption: factor.caption
        factor: factor
        isChecked: isChecked
      apply$ isChecked, (isChecked) ->
        if initialized
          updateFiltering()
          invalidate()
      self

    initialized = yes

    caption: variable.caption
    variable: variable
    items: items
    template: 'discrete-scoring-filter'

  createFiltersView = (variables...) ->
    filters: map variables, createDiscreteFilterView
    filterTemplate: (filter) -> filter.template
    template: 'scoring-filters'

  displayFilters = ->
    _.inspect _filtersView

  addVisualization = ->
    scoringVisualizationType =
      type: 'scoring'
      caption: 'Scoring'
      variables: metricVariables

    thresholdVisualizationType =
      type: 'threshold'
      caption: 'Threshold'
      variables: thresholdVariables
    visualizationTypes = [ scoringVisualizationType, thresholdVisualizationType ]
    parameters =
      visualizationTypes: visualizationTypes
      visualizationType: scoringVisualizationType
      axisXVariable: scoringVisualizationType.variables[1]
      axisYVariable: scoringVisualizationType.variables[2]
    _.configureScoringVisualization 'Add Visualization', parameters, (action, response) ->
      switch action
        when 'confirm'
          switch response.visualizationType
            when scoringVisualizationType
              _visualizations.push createMetricsVisualization _metricFrame.metrics, response.axisXVariable, response.axisYVariable
            when thresholdVisualizationType
              _visualizations.push createThresholdVisualization _metricFrame.metrics, response.axisXVariable, response.axisYVariable

    

  initialize _scorings

  metricTable: _metricTable
  visualizations: _visualizations
  displayFilters: displayFilters
  addVisualization: addVisualization



