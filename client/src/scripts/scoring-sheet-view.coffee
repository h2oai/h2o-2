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

createMetricInspection = (variables, metric) ->
  [ div, h1, h2, table, tbody, tr, th, td ] = geyser.generate words 'div h1 h2 table.table.table-condensed tbody tr th td'

  div [
    h1 metric.caption
    table tbody map variables, (variable) ->
      value = variable.read metric
      tr [
        th variable.caption
        td if isNaN value then 'NaN' else variable.format value
      ]
  ]

createThresholdInspection = (variables, metric, index) ->
  [ div, h1, h2, table, grid, tbody, tr, th, td ] = geyser.generate words 'div h1 h2 table.table.table-condensed table.table.table-bordered tbody tr th td'

  tabulateProperties = (index) ->
    table tbody map variables, (variable) ->
      tr [
        th variable.caption
        td variable.format variable.read metric, index
      ]

  div [
    h1 metric.caption
    h2 'Outputs'
    tabulateProperties index
  ]

renderMetricsVisualization = (metrics, variableX, variableY, inspect) ->
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
    .on 'click', (d) -> inspect d

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
  el

createFilter = (variable) ->
  items = map variable.domain, (factor) ->
    factor: factor
    isSelected: yes
  predicate = indexBy items, (item) -> item.factor.value

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

  _filteredMetricVariables = null
  _filteredMetrics = null
  _scoringFilter = null
  _metricTypeFilter = null
  _metricCriteriaFilter = null
  _allFilters = null

  initialize = (scorings) ->
    _metricFrame = createMetricFrameFromScorings scorings
    scoringVariable.domain = map _metricFrame.metrics, (metric) ->
      value: metric.id
      caption: metric.caption
    _scoringFilter = createFilter scoringVariable
    _metricTypeFilter = createFilter metricTypeVariable
    _metricCriteriaFilter = createFilter metricCriteriaVariable
    _allFilters = [ _scoringFilter, _metricTypeFilter, _metricCriteriaFilter ]
    updateFiltering()
    _metricTable createMetricTable _metricFrame 
    _visualizations.push addThresholdVisualization thresholdVariablesIndex.fpr, thresholdVariablesIndex.tpr, yes

  updateFiltering = ->
    _filteredMetricVariables = filter _metricFrame.metricVariables, (variable) ->
      if variable.meta
        if _metricTypeFilter.predicate[variable.meta.metricType.value] and _metricCriteriaFilter.predicate[variable.meta.metricCriterion.value]
          yes
        else
          no
      else
        #TODO AUC, Gini
        yes
    _filteredMetrics = filter _metricFrame.metrics, (metric) -> _scoringFilter.predicate[metric.id]

  invalidate = ->
    _metricTable createMetricTable()

  createMetricTable = ->
    [ table, thead, tbody, tr, th, thAsc, thDesc, td ] = geyser.generate words 'table.table.table-condensed thead tbody tr th th.y-sorted-asc th.y-sorted-desc td'
    [ span ] = geyser.generate [ "a data-variable-id='$id'" ]

    # Sort
    _filteredMetrics.sort (metricA, metricB) ->
      a = _sortByVariable.read metricA
      b = _sortByVariable.read metricB
      if _sortAscending then a > b else b > a
    
    header = tr map _filteredMetricVariables, (variable) ->
      tag = if variable isnt _sortByVariable then th else if _sortAscending then thAsc else thDesc
      tag span variable.caption, $id: variable.id

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
          _sortAscending = $anchor.parents('th').hasClass 'y-sorted-desc'
          invalidate()

    markup: markup
    behavior: behavior

  displayFilters = ->
    _.filterScorings _allFilters, (action, predicates) ->
      switch action
        when 'confirm'
          for predicate, i in predicates
            filter = _allFilters[i]
            filter.predicate = predicate
            for item in filter.items
              item.isSelected = predicate[item.factor.value]
          updateFiltering()
          invalidate()

  addMetricsVisualization = (variableX, variableY) ->
    rendering =  createMetricsVisualization _metricFrame.metrics, variableX, variableY, (metric) ->
      _.inspect
        content: createMetricInspection _metricFrame.metricVariables, metric
        template: 'geyser'
    caption: "#{variableX.caption} vs #{variableY.caption}"
    rendering: rendering

  addThresholdVisualization = (variableX, variableY, showReferenceLine) ->
    rendering =  createThresholdVisualization _metricFrame.metrics, variableX, variableY, showReferenceLine, (metric, index) ->
      _.inspect
        content: createThresholdInspection _metricFrame.thresholdVariables, metric, index
        template: 'geyser'
    caption: if variableX is thresholdVariablesIndex.fpr and variableY is thresholdVariablesIndex.tpr then 'ROC Chart' else "#{variableX.caption} vs #{variableY.caption}"
    rendering: rendering

  addVisualization = ->
    scoringVisualizationType =
      type: 'scoring'
      caption: 'Scoring'
      variables: filter metricVariables, (variable) -> variable.type is 'float'

    thresholdVisualizationType =
      type: 'threshold'
      caption: 'Threshold'
      variables: filter thresholdVariables, (variable) -> variable.type is 'float'

    visualizationTypes = [ scoringVisualizationType, thresholdVisualizationType ]

    parameters =
      visualizationTypes: visualizationTypes
      visualizationType: scoringVisualizationType
      variableX: scoringVisualizationType.variables[0]
      variableY: scoringVisualizationType.variables[1]
    _.configureScoringVisualization 'Add Visualization', parameters, (action, response) ->
      switch action
        when 'confirm'
          switch response.visualizationType
            when scoringVisualizationType
              _visualizations.push addMetricsVisualization response.variableX, response.variableY
            when thresholdVisualizationType
              _visualizations.push addThresholdVisualization response.variableX, response.variableY, no

  initialize _scorings

  metricTable: _metricTable
  visualizations: _visualizations
  displayFilters: displayFilters
  addVisualization: addVisualization



