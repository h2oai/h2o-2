#TODO check for memory leaks
Steam.ScoringView = (_, _scoring) ->
  _tag = node$ ''
  _caption = node$ ''
  _timestamp = node$ Date.now()
  _comparisonTable = node$ null
  _multiRocPlot = node$ null
  _stripPlot = node$ null
  _modelSummary = nodes$ []
  _hasFailed = node$ no
  _failure = node$ null
  _scoringType = node$ null
  _isScoringView = lift$ _scoringType, (type) -> type is 'scoring'
  _isComparisonView = lift$ _scoringType, (type) -> type is 'comparison'

  #TODO make this a property of the comparison object
  _isTabularComparisonView = node$ yes
  _isAdvancedComparisonView = lift$ _isTabularComparisonView, negate
  switchToTabularView = -> _isTabularComparisonView yes
  switchToAdvancedView = -> _isTabularComparisonView no

  createModelSummary = (model) ->
    [
      key: 'Model Category'
      value: model.model_category
    ,
      key: 'Response Column'
      value: model.response_column_name
    ]

  #TODO unused - remove
  createItem = (score) ->
    status = node$ if isNull score.status then '-' else score.status
    isSelected = lift$ status, (status) -> status is 'done'

    data: score
    algorithm: score.model.model_algorithm
    category: score.model.model_category
    responseColumn: score.model.response_column_name
    status: status

  initialize = (item) ->
    switch item.type
      when 'scoring'
        scoring = item
        input = scoring.data.input
        _tag 'Scoring'
        _caption "Scoring on #{input.frameKey}"
        _modelSummary createModelSummary input.model
        apply$ scoring.isReady, scoring.hasFailed, (isReady, hasFailed) ->
          if isReady
            if hasFailed
              _hasFailed yes
              _failure scoring.data.output
            else
              _timestamp (head scoring.data.output.metrics).scoring_time
              _comparisonTable createComparisonTable [ scoring ]
      when 'comparison'
        comparison = item
        _tag 'Comparison'
        _caption "Scoring Comparison"
        _timestamp comparison.data.timestamp
        _modelSummary null #TODO populate model summary
        scorings = comparison.data.scorings
        apply$ _isTabularComparisonView, (isTabularComparisonView) ->
          console.log _isAdvancedComparisonView()
          if isTabularComparisonView
            if scorings.length > 0
              _comparisonTable createComparisonTable scorings
            else
              _comparisonTable null
          else
            if scorings.length > 0
              metricsArray = createMetricsArray scorings
              _multiRocPlot createMultiRocPlot metricsArray
              _stripPlot createStripPlot metricsArray
            else
              _multiRocPlot null
              _stripPlot null

    _scoringType item.type

  renderRocCurve = (data) ->
    margin = top: 20, right: 20, bottom: 20, left: 30
    width = 175
    height = 175

    x = d3.scale.linear()
      .domain [ 0, 1 ]
      .range [ 0, width ]

    y = d3.scale.linear()
      .domain [ 0, 1 ]
      .range [ height, 0 ]

    axisX = d3.svg.axis()
      .scale x
      .orient 'bottom'
      .ticks 5

    axisY = d3.svg.axis()
      .scale y
      .orient 'left'
      .ticks 5

    line = d3.svg.line()
      .x (d) -> x d.fpr
      .y (d) -> y d.tpr

    el = document.createElementNS 'http://www.w3.org/2000/svg', 'svg'

    svg = (d3.select el)
      .attr 'class', 'y-roc-curve'
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
      .text 'FPR'

    svg.append 'g'
      .attr 'class', 'y axis'
      .call axisY
      .append 'text'
      .attr 'transform', 'rotate(-90)'
      .attr 'y', 6
      .attr 'dy', '.71em'
      .style 'text-anchor', 'end'
      .text 'TPR'

    svg.append 'line'
      .attr 'class', 'guide'
      .attr 'stroke-dasharray', '3,3'
      .attr
        x1: x 0
        y1: y 0
        x2: x 1
        y2: y 1

    svg.selectAll '.dot'
      .data data
      .enter()
      .append 'circle'
      .attr 'class', 'dot'
      .attr 'r', 1
      .attr 'cx', (d) -> x d.fpr
      .attr 'cy', (d) -> y d.tpr

    svg.append 'path'
      .datum data
      .attr 'class', 'line'
      .attr 'd', line

    el

  computeTPRandFPR = (cm) ->
    [[tn, fp], [fn, tp]] = cm

    tpr: tp / (tp + fn)
    fpr: fp / (fp + tn)

  createRocCurve = (cms) ->
    rates = map cms, computeTPRandFPR
    renderRocCurve rates

  createInputParameter = (key, value, isVisible) ->
    # DL, DRF have array-valued params, so handle that case properly
    formattedValue = if isArray value then value.join ', ' else value

    key: key
    value: formattedValue
    isVisible: isVisible
    isDifferent: no

  combineInputParameters = (model) ->
    critical = mapWithKey model.critical_parameters, (value, key) ->
      createInputParameter key, value, yes
    secondary = mapWithKey model.secondary_parameters, (value, key) ->
      createInputParameter key, value, no
    concat critical, secondary

  # Side-effects!
  markAsDifferent = (parametersArray, index) ->
    for parameters in parametersArray
      parameter = parameters[index]
      # mark this as different to enable special highlighting
      parameter.isDifferent = yes
      parameter.isVisible = yes
    return

  # Side-effects!
  compareInputParameters = (parametersArray) ->
    headParameters = head parametersArray
    tailParametersArray = tail parametersArray
    for parameters, index in headParameters
      for tailParameters in tailParametersArray
        if parameters.value isnt tailParameters[index].value
          markAsDifferent parametersArray, index
          break
    return

  renderStripPlot = (scorings) ->
    palette = if scorings.length > 10 then d3.scale.category20 else d3.scale.category10
    color = palette().domain map scorings, (scoring) -> scoring.id
    categories = keys (head scorings).outputs

    margin = top: 20, right: 20, bottom: 20, left: 140
    width = 190
    rowHeight = 18
    height = categories.length * rowHeight

    x = zipObject map categories, (category) ->
      scaleX = d3.scale.linear()
        #.domain d3.extent scorings, (d) -> +d.outputs[category]
        .domain [0, 1]
        .range [ 0, width ]
      [ category, scaleX ]

    y = d3.scale.ordinal()
      .domain categories
      .rangePoints [ 0, height ], 1

    line = d3.svg.line()

    axis = d3.svg.axis()
      .orient 'left'

    path = (d) ->
      line map categories, (category) ->
        [ (x[category] d.outputs[category]), (y category) ]

    el = document.createElementNS 'http://www.w3.org/2000/svg', 'svg'
    svg = (d3.select el)
      .attr 'class', 'y-strip-plot'
      .attr 'width', width + margin.left + margin.right
      .attr 'height', height + margin.top + margin.bottom
      .append 'g'
      .attr 'transform', "translate(#{margin.left},#{margin.top})"

    line = svg.append 'g'
      .attr 'class', 'line'
      .selectAll 'path'
      .data scorings
      .enter()
      .append 'path'
      .attr 'd', path
      .attr 'id', (d) -> "path-#{d.id}"

    forEach scorings, (scoring) ->
      stroke = color scoring.id
      svg.append 'g'
        .attr 'id', "strips-#{scoring.id}"
        .selectAll '.strip'
        .data categories
        .enter()
        .append 'line'
        .attr 'class', 'strip'
        .attr 'x1', (d) -> x[d] scoring.outputs[d]
        .attr 'y1', (d) -> -5 + y d
        .attr 'x2', (d) -> x[d] scoring.outputs[d]
        .attr 'y2', (d) -> 5 + y d
        .attr 'stroke', stroke
        .on 'mouseover', (d) -> svg.select("#path-#{scoring.id}").style 'stroke', '#ddd'
        .on 'mouseout', (d) -> svg.select("#path-#{scoring.id}").style 'stroke', 'none'
        .append 'title'
        .text (d) -> "#{scoring.caption}\n#{d} = #{scoring.outputs[d]}"

    g = svg.selectAll '.category'
      .data categories
      .enter()
      .append 'g'
      .attr 'transform', (d) -> "translate(#{-margin.left}, #{y d})"

    g.append 'text'
      .attr 'dy', 5
      .text String

    g.append 'line'
      .attr 'class', 'guide'
      .attr 'x1', 0
      .attr 'y1', rowHeight / 2
      .attr 'x2', margin.left + width + margin.right
      .attr 'y2', rowHeight / 2

#    g.append 'g'
#      .attr 'class', 'axis'
#      .each (d) -> d3.select(@).call axis.scale x[d]
#      .append 'text'
#      .attr 'text-anchor', 'middle'
#      .attr 'y', -9
#      .text String

    el

  reshapeAucForParallelCoords = (auc) ->
    data = {}

    criteria =
      'maximum F1': 'Max F1'
      'maximum F2': 'Max F2'
      'maximum F0point5': 'Max F0.5'
      'maximum Accuracy': 'Max Accuracy'
      'maximum Precision': 'Max Precision'
      'maximum Recall': 'Max Recall'
      'maximum Specificity': 'Max Specificity'
      'minimizing max per class Error': 'Min MPCE'

    outputs = [
      key: 'threshold_for_criteria'
      caption: 'Threshold'
    ,
      key: 'F0point5_for_criteria'
      caption: 'F0.5'
    ,
      key: 'F1_for_criteria'
      caption: 'F1'
    ,
      key: 'F2_for_criteria'
      caption: 'F2'
    ,
      key: 'accuracy_for_criteria'
      caption: 'Accuracy'
    ,
      key: 'precision_for_criteria'
      caption: 'Precision'
    ,
      key: 'recall_for_criteria'
      caption: 'Recall'
    ,
      key: 'specificity_for_criteria'
      caption: 'Specificity'
    ,
      key: 'max_per_class_error_for_criteria'
      caption: 'MPCE'
    ]

    for criterion, index in auc.threshold_criteria
      for output in outputs
        criterionCaption = criteria[criterion] or criterion
        value = auc[output.key][index]
        data["#{criterionCaption} - #{output.caption}"] = if value is 'NaN' then null else value

    data


  renderMultiRocPlot = (scorings) ->
    margin = top: 20, right: 20, bottom: 20, left: 30
    width = 300
    height = 300

    x = d3.scale.linear()
      .domain [ 0, 1 ]
      .range [ 0, width ]

    y = d3.scale.linear()
      .domain [ 0, 1 ]
      .range [ height, 0 ]

    # Go for higher contrast when comparing fewer scorings.
    palette = if scorings.length > 10 then d3.scale.category20 else d3.scale.category10
    color = palette().domain map scorings, (scoring) -> scoring.id

    axisX = d3.svg.axis()
      .scale x
      .orient 'bottom'
      .ticks 5

    axisY = d3.svg.axis()
      .scale y
      .orient 'left'
      .ticks 5

    line = d3.svg.line()
      .x (d) -> x d.fpr
      .y (d) -> y d.tpr

    el = document.createElementNS 'http://www.w3.org/2000/svg', 'svg'

    svg = (d3.select el)
      .attr 'class', 'y-multi-roc-curve'
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
      .text 'FPR'

    svg.append 'g'
      .attr 'class', 'y axis'
      .call axisY
      .append 'text'
      .attr 'transform', 'rotate(-90)'
      .attr 'y', 6
      .attr 'dy', '.71em'
      .style 'text-anchor', 'end'
      .text 'TPR'

    svg.append 'line'
      .attr 'class', 'guide'
      .attr 'stroke-dasharray', '3,3'
      .attr
        x1: x 0
        y1: y 0
        x2: x 1
        y2: y 1

    curve = svg.selectAll '.y-curve'
      .data scorings
      .enter()
      .append 'g'
      .attr 'class', 'y-curve'

    curve.append 'path'
      .attr 'id', (d) -> "curve#{d.id}"
      .attr 'class', 'line'
      .attr 'd', (d) -> line d.rates
      .style 'stroke', (d) -> color d.id
      .on 'mouseover', (d) -> console.log 'mouseover', d
      .on 'mouseout', (d) -> console.log 'mouseout', d

    forEach scorings, (scoring) ->
      stroke = color scoring.id
      svg.append 'g'
        .selectAll '.dot'
        .data scoring.rates
        .enter()
        .append 'circle'
        .attr 'class', 'dot'
        .attr 'r', 5
        .attr 'cx', (d) -> x d.fpr
        .attr 'cy', (d) -> y d.tpr
        .on 'mouseover', (d) ->
          d3.select(@).style 'stroke', stroke
          #TODO send signal
        .on 'mouseout', (d) ->
          d3.select(@).style 'stroke', 'none'
          #TODO send signal
    el

  createMetricsArray = (scores) ->
    uniqueScoringNames = {}
    createUniqueScoringName = (frameKey, modelKey) ->
      name = "#{modelKey} on #{frameKey}"
      if index = uniqueScoringNames[name]
        uniqueScoringNames[name] = index++
        name += ' #' + index
      else
        uniqueScoringNames[name] = 1
      name

    map scores, (score, index) ->
      metrics = head score.data.output.metrics

      id: index
      caption: createUniqueScoringName metrics.frame.key, metrics.model.key
      metrics: metrics

  createMultiRocPlot = (metricsArray) ->
    [ div ] = geyser.generate [ 'div' ]
    render = ($element) ->
      ratesArray = map metricsArray, (metrics) -> 
        id: metrics.id
        caption: metrics.caption
        rates: map metrics.metrics.auc.confusion_matrices, computeTPRandFPR

      multiRocPlot = renderMultiRocPlot ratesArray
      $element.empty().append multiRocPlot

    markup: div()
    behaviors: [ render ]

  createStripPlot = (metricsArray) ->
    [ div ] = geyser.generate [ 'div' ]
    render = ($element) ->
      outputsArray = map metricsArray, (metrics) ->
        id: metrics.id
        caption: metrics.caption
        outputs: reshapeAucForParallelCoords metrics.metrics.auc

      stripPlot = renderStripPlot outputsArray
      $element.empty().append stripPlot

    markup: div()
    behaviors: [ render ]

  createComparisonTable = (scores) ->
    [ div, table, kvtable, thead, tbody, tr, trExpert, diffSpan, th, thIndent, td, hyperlink] = geyser.generate words 'div table.table.table-condensed table.table-kv thead tbody tr tr.y-expert span.y-diff th th.y-indent td div.y-link'

    createParameterTable = ({ parameters }) ->
      kvtable [
        tbody map parameters, (parameter) ->
          trow = if parameter.isVisible then tr else trExpert
          trow [
            th parameter.key
            td if parameter.isDifferent then diffSpan parameter.value else parameter.value
          ]
      ]

    createComparisonGrid = (scores) ->
      algorithmRow = [ th 'Method' ]
      nameRow = [ th 'Name' ]
      rocCurveRow = [ th 'ROC Curve' ]
      inputParametersRow = [
        th [
          (div 'Input Parameters')
          (hyperlink 'Show more', 'toggle-advanced-parameters')
        ]
      ]
      errorRow = [ th 'Error' ]
      durationRow = [ th 'Time' ]
      aucRow = [ th 'AUC' ]
      thresholdCriterionRow = [ th 'Threshold Criterion' ]
      thresholdRow = [ thIndent 'Threshold' ]
      f1Row = [ thIndent 'F1' ]
      accuracyRow = [ thIndent 'Accuracy' ]
      precisionRow = [ thIndent 'Precision' ]
      recallRow = [ thIndent 'Recall' ]
      specificityRow = [ thIndent 'Specificity' ]
      maxPerClassErrorRow = [ thIndent 'Max Per Class Error' ]

      format4f = d3.format '.4f' # precision = 4

      #TODO what does it mean to have > 1 metrics
      scoreWithLowestError = min scores, (score) -> (head score.data.output.metrics).error_measure

      inputParamsWithAlgorithm = map scores, (score) ->
        model = score.data.input.model
        algorithm: model.model_algorithm
        parameters: combineInputParameters model

      inputParamsByScoreIndex = map inputParamsWithAlgorithm, (a) -> a.parameters

      inputParamsByAlgorithm = values groupBy inputParamsWithAlgorithm, (a) -> a.algorithm
      # Side-effects!
      forEach inputParamsByAlgorithm, (groups) ->
        compareInputParameters map groups, (group) -> group.parameters

      for score, scoreIndex in scores
        model = score.data.input.model
        #TODO what does it mean to have > 1 metrics
        metrics = head score.data.output.metrics
        auc = metrics.auc
        cm = metrics.cm
        errorBadge = if scores.length > 1 and score is scoreWithLowestError then ' (Lowest)' else ''

        algorithmRow.push td model.model_algorithm
        nameRow.push td model.key
        rocCurveRow.push td 'Loading...', "roc-#{scoreIndex}"
        inputParametersRow.push td createParameterTable parameters: inputParamsByScoreIndex[scoreIndex]
        errorRow.push td (format4f metrics.error_measure) + errorBadge #TODO change to bootstrap badge
        durationRow.push td "#{metrics.duration_in_ms} ms"
        aucRow.push td format4f auc.AUC
        thresholdCriterionRow.push td head auc.threshold_criteria
        thresholdRow.push td head auc.threshold_for_criteria
        f1Row.push td format4f head auc.F1_for_criteria
        accuracyRow.push td format4f head auc.accuracy_for_criteria
        precisionRow.push td format4f head auc.precision_for_criteria
        recallRow.push td format4f head auc.recall_for_criteria
        specificityRow.push td format4f head auc.specificity_for_criteria
        maxPerClassErrorRow.push td format4f head auc.max_per_class_error_for_criteria

      renderRocCurves = ($element) ->
        forEach scores, (score, scoreIndex) ->
          defer ->
            #TODO what does it mean to have > 1 metrics
            rocCurve = createRocCurve (head score.data.output.metrics).auc.confusion_matrices
            $("#roc-#{scoreIndex}", $element).empty().append rocCurve
        return

      toggleAdvancedParameters = ($element) ->
        isHidden = yes
        $toggleLink = $ '#toggle-advanced-parameters', $element
        $toggleLink.click ->
          if isHidden
            $('.y-expert', $element).show()
            $toggleLink.text 'Show less'
          else
            $('.y-expert', $element).hide()
            $toggleLink.text 'Show more'

          isHidden = not isHidden
          return
        return
      

      markup: table tbody [
        tr algorithmRow
        tr nameRow
        tr rocCurveRow
        tr inputParametersRow
        tr errorRow
        tr durationRow
        tr aucRow
        tr thresholdCriterionRow
        tr thresholdRow
        tr f1Row
        tr accuracyRow
        tr precisionRow
        tr recallRow
        tr specificityRow
        tr maxPerClassErrorRow
      ]
      behaviors: [ renderRocCurves, toggleAdvancedParameters ]

    createComparisonGrid scores


  initialize _scoring

  tag: _tag
  caption: _caption
  timestamp: _timestamp
  isScoringView: _isScoringView
  isComparisonView: _isComparisonView
  isTabularComparisonView: _isTabularComparisonView
  isAdvancedComparisonView: _isAdvancedComparisonView
  switchToTabularView: switchToTabularView
  switchToAdvancedView: switchToAdvancedView
  modelSummary: _modelSummary
  comparisonTable: _comparisonTable
  multiRocPlot: _multiRocPlot
  stripPlot: _stripPlot
  hasFailed: _hasFailed
  failure: _failure
  template: 'scoring-view'
