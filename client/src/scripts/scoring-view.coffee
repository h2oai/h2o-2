Steam.ScoringView = (_, _scoring) ->
  _tag = node$ ''
  _caption = node$ ''
  _timestamp = node$ ''
  _comparisonTable = node$ null
  _hasComparisonTable = lift$ _comparisonTable, (table) -> not isNull table
  _modelSummary = nodes$ []

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
        _timestamp new Date(input.timestamp).toString()
        _modelSummary createModelSummary input.model
        if scoring.isReady() and not scoring.hasFailed()
          displayComparisonTable [ scoring ]
      when 'comparison'
        comparison = item
        _tag 'Comparison'
        _caption "Scoring Comparison"
        _timestamp new Date(comparison.data.timestamp).toString()
        _modelSummary null #TODO populate model summary
        displayComparisonTable comparison.data.scorings

  displayComparisonTable = (scorings) ->
    renderComparisonTable scorings

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
  markAsDifferent = (parameterss, index) ->
    for parameters in parameterss
      parameter = parameters[index]
      # mark this as different to enable special highlighting
      parameter.isDifferent = yes
      parameter.isVisible = yes
    return

  # Side-effects!
  compareInputParameters = (parameterss) ->
    headParameters = head parameterss
    tailParameterss = tail parameterss
    for parameters, index in headParameters
      for tailParameters in tailParameterss
        if parameters.value isnt tailParameters[index].value
          markAsDifferent parameterss, index
          break
    return

  renderComparisonTable = (scores) ->
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

    _comparisonTable if scores.length > 0 then createComparisonGrid scores else null


  initialize _scoring

  tag: _tag
  caption: _caption
  timestamp: _timestamp
  modelSummary: _modelSummary
  comparisonTable: _comparisonTable
  hasComparisonTable: _hasComparisonTable
  template: 'scoring-view'
