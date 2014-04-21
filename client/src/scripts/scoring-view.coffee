Steam.ScoringView = (_, _scoring) ->
  _selections = nodes$ []
  _comparisonTable = node$ ''

  createSelection = (score) ->
    data: score
    algorithm: score.model.model_algorithm
    category: score.model.model_category
    responseColumn: score.model.response_column_name
    status: node$ score.status or '-'
    time: node$ score.time or '-'
    result: node$ score.result

  initialize = (scoring) ->
    _selections selections = map scoring.scores, createSelection
    if (every scoring.scores, (score) -> score.status is null)
      scoreModels scoring, selections
    else
      displayComparisonTable scoring

  runScoringJobs = (jobs, go) ->
    queue = copy jobs
    runNext = ->
      job = shift queue
      if job
        console.log "Queue size #{queue.length}/#{jobs.length}"
        job.run -> defer runNext
      else
        go()
    defer runNext

  scoreModels = (scoring, selections) ->
    frameKey = scoring.frameKey
    jobs = map selections, (selection) ->
      modelKey = selection.data.model.key
      selection.status 'waiting'
      run: (go) ->
        selection.status 'running'
        _.requestScoringOnFrame frameKey, modelKey, (error, result) ->
          data = if error then error.data else result
          selection.status data.response.status
          selection.time data.response.time
          selection.result error or result
          do go

    runScoringJobs jobs, ->
      forEach selections, (selection) ->
        score = selection.data
        score.status = selection.status()
        score.time = selection.time()
        score.result = selection.result()

      displayComparisonTable scoring

  displayComparisonTable = (scoring) ->
    console.log 'Comparision'
    console.log scoring
    [ table, kvtable, thead, tbody, tr, th, td ] = geyser.generate 'table.table.table-condensed table.table-kv thead tbody tr th td'

    transposeGrid = (grid) ->
      transposed = []
      for row, i in grid
        for cell, j in row
          column = transposed[j] or transposed[j] = []
          column[i] = cell
      transposed

    createParameterTable = (parameters) ->
      kvtable [
        tbody mapWithKey parameters, (value, key) ->
          tr [
            th key
            td value
          ]
      ]

    createROCChart = (data) ->
      margin = top: 20, right: 20, bottom: 30, left: 40
      width = 200
      height = 200

      x = d3.scale.linear()
        .domain [ 0, 1 ]
        .range [ 0, width ]

      y = d3.scale.linear()
        .domain [ 0, 1 ]
        .range [ height, 0 ]

      axisX = d3.svg.axis()
        .scale x
        .orient 'bottom'

      axisY = d3.svg.axis()
        .scale y
        .orient 'left'

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

    createROC = (cms) ->
      rates = map cms, computeTPRandFPR
      createROCChart rates


    createComparisonGrid = (scores) ->
      header = [
        'Method'
        'Category'
        'Response Column'
        'Input Parameters'
        'Error'
        'AUC'
        'Threshold Criterion'
        'Threshold'
        'F1'
        'Accuracy'
        'Precision'
        'Recall'
        'Specificity'
        'Max per class Error'
        'ROC Curve'
      ]
      format8f = d3.format '.8f' # precision = 8
      rows = map scores, (score) ->
        model = score.model
        metrics = score.result.metrics
        auc = metrics.auc.members
        cm = metrics.cm.members
        [
          model.model_algorithm
          model.model_category
          model.response_column_name
          model.parameters
          format8f metrics.error
          format8f auc.AUC
          head auc.threshold_criteria
          head auc.threshold_for_criteria
          format8f head auc.F1_for_criteria
          format8f head auc.accuracy_for_criteria
          format8f head auc.precision_for_criteria
          format8f head auc.recall_for_criteria
          format8f head auc.specificity_for_criteria
          format8f head auc.max_per_class_error_for_criteria
          createROC auc.confusion_matrices
        ]

      rows.unshift header
      rows

    renderTable = (grid) ->
      table tbody map grid, (row, i) ->
        tr map row, (cell, i) ->
          if i is 0
            th cell
          else
            if isElement cell
              td cell
            else if isObject cell
              td createParameterTable cell
            else
              td cell

    _comparisonTable renderTable transposeGrid createComparisonGrid filter scoring.scores, (score) -> score.status is 'done'


  initialize _scoring

  selections: _selections
  comparisonTable: _comparisonTable
  caption: "Scoring on #{_scoring.frameKey}"
  timestamp: new Date(_scoring.timestamp).toString()
  template: 'scoring-view'
