Steam.ModelView = (_, key, model) ->

  createDefinitionList = (data) ->
    [ dl, li, dt, dd ] = geyser.generate '.y-summary .y-summary-item .y-summary-key .y-summary-value'
    #BUG this should work with scalars
    dl mapWithKey data, (value, key) -> li [ (dt key), (dd value) ]

  # Summary section
  createSummarySection = (model) ->
    createDefinitionList
      'Response Column': model.response_column_name
      'Model Category': model.model_category
      'State': model.state

  # Parameters section
  createParametersSection = (model) ->
    createDefinitionList model.parameters
  
  # Input columns section
  createInputColumnsSection = (model) ->
    rows = map model.input_column_names, (columnName) -> [ columnName ]
    [ table, tbody, tr, td ] = geyser.generate 'table.table.table-condensed.table-hover tbody tr td'
    trs = map rows, (row) -> tr map row, td
    #BUG this should work with scalars
    table [tbody trs]
  
  data: model
  title: model.model_algorithm
  key: key
  summary: createSummarySection model
  parameters: createParametersSection model
  inputColumns: createInputColumnsSection model
  dispose: ->

