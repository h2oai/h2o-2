Steam.ModelView = (_, key, model) ->
  _compatibleFrames = node$ ''

  stringify = (value) ->
    if isArray value
      join value, ', '
    else
      value
  createDefinitionList = (data) ->
    [ dl, li, dt, dd ] = geyser.generate '.y-summary .y-summary-item .y-summary-key .y-summary-value'

    dl mapWithKey data, (value, key) ->
      li [ (dt key), (dd stringify value) ]

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

    #TODO duplicates logic in FrameView. Refactor.
    [ table, tbody, tr, td ] = geyser.generate 'table.table.table-condensed.table-hover tbody tr td'
    table [
      tbody map rows, (row) ->
        tr map row, td
    ]


  createCompatibleFramesSection = (model, frames) ->
    headers = [
      'Dataset'
      'Columns'
    ]

    rows = map model.compatible_frames, (key) ->
      frame = frames[key]
      [
        key
        join frame.column_names, ', '
      ]

    #TODO duplicates logic in FrameView. Refactor.
    [ table, thead, tbody, tr, th, td ] = geyser.generate 'table.table.table-condensed.table-hover thead tbody tr th td'

    table [
      thead [
        tr map headers, (header) ->
          th header
      ]
      tbody map rows, (row) ->
        tr map row, td
    ]

  _.requestModelAndCompatibleFrames key, (error, data) ->
    if error
      #TODO handle errors
    else
      #TODO typecheck
      _compatibleFrames createCompatibleFramesSection data.models[key], data.frames

  
  data: model
  title: model.model_algorithm
  key: key
  summary: createSummarySection model
  parameters: createParametersSection model
  inputColumns: createInputColumnsSection model
  compatibleFrames: _compatibleFrames
  dispose: ->

