Steam.FrameView = (_, _key, _frame) ->
  _compatibleModels = node$ ''

  # Columns section
  createColumnsSection = (frame) ->
    rows = map frame.column_names, (columnName) -> [ columnName ]
    [ table, tbody, tr, td ] = geyser.generate 'table.table.table-condensed.table-hover tbody tr td'
    table [
      tbody map rows, (row) ->
        tr map row, td
    ]

  createCompatibleModelsSection = (frame, modelTable) ->
      headers = [
        'Algorithm'
        'Category'
        'Response Column'
        'State'
      ]

      rows = map frame.compatible_models, (key) ->
        model = modelTable[key]
        [
          model.model_algorithm
          model.model_category
          model.response_column_name
          model.state
        ]

      [ table, thead, tbody, tr, th, td ] = geyser.generate 'table.table.table-condensed.table-hover thead tbody tr th td'

      table [
        thead [
          tr map headers, (header) ->
            th header
        ]
        tbody map rows, (row) ->
          tr map row, td
      ]

  loadCompatibleModels = ->
    _.switchToModels type: 'compatibleWithFrame', frameKey: _key

  _.requestFrameAndCompatibleModels _key, (error, data) ->
    if error
      #TODO handle errors
    else
      #TODO typecheck
      _compatibleModels createCompatibleModelsSection data.frames[_key], data.models
      return

  data: _frame
  title: _key
  key: _key
  columns: createColumnsSection _frame
  compatibleModels: _compatibleModels
  loadCompatibleModels: loadCompatibleModels
  dispose: ->

