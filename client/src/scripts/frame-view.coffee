Steam.FrameView = (_, _frame) ->
  _compatibleModels = node$ ''

  initialize = (frame) ->
    _.requestFrameAndCompatibleModels frame.key, (error, data) ->
      if error
        #TODO handle errors
      else
        #TODO typecheck
        unless isEmpty data.frames
          _compatibleModels createCompatibleModelsSection head data.frames
        return

  # Columns section
  createColumnsSection = (frame) ->
    rows = map frame.column_names, (columnName) -> [ columnName ]
    [ table, tbody, tr, td ] = geyser.generate 'table.table.table-condensed.table-hover tbody tr td'
    table [
      tbody map rows, (row) ->
        tr map row, td
    ]

  createCompatibleModelsSection = (frame) ->
      headers = [
        'Method'
        'Category'
        'Response Column'
        'State'
      ]

      rows = map frame.compatible_models, (model) ->
        [
          model.model_algorithm
          model.model_category
          model.response_column_name
          model.state
        ]

      [ table, thead, tbody, tr, th, td ] = geyser.generate words 'table.table.table-condensed.table-hover thead tbody tr th td'

      table [
        thead [
          tr map headers, (header) ->
            th header
        ]
        tbody map rows, (row) ->
          tr map row, td
      ]

  loadCompatibleModels = ->
    _.switchToModels type: 'compatibleWithFrame', frameKey: _frame.key

  initialize _frame

  data: _frame
  key: _frame.key
  title: _frame.key
  columns: createColumnsSection _frame
  compatibleModels: _compatibleModels
  loadCompatibleModels: loadCompatibleModels
  dispose: ->
  template: 'frame-view'

