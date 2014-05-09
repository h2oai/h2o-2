Steam.FrameView = (_, _frame) ->
  _compatibleModels = node$ ''
  _compatibleModelsCount = node$ ''

  initialize = (frame) ->
    _.requestFrameAndCompatibleModels frame.key, (error, data) ->
      if error
        #TODO handle errors
      else
        #TODO typecheck
        unless isEmpty data.frames
          aFrame = head data.frames
          _compatibleModels createCompatibleModelsSection aFrame
          _compatibleModelsCount "(#{aFrame.compatible_models.length})"
        return

  # Columns section
  createColumnsSection = (columnNames) ->
    rows = map columnNames, (columnName) -> [ columnName ]
    [ table, tbody, tr, td ] = geyser.generate words 'table.table.table-condensed tbody tr td'
    table [
      tbody map rows, (row) ->
        tr map row, td
    ]

  createCompatibleModelsSection = (frame) ->
      headers = [
        'Key'
        'Method'
        'Category'
        'Response Column'
        #TODO uncomment when this is functional
        # 'State'
      ]

      rows = map frame.compatible_models, (model) ->
        [
          model.key
          model.model_algorithm
          model.model_category
          model.response_column_name
          #TODO uncomment when this is functional
          # model.state
        ]

      [ table, thead, tbody, tr, th, td ] = geyser.generate words 'table.table.table-condensed thead tbody tr th td'

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
  timestamp: _frame.creation_epoch_time_millis
  title: _frame.key
  columns: createColumnsSection _frame.column_names
  columnCount: "(#{_frame.column_names.length})"
  compatibleModels: _compatibleModels
  compatibleModelsCount: _compatibleModelsCount
  loadCompatibleModels: loadCompatibleModels
  dispose: ->
  template: 'frame-view'

