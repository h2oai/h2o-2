Steam.FrameView = (_, key, frame) ->
  _compatibleModels = node$ ''

  # Columns section
  createInputColumnsSection = (frame) ->
    rows = map frame.column_names, (columnName) -> [ columnName ]
    [ table, tbody, tr, td ] = geyser.generate 'table.table.table-condensed.table-hover tbody tr td'
    table [
      tbody map rows, (row) ->
        tr map row, td
    ]

  createCompatibleModelsSection = (frame, models) ->
      headers = [
        'Algorithm'
        'Category'
        'Response Column'
        'State'
      ]

      rows = map frame.compatible_models, (key) ->
        model = models[key]
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

  _.requestFrameAndCompatibleModels key, (error, data) ->
    if error
      #TODO handle errors
    else
      #TODO typecheck
      _compatibleModels createCompatibleModelsSection data.frames[key], data.models
      return

  data: frame
  title: key
  key: key
  columns: createInputColumnsSection frame
  compatibleModels: _compatibleModels
  dispose: ->

