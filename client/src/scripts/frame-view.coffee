Steam.FrameView = (_, key, frame) ->

  # Columns section
  createInputColumnsSection = (frame) ->
    rows = map frame.column_names, (columnName) -> [ columnName ]
    [ table, tbody, tr, td ] = geyser.generate 'table.table.table-condensed.table-hover tbody tr td'
    trs = map rows, (row) -> tr map row, td
    #BUG this should work with scalars
    table [tbody trs]

  data: frame
  title: key
  key: key
  columns: createInputColumnsSection frame
  dispose: ->

