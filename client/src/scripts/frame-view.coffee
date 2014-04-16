Steam.FrameView = (_, key, frame) ->
  data: frame
  title: key
  key: key
  columns:
    headers: ['Columns']
    rows: map frame.column_names, (columnName) -> [ columnName ]
  dispose: ->

