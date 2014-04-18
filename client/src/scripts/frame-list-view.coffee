Steam.FrameListView = (_) ->
  _items = do nodes$

  activate = (item) ->
    for other in _items()
      if other is item
        other.isActive yes
      else
        other.isActive no

    _.displayFrame item.key, item.data

    return

  createItem = (frame, key) ->
    #TODO replace with type checking
    console.assert isArray frame.column_names
    self =
      data: frame
      key: key
      title: key
      caption: describeCount frame.column_names.length, 'column'
      cutline: join frame.column_names, ', '
      display: -> activate self
      isActive: node$ no
  
  displayFrames = (frameTable) ->
    _items items = mapWithKey frameTable, createItem
    activate head items unless isEmpty items

  loadFrames = (opts) ->
    console.assert isDefined opts
    switch opts.type
      when 'all'
        _.requestFrames (error, data) ->
          if error
            #TODO handle errors
          else
            displayFrames data.frames
      when 'compatibleWithModel'
        _.requestModelAndCompatibleFrames opts.modelKey, (error, data) ->
          if error
            #TODO handle errors
          else
            displayFrames data.frames
    return

  link$ _.loadFrames, loadFrames

  items: _items
  dispose: ->

