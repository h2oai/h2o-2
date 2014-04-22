Steam.FrameListView = (_) ->
  _predicate = node$ type: 'all'
  _items = do nodes$
  _hasItems = lift$ _items, (items) -> items.length > 0

  _canClearPredicate = lift$ _predicate, (predicate) -> predicate.type isnt 'all'
  _predicateCaption = lift$ _predicate, (predicate) ->
    switch predicate.type
      when 'all'
        'Showing\nall frames'
      when 'compatibleWithModel'
        "Showing frames compatible with\n#{predicate.modelKey}"
      else
        ''

  activateItem = (item) ->
    for other in _items()
      if other is item
        other.isActive yes
      else
        other.isActive no

    _.displayFrame item.data

    return

  createItem = (frame) ->
    #TODO replace with type checking
    console.assert isArray frame.column_names
    self =
      data: frame
      title: frame.key
      caption: describeCount frame.column_names.length, 'column'
      cutline: join frame.column_names, ', '
      display: -> activateItem self
      isActive: node$ no
  
  displayFrames = (frames) ->
    _items items = map frames, createItem
    if isEmpty items
      _.displayEmpty()
    else
      activateItem head items

  apply$ _predicate, (predicate) ->
    console.assert isDefined predicate
    switch predicate.type
      when 'all'
        _.requestFrames (error, data) ->
          if error
            #TODO handle errors
          else
            displayFrames data.frames

      when 'compatibleWithModel'
        _.requestModelAndCompatibleFrames predicate.modelKey, (error, data) ->
          if error
            #TODO handle errors
          else
            displayFrames (head data.models).compatible_frames
    return

  clearPredicate = -> _predicate type: 'all'

  link$ _.loadFrames, _predicate

  items: _items
  predicateCaption: _predicateCaption
  clearPredicate: clearPredicate
  canClearPredicate: _canClearPredicate
  hasItems: _hasItems
  template: 'frame-list-view'

