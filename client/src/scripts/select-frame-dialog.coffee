Steam.SelectFrameDialog = (_, _go) ->
  _isBusy = node$ yes
  _items = do nodes$
  _selectedItem = node$ null
  _hasSelection = lift$ _selectedItem, isTruthy

  selectItem = (target) ->
    for item in _items()
      item.isActive item is target
    _selectedItem target

  createItem = (frame) ->
    self =
      data: frame
      key: frame.key
      count: describeCount frame.column_names.length, 'column'
      timestamp: frame.creation_epoch_time_millis
      select: -> selectItem self
      isActive: node$ no

  _.requestFrames (error, data) ->
    if error
      _go 'error', error
    else
      if data.frames.length > 0
        _items items = map data.frames, createItem
        selectItem head items
      _isBusy no

  confirm = -> _go 'confirm', _selectedItem().data.key
  cancel = -> _go 'cancel'
  
  items: _items
  hasSelection: _hasSelection
  isBusy: _isBusy
  confirm: confirm
  cancel: cancel
  template: 'select-frame-dialog'
