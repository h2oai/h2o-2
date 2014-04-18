Steam.ModelListView = (_) ->
  _items = do nodes$

  activateItem = (item) ->
    for other in _items()
      if other is item
        other.isActive yes
      else
        other.isActive no

    _.displayModel item.key, item.data

  selectItem = (item) ->

  createItem = (model, key) ->
    #TODO replace with type checking
    console.assert isArray model.input_column_names
    console.assert has model, 'model_algorithm'
    console.assert has model, 'model_category'
    console.assert isObject model.parameters
    console.assert has model, 'response_column_name'
    console.assert has model, 'state'

    self =
      data: model
      key: key
      title: model.model_algorithm
      caption: model.model_category
      cutline: 'Response Column: ' + model.response_column_name
      display: -> activateItem self
      isActive: node$ no
      isChecked: node$ no

    apply$ self.isChecked, (isChecked) ->
      console.log self.key, if isChecked then 'checked' else 'unchecked'

    self

  displayModels = (modelTable) ->
    _items items = mapWithKey modelTable, createItem
    activateItem head items unless isEmpty items

  loadModels = (opts) ->
    console.assert isDefined opts
    switch opts.type
      when 'all'
        _.requestModels (error, data) ->
          if error
            #TODO handle errors
          else
            displayModels data.models

      when 'compatibleWithFrame'
        _.requestFrameAndCompatibleModels opts.frameKey, (error, data) ->
          if error
            #TODO handle errors
          else
            # data.frames[opts.frameKey], data.models
            displayModels data.models
    return

  link$ _.loadModels, loadModels

  items: _items
  dispose: ->

