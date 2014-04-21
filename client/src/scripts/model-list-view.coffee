Steam.ModelListView = (_) ->
  _items = do nodes$

  #TODO ugly
  _isLive = node$ yes

  activateItem = (item) ->
    for other in _items()
      if other is item
        other.isActive yes
      else
        other.isActive no

    _.displayModel item.data

  createItem = (model) ->
    #TODO replace with type checking
    console.assert isArray model.input_column_names
    console.assert has model, 'model_algorithm'
    console.assert has model, 'model_category'
    console.assert isObject model.parameters
    console.assert has model, 'response_column_name'
    console.assert has model, 'state'

    self =
      data: model
      title: model.model_algorithm
      caption: model.model_category
      cutline: 'Response Column: ' + model.response_column_name
      display: -> activateItem self
      isActive: node$ no
      isSelected: node$ no

    apply$ _isLive, self.isSelected, (isLive, isSelected) ->
      _.modelSelectionChanged isSelected, _predicate, model if isLive
    self

  displayModels = (models) ->
    _items items = map models, createItem
    activateItem head items unless isEmpty items

  _predicate = null
  loadModels = (predicate) ->
    console.assert isDefined predicate
    _predicate = predicate
    switch predicate.type
      when 'all'
        _.requestModels (error, data) ->
          if error
            #TODO handle errors
          else
            displayModels data.models

      when 'compatibleWithFrame'
        _.requestFrameAndCompatibleModels predicate.frameKey, (error, data) ->
          if error
            #TODO handle errors
          else
            # data.frames[predicate.frameKey], data.models
            displayModels (head data.frames).compatible_models
    return

  link$ _.loadModels, loadModels
  link$ _.deselectAllModels, ->
    #TODO ugly
    _isLive no
    for item in _items()
      item.isSelected no
    _isLive yes

  items: _items
  template: 'model-list-view'

