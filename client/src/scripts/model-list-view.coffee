Steam.ModelListView = (_) ->
  _items = do nodes$

  activate = (item) ->
    for other in _items()
      if other is item
        other.isActive yes
      else
        other.isActive no

    _.displayModel item.key, item.data

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
      cutline: key
      display: -> activate self
      isActive: node$ no

  displayModels = (modelTable) ->
    _items items = mapWithKey modelTable, createItem
    activate head items unless isEmpty items

  displayModels_ = link$ _.displayModels, (data) ->
    _.requestAllModels (error, data) ->
      if error
        #TODO handle errors
      else
        displayModels data.models

  items: _items
  dispose: -> unlink$ displayModels_

