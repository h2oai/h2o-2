
helpIndex =
  modelCategories:
    Binomial: 'model.category.binomial'
  modelMethods:
    DeepLearning: 'model.method.deep_learning'
    GBM: 'model.method.gbm'
    GLM: 'model.method.glm'
    DRF: 'model.method.drf'
    SpeeDRF: 'model.method.speed_drf'
  
Steam.FrameView = (_, _frame) ->
  createCompatibleModelItem = (model) ->
    key: model.key
    algorithm: model.model_algorithm
    category: model.model_category
    responseColumnName: model.response_column_name
    inspect: -> console.log model.key
    inspectAlgorithm: -> _.help helpIndex.modelMethods[model.model_algorithm]
    inspectCategory: -> _.help helpIndex.modelCategories[model.model_category]

  createColumnItem = (name) -> name: name

  loadCompatibleModels = ->
    _.switchToModels type: 'compatibleWithFrame', frameKey: _frame.key

  data: _frame
  key: _frame.key
  timestamp: _frame.creation_epoch_time_millis
  title: _frame.key
  columns: map _frame.column_names, createColumnItem
  columnCount: "(#{_frame.column_names.length})"
  compatibleModels: map _frame.compatible_models, createCompatibleModelItem
  compatibleModelsCount: "(#{_frame.compatible_models.length})"
  hasCompatibleModels: _frame.compatible_models.length > 0
  loadCompatibleModels: loadCompatibleModels
  isRawFrame: _frame.is_raw_frame
  parseUrl: "/2/Parse2.query?source_key=#{encodeURIComponent _frame.key}"
  dispose: ->
  template: 'frame-view'

