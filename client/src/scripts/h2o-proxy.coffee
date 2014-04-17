Steam.H2OProxy = (_) ->

  composeUri = (uri, parameters) ->
    if isEmpty parameters
      uri
    else
      uri + '?' + join parameters, '&'

  request = (uri, parameters, go) ->
    _.requestJSON (composeUri uri, parameters), (error, response) ->
      if error
        #TODO error logging / retries, etc.
        console.error error
        console.error response
        go error, response
      else
        go error, response.data

  requestFrames = (go, opts) ->
    parameters = []
    if opts
      #TODO typecheck opts
      push parameters, "key=#{opts.key}" if opts.key
      push parameters, 'find_compatible_models=true' if opts.find_compatible_models
    request '/2/Frames.json', parameters, go

  requestModels = (go, opts) ->
    parameters = []
    if opts
      #TODO typecheck opts
      push parameters, "key=#{opts.key}" if opts.key
      push parameters, 'find_compatible_frames=true' if opts.find_compatible_frames
    request '/2/Models.json', parameters, go

  link$ _.requestFrames, (go) -> requestFrames go
  link$ _.requestFramesAndCompatibleModels, (go) -> requestFrames go, find_compatible_models: yes
  link$ _.requestFrame, (key, go) -> requestFrames go, key: key
  link$ _.requestFrameAndCompatibleModels, (key, go) -> requestFrames go, key: key, find_compatible_models: yes
  link$ _.requestScoringOnFrame, (frameKey, modelKey, go) -> requestFrames go, key: frameKey, score_model: modelKey
  link$ _.requestModels, (go) -> requestModels go
  link$ _.requestModelsAndCompatibleFrames, (key, go) -> requestModels go, find_compatible_models: yes
  link$ _.requestModel, (key, go) -> requestModels go, key: key
  link$ _.requestModelAndCompatibleFrames, (key, go) -> requestModels go, key: key, find_compatible_frames: yes



