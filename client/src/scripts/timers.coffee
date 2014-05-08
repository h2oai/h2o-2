Steam.Timers = (_) ->
  _timers = {}

  link$ _.schedule, (ms, func) ->
    throw new Error 'Not a function' unless isFunction func
    if timer = _timers[ms]
      push timer.functions, func
    else
      tick = ->
        for f in _timers[ms].functions when f
          do f
        return
      timerId = setInterval tick, ms
      _timers[ms] =
        id: timerId
        functions: [ func ]

  link$ _.unschedule, (ms, func) ->
    if timer = _timers[ms]
      if func
        remove timer.functions, func 
        if timer.functions.length is 0
          clearInterval timer.id
          delete _timers[ms]
      else
        clearInterval timer.id
        delete _timers[ms]
    return


    

