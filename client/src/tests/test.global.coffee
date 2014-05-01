testAsync = (done, func) ->
  try
    func()
    done()
  catch error
    done error

dumpAssertion = (path, value) ->
  if isUndefined value
    console.log "ok (isUndefined #{path}), '#{path} is not undefined'"
  else if value is null
    console.log "ok (null is #{path}), '#{path} is not null'"
  else if isString value
    escapedValue = value.replace(/\n/g, '\\n').replace(/\t/g, '\\t')
    console.log "equal #{path}, '#{escapedValue}', 'String #{path} does not equal <#{escapedValue}>'"
  else if isBoolean value
    console.log "equal #{path}, #{value}, 'Boolean #{path} does not equal <#{value}>'"
  else if isNumber value
    console.log "equal #{path}, #{value}, 'Number #{path} does not equal <#{value}>'"
  else if isRegExp value
    console.log "equal #{path}.toString(), #{value}.toString(), 'Regexp #{path} does not equal <#{value}>'"
  else if isDate value
    console.log "equal #{path}.toString(), #{value}.toString(), 'Date #{path} does not equal <#{value}>'"
  else if isFunction value
    console.log "ok (isFunction #{path}), '#{path} is not a function'"
  else if isArray value
    console.log "equal #{path}.length, #{value.length}, '#{path} array length mismatch'"
    for element, index in value
      dumpAssertion "#{path}[#{index}]", element
  else if isObject value
    dumpAssertions value, path
  else
    throw new Error "Cannot dump #{path}"
  return

dumpAssertions = (obj, name='subject') ->
  throw new Error 'Not an object' unless isObject obj
  for key, value of obj
    if isNode$ value
      dumpAssertion "#{name}.#{key}()", value()
    else
      dumpAssertion "#{name}.#{key}", value
  return

    


