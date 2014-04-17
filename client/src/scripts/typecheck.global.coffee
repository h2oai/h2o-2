Steam.Typecheck = do ->
  t_num = ->
  t_str = ->
  t_bool = ->
  t_arr = ->
  t_func = ->
  t_err = ->
  t_date = ->
  t_regexp = ->

  format = (name, val, expected) ->
    "#{name} = <#{val}> : expecting #{expected}"

  validateNative = (name, value, validate, typeName) ->
    if validate value
      undefined
    else
      format name, value, typeName

  validate = (name, value, type) ->
    switch type
      when t_num
        validateNative name, value, isNumber, 'number'
      when t_str
        validateNative name, value, isString, 'string'
      when t_bool
        validateNative name, value, isBoolean, 'boolean'
      when t_arr
        validateNative name, value, isArray, 'array'
      when t_func
        validateNative name, value, isFunction, 'function'
      when t_err
        validateNative name, value, isError, 'error'
      when t_date
        validateNative name, value, isDate, 'date'
      when t_regexp
        validateNative name, value, isRegExp, 'regexp'
      else
        throw new Error "Unknown type [#{type}]"

  t_c = (value, def) ->
    [ name, type ] = head pairs def
    if message = validate name, value, type
      console.warn message
      no
    else
      yes

  t_num: t_num
  t_str: t_str
  t_bool: t_bool
  t_arr: t_arr
  t_func: t_func
  t_err: t_err
  t_date: t_date
  t_regexp: t_regexp
  t_c: t_c

{ t_num, t_str, t_bool, t_arr, t_func, t_err, t_date, t_regexp, t_c } = Steam.Typecheck
