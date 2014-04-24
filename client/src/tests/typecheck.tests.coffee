describe 'Typecheck', ->

  s_undef = undefined
  s_null = null
  s_nan = Number.NaN
  s_0 = 0
  s_num = 42
  s_empty = ''
  s_str = 'foo'
  s_true = yes
  s_false = no
  s_arr = [1, 2, 3]
  s_func = -> yes
  s_err = new Error 'fail'
  s_date = new Date()
  s_regexp = /(.+)/g
  s_obj = {}

  s_all = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_nums = [ s_nan, s_0, s_num ]
  s_not_nums = [ s_undef, s_null, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_strs = [ s_empty, s_str ]
  s_not_strs = [ s_undef, s_null, s_nan, s_0, s_num, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_bools = [ s_true, s_false ]
  s_not_bools = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_arr, s_func, s_err, s_date, s_regexp, s_obj ]

  s_not_arrs = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_func, s_err, s_date, s_regexp, s_obj ]

  s_not_funcs = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_err, s_date, s_regexp, s_obj ]

  s_not_errs = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_date, s_regexp, s_obj ]

  s_not_dates = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_regexp, s_obj ]

  s_not_regexps = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_date, s_obj ]

  s_not_objs = [ s_undef, s_null, s_nan, s_0, s_num, s_empty, s_str, s_true, s_false, s_arr, s_func, s_err, s_date, s_regexp ]

  checkValid = (values, type) ->
    for value in values
      ok t_c value, type
    return

  checkInvalid = (values, type) ->
    for value in values
      notOk t_c value, type
    return

  check = (validValues, invalidValues, type) ->
    checkValid validValues, type
    checkInvalid invalidValues, type

  it 't_num', ->
    type = foo: t_num
    check s_nums, s_not_nums, type
  it 't_str', ->
    type = foo: t_str
    check s_strs, s_not_strs, type
  it 't_bool', ->
    type = foo: t_bool
    check s_bools, s_not_bools, type
  it 't_arr', ->
    type = foo: t_arr
    check [s_arr], s_not_arrs, type
  it 't_func', ->
    type = foo: t_func
    check [s_func], s_not_funcs, type
  it 't_err', ->
    type = foo: t_err
    check [s_err], s_not_errs, type
  it 't_date', ->
    type = foo: t_date
    check [s_date], s_not_dates, type
  it 't_regexp', ->
    type = foo: t_regexp
    check [s_regexp], s_not_regexps, type

