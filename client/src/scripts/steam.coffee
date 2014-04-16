Steam.Types = (_) ->
  
  TypeAddress =
    anthing: t_any
    boolean: t_bool
    date: t_date
    element: t_el
    function: t_function
    number: t_num
    integer: t_int
    regexp: t_regexp
    string: t_str
    error: t_error
    undef: t_undef
    optional_string: t_opt t_str
    array_int: [ t_int ]
    string_or_int: [ t_str, t_int ]
    tuple_3: t_tuple t_str, t_int, t_num
    custom_div_by_10: t_int (x) -> x % 10 is 0
    enumeration: t_str [10, 20, 30]
    opt_args: t_str archetype: 'email', maxlength: 100
    # each t_* for native types should accept variadic forms:
    # t_str = (array) ->
    # t_str = (function...) ->
    # t_str = (array, function...) ->
    # t_str = (obj) ->
    # t_str = (array, obj) ->
    # t_str = (array, obj, function...) ->
    #
    # -> array implies enum
    # -> object implies opts, including json-schema properties
    # -> function implies a validation function
    # Every schema/type definition should start with one and only
    # one property that indicates the name of the type.
    # e.g. validate a scalar:
    #     rtt.check 42, answer: t_int
    # e.g. validate an array:
    #     rtt.check countries, countries: [ t_string ]
    # e.g. validate multiple things: 
    #     rtt.check arguments, answer: t_int, countries: [t_string]
    # e.g. validate an address:
    #
    #     rtt.check mailingAddress,
    #       Address:
    #         line1: t_str
    #         line2: t_str
    #         city: t_str
    #         state: t_str
    #         zip: t_int
    #
    # things you can do with it:
    # Using faker.js
    #     fakeAddress = rtt.fake t_address
    # Generate json-schema
    #     addressSchema = rtt.toJsonSchema t_address
    #

# TODO
# link$ _.onRouteFailed, (address) -> console.error "Not found: #{address}"
$ ->

  window.steam = steam = Steam.Application do Steam.ApplicationContext
  ko.applyBindings steam


