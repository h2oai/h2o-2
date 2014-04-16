#
# Custom Knockout.js binding handlers
#
# init:
#   This will be called when the binding is first applied to an element
#   Set up any initial state, event handlers, etc. here
#
# update:
#   This will be called once when the binding is first applied to an element,
#    and again whenever the associated observable changes value.
#   Update the DOM element based on the supplied values here.


ko.bindingHandlers.steamTable =
  init: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    data = ko.unwrap valueAccessor()

    [ table, thead, tbody, tr, th, td ] = geyser.generate words 'table thead tbody tr th td'
    ths = map data.headers, th
    trs = map data.rows, (row) -> tr map row, td
    $(element).html geyser.render table [ (thead ths), (tbody trs) ]

ko.bindingHandlers.steamGrid =
  init: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    data = ko.unwrap valueAccessor()

    [ table, tbody, tr, td ] = geyser.generate words 'table tbody tr td'
    trs = map data.rows, (row) -> tr map row, td
    #BUG this should work with scalars
    $(element).html geyser.render table [tbody trs]

ko.bindingHandlers.steamStringify =
  init: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    data = ko.unwrap valueAccessor()

    $(element).text JSON.stringify data, null, 2




