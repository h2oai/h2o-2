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


ko.bindingHandlers.json =
  init: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    data = ko.unwrap valueAccessor()

    $(element).text JSON.stringify data, null, 2

ko.bindingHandlers.geyser =
  update: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    if data = ko.unwrap valueAccessor()
      $(element).html geyser.render data





