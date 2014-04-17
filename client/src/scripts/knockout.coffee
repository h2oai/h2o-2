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
#
# Registering a callback on the disposal of an element
# 
# To register a function to run when a node is removed, you can call ko.utils.domNodeDisposal.addDisposeCallback(node, callback). As an example, suppose you create a custom binding to instantiate a widget. When the element with the binding is removed, you may want to call the destroy method of the widget:
# 
# ko.bindingHandlers.myWidget = {
#     init: function(element, valueAccessor) {
#         var options = ko.unwrap(valueAccessor()),
#             $el = $(element);
#  
#         $el.myWidget(options);
#  
#         ko.utils.domNodeDisposal.addDisposeCallback(element, function() {
#             // This will be called when the element is removed by Knockout or
#             // if some other part of your code calls ko.removeNode(element)
#             $el.myWidget("destroy");
#         });
#     }
# };
# 

ko.bindingHandlers.json =
  init: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    data = ko.unwrap valueAccessor()

    $(element).text JSON.stringify data, null, 2

ko.bindingHandlers.geyser =
  update: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    if data = ko.unwrap valueAccessor()
      $(element).html geyser.render data


ko.bindingHandlers.collapse =
  init: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    angleDown = 'fa-angle-down'
    angleRight = 'fa-angle-right'
    isCollapsed = ko.unwrap valueAccessor()
    disclosureEl = document.createElement 'i'
    disclosureEl.className = 'fa'
    element.appendChild disclosureEl
    $el = $ element
    $nextEl = $el.next()
    $disclosureEl = $ disclosureEl
    toggle = ->
      if isCollapsed
        $disclosureEl
          .removeClass angleDown
          .addClass angleRight
        $nextEl.hide()
      else
        $disclosureEl
          .removeClass angleRight
          .addClass angleDown
        $nextEl.show()
      isCollapsed = not isCollapsed

    $el.css 'cursor', 'pointer'
    $disclosureEl.css 'margin-left', '10px'
    $el.click toggle
    toggle()
    ko.utils.domNodeDisposal.addDisposeCallback element, ->
      $el.off 'click'
    return

