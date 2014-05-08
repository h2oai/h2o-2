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

ko.bindingHandlers.paragraph =
  update: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    if data = ko.unwrap valueAccessor()
      if -1 isnt data.indexOf '\n'
        ko.utils.setHtml element, "<span>#{data.replace /\n/g, '<br/>'}</span>"
      else
        ko.utils.setTextContent element, data
    else
      ko.utils.setTextContent element, ''

ko.bindingHandlers.json =
  init: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    data = ko.unwrap valueAccessor()

    $(element).text JSON.stringify data, null, 2

ko.bindingHandlers.geyser =
  update: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    if data = ko.unwrap valueAccessor()
      $element = $ element
      markup = data.markup or data
      $element.html geyser.render markup
      if data.behaviors
        for behavior in data.behaviors
          behavior $element
    else
      $(element).text 'Loading. Please wait..'
    return

ko.bindingHandlers.icon =
  update: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    if icon = ko.unwrap valueAccessor()
      element.className = "fa fa-#{icon.image}"
      element.style.color = if icon.color then icon.color else null
      element.title = if icon.caption then icon.caption else ''
    return

ko.bindingHandlers.tooltip =
  update: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    title = ko.unwrap valueAccessor()

    #HACK simply setting a new title without calling 'destroy' does not update the tooltip.
    $(element).tooltip 'destroy'
    if title
      $(element).tooltip title: title

    #TODO can remove this callback if ko/jquery are disposing the tooltip's bindings properly.
    ko.utils.domNodeDisposal.addDisposeCallback element, ->
      $(element).tooltip 'destroy'

timeagoUpdateInterval = 60000
momentTimestampFormat = 'MMMM Do YYYY, h:mm:ss a'
ko.bindingHandlers.timeago =
  init: (element, valueAccessor, allBindings, viewModel, bindingContext) ->
    timestamp = ko.unwrap valueAccessor()
    timestamp = parseInt timestamp if isString timestamp
    $element = $ element
    date = moment new Date timestamp
    $element.attr 'title', date.format momentTimestampFormat
    tick = ->
      label = date.fromNow()
      if $element.text() isnt label
        $element.text label
      return

    if window.steam
      window.steam.context.schedule timeagoUpdateInterval, tick

      ko.utils.domNodeDisposal.addDisposeCallback element, ->
        window.steam.context.unschedule timeagoUpdateInterval, tick

    tick()
    return

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
    throw new Error 'No collapsible sibling found' unless $nextEl.length
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
    $el.attr 'title', 'Click to expand/collapse'
    $disclosureEl.css 'margin-left', '10px'
    $el.click toggle
    toggle()
    ko.utils.domNodeDisposal.addDisposeCallback element, ->
      $el.off 'click'
    return

