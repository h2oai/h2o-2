#
# Reactive programming / Dataflow programming wrapper over KO
#

Steam.Hypergraph = do ->

  createEdge = ->
    arrow = null

    self = (args...) ->
      if arrow
        apply arrow.func, args
      else
        undefined

    self.subscribe = (func) ->
      console.assert isFunction func
      if arrow
        throw new Error 'Cannot re-attach edge'
      else
        arrow =
          func: func
          dispose: -> arrow = null

    self.dispose = ->
      arrow.dispose() if arrow

    self

  createPolyedge = ->
    arrows = []

    self = (args...) ->
      map arrows, (arrow) -> apply arrow.func, args

    self.subscribe = (func) ->
      console.assert isFunction func
      arrows.push arrow =
        func: func
        dispose: -> remove arrows, arrow
      arrow

    self.dispose = ->
      forEach (copy arrows), (arrow) -> arrow.dispose()

    self

  if ko?
    createObservable = ko.observable
    createObservableArray = ko.observableArray
  else
    createObservable = (initialValue) ->
      arrows = []
      currentValue = initialValue

      notifySubscribers = (arrows, newValue) ->
        for arrow in arrows
          arrow.func newValue
        return

      self = (newValue) ->
        if arguments.length is 0
          currentValue
        else
          unchanged = if self.equalityComparer
            self.equalityComparer currentValue, newValue
          else
            currentValue is newValue

          unless unchanged
            currentValue = newValue
            notifySubscribers arrows, newValue

      self.subscribe = (func) ->
        console.assert isFunction func
        arrows.push arrow =
          func: func
          dispose: -> remove arrows, arrow
        arrow

      self

    createObservableArray = createObservable

  createNode = (value, equalityComparer) ->
    if arguments.length is 0
      createNode undefined, never
    else
      observable = createObservable value
      observable.equalityComparer = equalityComparer if isFunction equalityComparer
      observable

  createPolynode = (array) -> createObservableArray array or []

  createBundle = (edges, parent=null) ->
    bundle = root: null, parent: null

    if parent
      bundle.root = parent.root
      bundle.parent = parent
    else
      # This is the root
      bundle.root = bundle.parent = bundle

    for name, edge of edges
      console.assert isFunction edge
      console.assert not(name of bundle)

      #TODO Policy injection for debugging
      bundle[name] = edge

    bundle

  link = (node, func) -> node.subscribe func

  unlink = (arrows) ->
    if isArray arrows
      for arrow in arrows
        arrow.dispose()
    else
      arrows.dispose()


  #
  # Combinators
  #

  _apply = (sources, func) ->
    apply func, map sources, (source) -> source()

  callOnChange = (sources..., func) ->
    func()
    map sources, (source) ->
      link source, -> func()

  applyOnChange = (sources..., func) ->
    _apply sources, func
    map sources, (source) ->
      link source, -> _apply sources, func

  joinNodes = (sources..., target, func) ->
    target _apply sources, func
    map sources, (source) ->
      link source, -> target _apply sources, func

  zipNodes = (sources, func) ->
    evaluate = -> _apply sources, func
    target = createNode evaluate()
    map sources, (source) ->
      link source, -> target evaluate()
    target

  liftNodes = (sources..., func) ->
    zipNodes sources, func

  filterNode = (source, predicate) ->
    target = createNode if predicate value = source() then value else undefined
    link source, (value) -> target value if predicate value
    target

  debounceNode = (source, wait, options) ->
    target = createNode undefined
    link source, debounce target, wait, options
    target

  throttleNode = (source, wait, options) ->
    target = createNode undefined
    link source, throttle target, wait, options
    target
 
  createEdge: createEdge
  createPolyedge: createPolyedge
  createNode: createNode
  createPolynode: createPolynode
  createBundle: createBundle
  link: link
  unlink: unlink
  callOnChange: callOnChange
  applyOnChange: applyOnChange
  joinNodes: joinNodes
  zipNodes: zipNodes
  liftNodes: liftNodes
  filterNode: filterNode
  debounceNode: debounceNode
  throttleNode: throttleNode


#
# Destructure into application scope with shorter names.
#

{ createEdge: edge$, createPolyedge: edges$, createNode: node$, createPolynode: nodes$, createBundle: bundle$, link: link$, unlink: unlink$, callOnChange: call$, applyOnChange: apply$, joinNodes: join$, zipNodes: zip$, liftNodes: lift$, filterNode: filter$, debounceNode: debounce$, throttleNode: throttle$ } = Steam.Hypergraph


#
# Common combinators easily expressed with zip$().
# 

if$ = (condition, valueIfTrue, valueIfFalse) ->
  zip$ [condition, valueIfTrue, valueIfFalse], (c, t, f) -> if c then t else f

and$ = (sources...) ->
  zip$ sources, (values...) -> every values

or$ = (sources...) ->
  zip$ sources, (values...) -> some values

not$ = (source) ->
  zip$ [source], negate


