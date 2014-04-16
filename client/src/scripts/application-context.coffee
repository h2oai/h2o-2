Steam.ApplicationContext = ->
  context$

    route: do edge$
    setRoute: do edge$
    getRoute: do edge$
    onRouteSucceeded: do edge$
    onRouteFailed: do edge$

    requestJSON: do edge$

    requestAllFrames: do edge$
    requestFrame: do edge$
    requestFramesAndCompatibleModels: do edge$
    requestFrameAndCompatibleModels: do edge$
    requestScoringOnFrame: do edge$
    requestAllModels: do edge$
    requestModel: do edge$

    switchTopic: do edge$
    displayFrames: do edge$
    displayFrame: do edge$
    displayModels: do edge$
    displayModel: do edge$

