Steam.ApplicationContext = ->
  context$

    route: do edge$
    setRoute: do edge$
    getRoute: do edge$
    onRouteSucceeded: do edge$
    onRouteFailed: do edge$

    requestJSON: do edge$

    requestFrames: do edge$
    requestFramesAndCompatibleModels: do edge$
    requestFrame: do edge$
    requestFrameAndCompatibleModels: do edge$
    requestScoringOnFrame: do edge$
    requestModels: do edge$
    requestModelsAndCompatibleFrames: do edge$
    requestModel: do edge$
    requestModelAndCompatibleFrames: do edge$

    switchTopic: do edge$
    displayFrames: do edge$
    displayFrame: do edge$
    displayModels: do edge$
    displayModel: do edge$

