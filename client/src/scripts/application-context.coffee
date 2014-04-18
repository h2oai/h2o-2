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

    switchToFrames: do edge$
    switchToModels: do edge$
    loadFrames: do edge$
    displayFrame: do edge$
    loadModels: do edge$
    displayModel: do edge$
    modelSelectionChanged: do edge$
    modelsSelected: do edge$
    modelsDeselected: do edge$
    deselectAllModels: do edge$
    clearModelSelection: do edge$

