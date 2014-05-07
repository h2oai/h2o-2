Steam.ApplicationContext = ->
  context$

    error: do edge$
    warn: do edge$
    info: do edge$
    fatal: do edge$
    notify: do edge$

    route: do edge$
    setRoute: do edge$
    getRoute: do edge$
    onRouteSucceeded: do edge$
    onRouteFailed: do edge$

    invokeH2O: do edge$

    requestFrames: do edge$
    requestFramesAndCompatibleModels: do edge$
    requestFrame: do edge$
    requestFrameAndCompatibleModels: do edge$
    requestScoringOnFrame: do edge$
    requestModels: do edge$
    requestModelsAndCompatibleFrames: do edge$
    requestModel: do edge$
    requestModelAndCompatibleFrames: do edge$

    # Local Storage
    putLocalObject: do edge$
    getLocalObject: do edge$
    getLocalObjects: do edge$
    deleteLocalObject: do edge$


    # Cache
    putIntoCache: do edge$
    getFromCache: do edge$
    removeFromCache: do edge$

    switchTopic: do edge$
    switchToFrames: do edge$
    switchToModels: do edge$
    switchToScoring: do edge$
    switchToNotifications: do edge$

    displayEmpty: do edge$

    loadFrames: do edge$
    framesLoaded: do edge$
    displayFrame: do edge$

    loadModels: do edge$
    displayModel: do edge$
    modelSelectionChanged: do edge$
    modelSelectionCleared: do edge$
    modelsSelected: do edge$
    modelsDeselected: do edge$
    deselectAllModels: do edge$
    clearModelSelection: do edge$

    loadNotifications: do edge$
    displayNotification: do edge$

    loadScorings: do edge$
    scoringsLoaded: do edge$
    displayScoring: do edge$
    scoringSelectionChanged: do edge$
    scoringSelectionCleared: do edge$
    scoringsSelected: do edge$
    scoringsDeselected: do edge$
    deselectAllScorings: do edge$
    clearScoringSelection: do edge$
    deleteScorings: do edge$
    deleteActiveScoring: do edge$


