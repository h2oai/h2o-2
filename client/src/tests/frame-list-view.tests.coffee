describe 'FrameListView', ->
  it 'should display all frames when no predicate is applied', (go) ->
    _ = Steam.ApplicationContext()
    Steam.Xhr _
    Steam.H2OProxy _
    frameListView = Steam.FrameListView _

    link$ _.framesLoaded, -> testAsync go, ->
      ok (frameListView.items() isnt null), 'frameListView.items() array is null'
      equal frameListView.items().length, 3, 'frameListView.items() array length mismatch'
      ok (frameListView.items()[0] isnt null), 'frameListView.items()[0] object is null'
      ok (frameListView.items()[0].data isnt null), 'frameListView.items()[0].data object is null'
      equal frameListView.items()[0].title, 'airlines_test.hex', 'frameListView.items()[0].title does not equal <airlines_test.hex>'
      equal frameListView.items()[0].caption, '13 columns', 'frameListView.items()[0].caption does not equal <13 columns>'
      equal frameListView.items()[0].cutline, 'fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded', 'frameListView.items()[0].cutline does not equal <fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded>'
      ok (isFunction frameListView.items()[0].display), 'frameListView.items()[0].display is not a function'
      equal frameListView.items()[0].isActive(), true, 'frameListView.items()[0].isActive() does not equal <true>'
      ok (frameListView.items()[1] isnt null), 'frameListView.items()[1] object is null'
      ok (frameListView.items()[1].data isnt null), 'frameListView.items()[1].data object is null'
      equal frameListView.items()[1].title, 'airlines_train.hex', 'frameListView.items()[1].title does not equal <airlines_train.hex>'
      equal frameListView.items()[1].caption, '13 columns', 'frameListView.items()[1].caption does not equal <13 columns>'
      equal frameListView.items()[1].cutline, 'fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded', 'frameListView.items()[1].cutline does not equal <fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded>'
      ok (isFunction frameListView.items()[1].display), 'frameListView.items()[1].display is not a function'
      equal frameListView.items()[1].isActive(), false, 'frameListView.items()[1].isActive() does not equal <false>'
      ok (frameListView.items()[2] isnt null), 'frameListView.items()[2] object is null'
      ok (frameListView.items()[2].data isnt null), 'frameListView.items()[2].data object is null'
      equal frameListView.items()[2].title, 'prostate.hex', 'frameListView.items()[2].title does not equal <prostate.hex>'
      equal frameListView.items()[2].caption, '9 columns', 'frameListView.items()[2].caption does not equal <9 columns>'
      equal frameListView.items()[2].cutline, 'ID, CAPSULE, AGE, RACE, DPROS, DCAPS, PSA, VOL, GLEASON', 'frameListView.items()[2].cutline does not equal <ID, CAPSULE, AGE, RACE, DPROS, DCAPS, PSA, VOL, GLEASON>'
      ok (isFunction frameListView.items()[2].display), 'frameListView.items()[2].display is not a function'
      equal frameListView.items()[2].isActive(), false, 'frameListView.items()[2].isActive() does not equal <false>'
      equal frameListView.predicateCaption(), 'Showing\nall datasets', 'frameListView.predicateCaption() does not equal <Showing\nall datasets>'
      ok (isFunction frameListView.clearPredicate), 'frameListView.clearPredicate is not a function'
      equal frameListView.canClearPredicate(), false, 'frameListView.canClearPredicate() does not equal <false>'
      equal frameListView.hasItems(), true, 'frameListView.hasItems() does not equal <true>'
      equal frameListView.template, 'frame-list-view', 'frameListView.template does not equal <frame-list-view>'

    _.loadFrames type: 'all'

  it 'should display compatible frames when a model predicate is applied', (go) ->
    _ = Steam.ApplicationContext()
    Steam.Xhr _
    Steam.H2OProxy _
    testAsync go, ->
      _.requestFrameAndCompatibleModels 'airlines_train.hex', (error, data) ->
        return go new Error error if error
        testAsync go, ->
          frame = head data.frames
          model = head frame.compatible_models
          frameListView = Steam.FrameListView _
          link$ _.framesLoaded, ->
            testAsync go, ->
              equal frameListView.items().length, 2, 'frameListView.items() array length mismatch'
              equal frameListView.items()[0].title, 'airlines_test.hex', 'String frameListView.items()[0].title does not equal <airlines_test.hex>'
              equal frameListView.items()[0].caption, '13 columns', 'String frameListView.items()[0].caption does not equal <13 columns>'
              equal frameListView.items()[0].cutline, 'fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded', 'String frameListView.items()[0].cutline does not equal <fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded>'
              ok (isFunction frameListView.items()[0].display), 'frameListView.items()[0].display is not a function'
              equal frameListView.items()[0].isActive(), true, 'Boolean frameListView.items()[0].isActive() does not equal <true>'
              equal frameListView.items()[1].title, 'airlines_train.hex', 'String frameListView.items()[1].title does not equal <airlines_train.hex>'
              equal frameListView.items()[1].caption, '13 columns', 'String frameListView.items()[1].caption does not equal <13 columns>'
              equal frameListView.items()[1].cutline, 'fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded', 'String frameListView.items()[1].cutline does not equal <fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded>'
              ok (isFunction frameListView.items()[1].display), 'frameListView.items()[1].display is not a function'
              equal frameListView.items()[1].isActive(), false, 'Boolean frameListView.items()[1].isActive() does not equal <false>'
              equal frameListView.predicateCaption(), 'Showing datasets compatible with\ndl_AirlinesTrain_binary_1', 'String frameListView.predicateCaption() does not equal <Showing datasets compatible with\ndl_AirlinesTrain_binary_1>'
              ok (isFunction frameListView.clearPredicate), 'frameListView.clearPredicate is not a function'
              equal frameListView.canClearPredicate(), true, 'Boolean frameListView.canClearPredicate() does not equal <true>'
              equal frameListView.hasItems(), true, 'Boolean frameListView.hasItems() does not equal <true>'
              equal frameListView.template, 'frame-list-view', 'String frameListView.template does not equal <frame-list-view>'
          _.loadFrames type: 'compatibleWithModel', modelKey: model.key

