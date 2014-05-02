test 'FrameListView should display all frames when no predicate is applied', (t) ->
  _ = Steam.ApplicationContext()
  Steam.Xhr _
  Steam.H2OProxy _
  frameListView = Steam.FrameListView _

  link$ _.framesLoaded, ->
    t.equal frameListView.items().length, 3, 'frameListView.items() array lengths match'
    t.equal frameListView.items()[0].title, 'airlines_test.hex', 'String frameListView.items()[0].title equals [airlines_test.hex]'
    t.equal frameListView.items()[0].caption, '13 columns', 'String frameListView.items()[0].caption equals [13 columns]'
    t.equal frameListView.items()[0].cutline, 'fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded', 'String frameListView.items()[0].cutline equals [fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded]'
    t.ok (isFunction frameListView.items()[0].display), 'frameListView.items()[0].display is a function'
    t.equal frameListView.items()[0].isActive(), true, 'Boolean frameListView.items()[0].isActive() equals [true]'
    t.equal frameListView.items()[1].title, 'airlines_train.hex', 'String frameListView.items()[1].title equals [airlines_train.hex]'
    t.equal frameListView.items()[1].caption, '13 columns', 'String frameListView.items()[1].caption equals [13 columns]'
    t.equal frameListView.items()[1].cutline, 'fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded', 'String frameListView.items()[1].cutline equals [fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded]'
    t.ok (isFunction frameListView.items()[1].display), 'frameListView.items()[1].display is a function'
    t.equal frameListView.items()[1].isActive(), false, 'Boolean frameListView.items()[1].isActive() equals [false]'
    t.equal frameListView.items()[2].title, 'prostate.hex', 'String frameListView.items()[2].title equals [prostate.hex]'
    t.equal frameListView.items()[2].caption, '9 columns', 'String frameListView.items()[2].caption equals [9 columns]'
    t.equal frameListView.items()[2].cutline, 'ID, CAPSULE, AGE, RACE, DPROS, DCAPS, PSA, VOL, GLEASON', 'String frameListView.items()[2].cutline equals [ID, CAPSULE, AGE, RACE, DPROS, DCAPS, PSA, VOL, GLEASON]'
    t.ok (isFunction frameListView.items()[2].display), 'frameListView.items()[2].display is a function'
    t.equal frameListView.items()[2].isActive(), false, 'Boolean frameListView.items()[2].isActive() equals [false]'
    t.equal frameListView.predicateCaption(), 'Showing\nall datasets', 'String frameListView.predicateCaption() equals [Showing\nall datasets]'
    t.ok (isFunction frameListView.clearPredicate), 'frameListView.clearPredicate is a function'
    t.equal frameListView.canClearPredicate(), false, 'Boolean frameListView.canClearPredicate() equals [false]'
    t.equal frameListView.hasItems(), true, 'Boolean frameListView.hasItems() equals [true]'
    t.equal frameListView.template, 'frame-list-view', 'String frameListView.template equals [frame-list-view]'
    t.end()

  _.loadFrames type: 'all'

test 'FrameListView should display compatible frames when a model predicate is applied', (t) ->
  _ = Steam.ApplicationContext()
  Steam.Xhr _
  Steam.H2OProxy _
  _.requestFrameAndCompatibleModels 'airlines_train.hex', (error, data) ->
    throw Error if error
    frame = head data.frames
    model = head frame.compatible_models
    frameListView = Steam.FrameListView _
    link$ _.framesLoaded, ->
      t.equal frameListView.items().length, 2, 'frameListView.items() array lengths match'
      t.equal frameListView.items()[0].title, 'airlines_test.hex', 'String frameListView.items()[0].title equals [airlines_test.hex]'
      t.equal frameListView.items()[0].caption, '13 columns', 'String frameListView.items()[0].caption equals [13 columns]'
      t.equal frameListView.items()[0].cutline, 'fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded', 'String frameListView.items()[0].cutline equals [fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded]'
      t.ok (isFunction frameListView.items()[0].display), 'frameListView.items()[0].display is a function'
      t.equal frameListView.items()[0].isActive(), true, 'Boolean frameListView.items()[0].isActive() equals [true]'
      t.equal frameListView.items()[1].title, 'airlines_train.hex', 'String frameListView.items()[1].title equals [airlines_train.hex]'
      t.equal frameListView.items()[1].caption, '13 columns', 'String frameListView.items()[1].caption equals [13 columns]'
      t.equal frameListView.items()[1].cutline, 'fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded', 'String frameListView.items()[1].cutline equals [fYear, fMonth, fDayofMonth, fDayOfWeek, DepTime, ArrTime, UniqueCarrier, Origin, Dest, Distance, IsDepDelayed, IsDepDelayed_REC, IsDepDelayed_REC_recoded]'
      t.ok (isFunction frameListView.items()[1].display), 'frameListView.items()[1].display is a function'
      t.equal frameListView.items()[1].isActive(), false, 'Boolean frameListView.items()[1].isActive() equals [false]'
      t.equal frameListView.predicateCaption(), 'Showing datasets compatible with\ndl_AirlinesTrain_binary_1', 'String frameListView.predicateCaption() equals [Showing datasets compatible with\ndl_AirlinesTrain_binary_1]'
      t.ok (isFunction frameListView.clearPredicate), 'frameListView.clearPredicate is a function'
      t.equal frameListView.canClearPredicate(), true, 'Boolean frameListView.canClearPredicate() equals [true]'
      t.equal frameListView.hasItems(), true, 'Boolean frameListView.hasItems() equals [true]'
      t.equal frameListView.template, 'frame-list-view', 'String frameListView.template equals [frame-list-view]'
      t.end()
    _.loadFrames type: 'compatibleWithModel', modelKey: model.key

