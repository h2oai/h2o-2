Steam.TextMetrics = (_) ->
  _$box = null
  createMeasurementBox = ->
    unless _$box
      $container = $ document.createElement 'div'
      $container.attr 'style', 'position:absolute;left:0;top:0;width:0;height:0;overflow:hidden'
      $box = $ document.createElement 'span'
      $box.attr 'style', 'font-size:11px'

      $container.append $box
      $('body').append $container

      _$box = $box
    _$box

  measureTextWidth = (text) ->
    $box = createMeasurementBox()
    $box.text text
    $box.width()


  link$ _.measureTextWidth, measureTextWidth
