$.fn.isOnScreen = (scrollTop, scrollLeft, windowWidth, windowHeight) ->
    scrollRight     = scrollLeft + windowWidth;
    scrollBottom    = scrollTop + windowHeight;
     
    bounds          = this.offset()

    if @is(':visible')
        bounds.right    = bounds.left + @outerWidth()
        bounds.bottom   = bounds.top + @outerHeight()
    else
        bounds.right    = bounds.left
        bounds.bottom   = bounds.top
 
    !(scrollRight < bounds.left || scrollLeft > bounds.right || scrollBottom < bounds.top || scrollTop > bounds.bottom)