widgetsApp = angular.module('h2o.widgets', [])

widgetsApp.directive "slider", () ->
    restrict: "E"
    template: "<div class=\"slider\"></div>"
    replace: true
    link: (scope, element, attrs) ->
        tooltipValue = (value) ->
            scope[attrs.tooltipValue] value

        jqElement = $(element)

        hasTooltip = attrs.tooltipValue?
        theTooltip = null

        dragSliderArgs = 
            animate: true
            min: 0
            max: 0
            value: 0
            step: 1
            change: (event, ui) ->
                newOffset = ui.value

                return if newOffset == scope[attrs.offset]

                scope.$apply () ->
                    scope[attrs.offset] = newOffset
                    scope[attrs.rsChange]()

        if hasTooltip
            $.extend dragSliderArgs,
                start: (event, ui) ->
                    jqHandleElement.tooltipster("show")
                slide: (event, ui) ->
                    jqHandleElement.tooltipster("update", tooltipValue ui.value)
                    jqHandleElement.tooltipster("reposition")
                stop: (event, ui) ->
                    jqHandleElement.tooltipster("hide")

        slider = jqElement.dragslider dragSliderArgs
        jqHandleElement = $(jqElement.find("a"))

        if hasTooltip
            jqHandleElement.tooltipster
                delay: 0
                speed: 100
                updateAnimation: false
                trigger: "custom"
                content: tooltipValue(0)

        updateSlider = () ->
            slider.dragslider 
                min: scope[attrs.from]
                max: scope[attrs.to]
                value: scope[attrs.offset]
                disabled: scope[attrs.disabled]

        scope.$watch attrs.from, updateSlider
        scope.$watch attrs.to, updateSlider
        scope.$watch attrs.offset, updateSlider
        scope.$watch attrs.disabled, updateSlider

# widgetsApp.directive "rangeslider", () ->
#     restrict: "E"
#     template: "<div class=\"slider\"></div>"
#     replace: true
#     link: (scope, element, attrs) ->
#         sliderElement = jqElement.dragslider
#             animate: true
#             min: 0
#             max: 0
#             values: [0, 0]
#             step: 1
#             range: true
#             rangeDrag: true
#             change: (event, ui) ->
#                 newOffset = ui.values[0]
#                 newLimit = ui.values[1] - newOffset

#                 return if not newLimit
#                 return if newOffset == scope[attrs.offset] and newLimit == scope[attrs.limit]

#                 scope.$apply () ->
#                     scope[attrs.offset] = newOffset
#                     scope[attrs.limit] = newLimit
#                     scope[attrs.rsChange]()

#         updateSlider = () ->
#             sliderElement.dragslider 
#                 min: scope[attrs.from]
#                 max: scope[attrs.to]
#                 values: [scope[attrs.offset], scope[attrs.offset]+scope[attrs.limit]]

#         scope.$watch attrs.from, updateSlider
#         scope.$watch attrs.to, updateSlider
#         scope.$watch attrs.offset, updateSlider
#         scope.$watch attrs.limit, updateSlider
    