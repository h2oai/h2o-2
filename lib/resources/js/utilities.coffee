utilitiesApp = angular.module('h2o.utilities', [])

utilitiesApp.directive "blurOnEnter", () ->
    restrict: 'A',
    link: (scope, element, attrs) ->
        return if attrs.type == 'radio' or attrs.type == 'checkbox'

        jqElement = $(element)

        element.bind 'keydown', (e) ->
            jqElement.blur() if e.which == 13

utilitiesApp.filter "humanizeFilesize", () ->
    (input) ->
        humanize.filesize(input)

utilitiesApp.filter "minZero", () ->
    (input) ->
        if input < 0 then 0 else input


unless Array::filter
  Array::filter = (callback) ->
    element for element in this when callback(element)

unless Array::sum
  Array::sum = () ->
    result = 0
    for i in [0...@length]
        result += @[i]
    result