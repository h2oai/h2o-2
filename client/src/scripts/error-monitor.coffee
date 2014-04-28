Steam.ErrorMonitor = (_) ->

  if window
    window.onerror = (message, url, lineNumber) ->
      _.fatal message,
        url: url
        lineNumber: lineNumber


