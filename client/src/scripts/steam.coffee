
unless exports?
  getJSONFromServer = (uri, go) ->
    $.getJSON uri
      .done (data, status, xhr) ->
        go null, status: status, data: data, xhr: xhr
      .fail (xhr, status, error) ->
        go status: status, error: error, xhr: xhr

  getJSONFromServer '/2/Frames.json', (error, response) ->
    if error
      console.debug error
    else
      console.debug response

  getJSONFromServer '/2/Models.json', (error, response) ->
    if error
      console.debug error
    else
      console.debug response

