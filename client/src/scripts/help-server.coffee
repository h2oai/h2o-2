Steam.HelpServer = (_) ->
  _helpContents = null

  initialize = ->
    [h1, p, ul, li, icon, span] = geyser.generate words 'h1 p ul li i.fa.fa-question-circle span'

    #TODO obsolete
    _helpContents = [
      h1 'Welcome to H<sub>2</sub>O'
      p 'H<sub>2</sub>O by 0xdata brings better algorithms to big data. H<sub>2</sub>O is the open source math and machine learning platform for speed and scale. With H<sub>2</sub>O, enterprises can use all of their data (instead of sampling) in real-time for better predictions. Data Scientists can take both simple and sophisticated models to production from H<sub>2</sub>O the same interactive platform used for modeling, within R and JSON. H<sub>2</sub>O is also used as an algorithms library for Making Hadoop Do Math.'
      h1 'User Guide'
      ul [
        li [ icon(), span ' General' ]
        li [ icon(), span ' Data' ]
        li [ icon(), span ' Model' ]
        li [ icon(), span ' Score' ]
        li [ icon(), span ' Administration' ]
      ]
      h1 'Walkthroughs'
      ul [
        li [ icon(), span ' GLM' ]
        li [ icon(), span ' GLM Grid' ]
        li [ icon(), span ' K Means' ]
        li [ icon(), span ' Random Forest' ]
        li [ icon(), span ' PCA' ]
        li [ icon(), span ' GBM' ]
        li [ icon(), span ' GBM Grid' ]
      ]
    ]

  createHelpView = (title, content) ->
    title: title
    content: content
    template: 'help'

  link$ _.help, (id) ->
    if id
      entry = Steam.Help[id]
      if entry
        content = entry.content or 'No help available.'
        $content = $ "<div>#{content}</div>"
        $('a', $content).each ->
          $self = $ @
          url = $self.attr 'href'
          if url
            if 0 is url.indexOf 'help:'
              # Set link to call back into the help routine.
              $self.removeAttr 'href'
              $self.click -> _.help url.substr 1 + url.indexOf ':'
            else
              # Set link without ids to open in a new window.
              $self.attr 'target', '_blank'

        _.inspect createHelpView entry.title, $content[0]
      else
        _.help()
    else
      _.inspect createHelpView 'Help not found', $ '<div>Could not find help content for this item.</div>'
    return

  link$ _.loadHelp, (id) ->
    if id
      _.displayHelp()
    else
      _.displayHelp _helpContents

  initialize()

