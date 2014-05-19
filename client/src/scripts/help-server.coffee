Steam.HelpServer = (_) ->
  _helpContents = null

  initialize = ->
    [h1, p, ul, li, icon, span] = geyser.generate words 'h1 p ul li i.fa.fa-question-circle span'

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

  link$ _.loadHelp, (id) ->
    if id
      _.displayHelp()
    else
      _.displayHelp _helpContents

  initialize()

