\name{h2o-package}
\alias{h2o-package}
\alias{h2o}
\docType{package}
\title{
H2O R Interface
}
\description{
This is a package for running H2O via its REST API from within R.
}
\details{
\tabular{ll}{
Package: \tab h2o\cr
Type: \tab Package\cr
Version: \tab 0.3\cr
Date: \tab 2013-07-16\cr
License: \tab GPL-2\cr
Depends: \tab RCurl, rjson\cr
}
This package allows the user to run basic H2O commands using R commands. In order to use it, you must first have H2O running on a server (See \href{http://0xdata.com/h2o/docs/}{How to Start H2O}). Load the library and create a \code{\linkS4class{H2OClient}} object with the server IP and port. The command \code{\link{importFile}} will import and parse a delimited data file, returning an \code{\linkS4class{H2OParsedData}} object.

H2O supports a number of standard statistical models, such as GLM, K-means, and random forest classification. For example, to run GLM, call \code{\link{h2o.glm}} with the parsed data and parameters (response variable, error distribution) as arguments. (The operation will be done on the server associated with the data object). 

Note that no actual data is stored in the workspace - R only saves the hex keys, which uniquely identify the data set, model, etc on the server. When the user makes a request, R queries the server via the REST API, which returns a JSON file with the relevant information that R then displays in the console.
}
\author{
Anqi Fu and Tom Kraljevic, with contributions from the 0xdata team

Maintainer: Anqi Fu <anqi@0xdata.com>
}
\references{
\itemize{
  \item \href{http://www.0xdata.com}{0xdata Homepage}
  \item \href{https://github.com/0xdata/h2o}{H2O on Github}
  }
}
~~ Optionally other standard keywords, one per line, from file KEYWORDS in the ~~
~~ R documentation directory ~~
\keyword{ package }
\seealso{
~~ Optional links to other man pages, e.g. ~~
~~ \code{\link[<pkg>:<pkg>-package]{<pkg>}} ~~
}
\examples{
prostate.hex = importURL(h2o, "https://raw.github.com/0xdata/h2o/master/smalldata/
logreg/prostate.csv", "prostate.hex")
summary(prostate.hex)
prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, 
family = "binomial", nfolds = 10, alpha = 0.5)
print(prostate.glm)
h2o.kmeans(data = prostate.hex, centers = 5)
}
