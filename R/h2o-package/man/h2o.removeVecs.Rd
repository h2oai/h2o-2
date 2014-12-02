\name{h2o.removeVecs}
\alias{h2o.removeVecs}
\title{Removes columns or vectors from H2OParsedData objects instead of making a copy of the data without the specified columns.}
\description{Allows users to remove columns from H2O objects. This call acts on the H2O server through the R console as well as update the associated named object in the R environment.}
\usage{
h2o.removeVecs(data, cols)
}
\arguments{
  \item{data}{ An\code{\linkS4class{H2OParsedData}} object.}
  \item{cols}{ A vector containing the names or indices of the data columns chosen to be removed.}
}

\seealso{
\code{\link{h2o.rm}}, \code{\link{cbind}}
}
\examples{
library(h2o)
localH2O = h2o.init()
prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile(localH2O, path = prosPath, key = "prostate.hex")

# Remove ID and GLEASON column from prostate data
prostate.hex = h2o.removeVecs(prostate.hex, c('ID', 'GLEASON'))
summary(prostate.hex)
}

