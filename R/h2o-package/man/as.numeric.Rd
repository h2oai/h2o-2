\name{as.numeric.H2OParsedData}
\alias{as.numeric.H2OParsedData}
\title{Converts a enum or factor column from to a numeric or integer column.}
\description{ 
	Specify a column of type enum (or a factor column) to convert to a numeric (or integer) column.
	}
\usage{
  \method{as.numeric}{H2OParsedData}(x, \dots)
}

\arguments{
  \item{x}{A column in an object of class \code{\linkS4class{H2OParsedData}}, or data frame.}
  \item{\dots}{further arguments passed to or from other methods. (Currently ignored).}
}
\value{
Returns the original object of class \code{\linkS4class{H2OParsedData}}, with the requested column specified as a numeric, rather than factor. 
}
\examples{
library(h2o)
localH2O = h2o.init()
prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile(localH2O, path = prosPath)

prostate.hex[,2] = as.factor(prostate.hex[,2])
summary(prostate.hex)
prostate.hex[,2] = as.numeric.H2OParsedData(prostate.hex[,2])
summary(prostate.hex)
}
