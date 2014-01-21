\name{colnames}
\alias{colnames}
\title{ Returns column names for a parsed H2O data object.}
\description{Returns column names for an \code{\linkS4class{H2OParsedData} object.}
\usage{
colnames(x)
}
\arguments{
  \item{x}{ An\code{\linkS4class{H2OParsedData}} object. 
}

\value{Returns a vector of column names. 
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, 
 + promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
summary(iris.hex)
colnames(iris.hex)
}

