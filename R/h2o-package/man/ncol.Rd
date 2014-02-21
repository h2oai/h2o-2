\name{ncol}
\alias{ncol}
\title{
Number of Columns of a H2O Dataset  
}
\description{
Returns a count of the number of columns in an \code{\linkS4class{H2OParsedData}} object.
}
\usage{
ncol(x)
}

\arguments{
  \item{x}{An \code{\linkS4class{H2OParsedData}} object.}
}

\value{ An integer count of the number of columns.}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
irisPath = system.file("extdata", "iris.csv", package="h2o")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
ncol(iris.hex)
h2o.shutdown(localH2O)
}

