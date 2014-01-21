\name{ncol}
\alias{ncol}
\title{
Returns a count of the number of columns in an code{\linkS4class{H2OParsedData} object. 
}
\description{
Returns a count of the number of columns in an code{\linkS4class{H2OParsedData} object.
}
\usage{
ncol(x)
}

\arguments{
  \item{x}{H2O parsed data object}
}

\value{ An integer for the number of columns in an H2O parsed data object.}
\examples{
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
ncol(iris.hex)
}

