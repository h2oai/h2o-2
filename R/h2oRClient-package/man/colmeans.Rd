\name{colMeans}
\alias{colMeans}

\title{
Returns a vector of the column means. 
}
\description{
Returns a vector of column names for an \code{\linkS4class{H2OParsedData} object. 
}
\usage{
colMeans(x)
}
\arguments{
  \item{x}{An H2O parsed data object containing real, continuious data.}
}

\value{Returns a vector of the arithmetic mean of the requested columns as computed by H2O.
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
 + silentUpgrade = TRUE, promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
colMeans(iris.hex[,2:4])
}
  
