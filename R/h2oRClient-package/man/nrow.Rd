\name{nrow}
\alias{nrow}
\title{
Returns a count of rows for an code{\linkS4class{H2OParsedData} object. 
}}
\description{
Returns a count of rows for an code{\linkS4class{H2OParsedData} object
}}
\usage{
nrow(x)
}
\arguments{
  \item{x}{code{\linkS4class{H2OParsedData} object
}}
}
\value{ An integer count of the number of rows 
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
nrow(iris.hex)
h2o.shutdown(localH2O)
}

