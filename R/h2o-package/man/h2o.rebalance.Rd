\name{h2o.rebalance}
\alias{h2o.rebalance}

\title{
Rebalance a H2O data frame
}
\description{
Repartition an existing H2O data set into given number of chunks (per Vec), for load-balancing. Does not alter data.
}
\usage{
  h2o.rebalance(data, key, chunks)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{An \code{\linkS4class{H2OParsedData}} object representing the dataset to split.}
  \item{key}{Destination key}
  \item{chunks}{A numeric value indicating how many chunks to split the dataset into.}
}
\value{
Returns the rebalanced object of class \code{\linkS4class{H2OParsedData}}.
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
irisPath = system.file("extdata", "iris.csv", package = "h2o")
iris.hex = h2o.importFile(localH2O, path = irisPath)
iris.reb = h2o.rebalance(iris.hex, key="iris.hex.rebalanced", chunks = 32)
summary(iris.reb)
}
