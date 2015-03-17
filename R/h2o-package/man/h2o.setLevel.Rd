\name{h2o.setLevel}
\alias{h2o.setLevel}
\title{Pattern Replacement}
\description{ \code{h2o.setLevel}, a method to set a factor column to one of the levels.}
\usage{h2o.setLevel(x, level)}
\arguments{
  \item{x}{An \code{\linkS4class{H2OParsedData}} object with a single factor column.}
  \item{level}{The level at which the column will be set.}
  }
\details{
  Replace all other occurrences with `level` in a factor column.
}
\value{
An object of class "H2OParsedData".
}

\examples{
library(h2o)
localH2O <- h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
hex <- as.h2o(localH2O  , iris)
hex$Species <- h2o.setLevel(hex$Species, "versicolor")
}
