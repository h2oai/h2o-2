\name{rbind.H2OParsedData}
\alias{rbind.H2OParsedData}
\title{
Combine H2O Datasets by Rows
}
\description{
\code{rbind.H2OParsedData}, a method for the \code{\link{rbind}} generic. Takes a sequence of H2O datasets and combines them by row.
}
\usage{
\method{rbind}{H2OParsedData}(\dots, deparse.level = 1)
}
\arguments{
  \item{\dots}{A sequence of \code{\linkS4class{H2OParsedData}} arguments. All datasets must exist on the same H2O instance (IP and port) and contain the same number of rows.
  }
  \item{deparse.level}{Integer controlling the construction of row names. Currently unimplemented.
  }
}
\value{
An \code{\linkS4class{H2OParsedData}} object containing the combined \dots arguments row-wise.
}

\examples{
library(h2o)
localH2O = h2o.init()
prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
prostate.rbind = rbind(prostate.hex, prostate.hex)
head(prostate.rbind)
}
