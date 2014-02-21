\name{min}
\alias{min}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Minimum of Numeric Values
}
\description{
Calculates the (parallel) minimum of the input values. This method extends the \code{\link{min}} generic to deal with \code{\linkS4class{H2OParsedData}} objects.
}
\usage{
min(..., na.rm = FALSE)
}
\arguments{
  \item{...}{Numeric, character or \code{\linkS4class{H2OParsedData}} arguments. 
  }
  \item{na.rm}{Logical value where FALSE does not remove NA's in the calculation and TRUE removes NA's in the calculation.
  }
}

\value{ Returns the minimum over all the input arguments. For a \code{\linkS4class{H2OParsedData}} object, the minimum is taken over all entries in the dataset. An error will occur if any of those entries is non-numeric.
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
ausPath = system.file("extdata", "australia.csv", package="h2o")
australia.hex = h2o.importFile(localH2O, path = ausPath, key = "australia.hex")
min(australia.hex)
min(c(-1, 0.5, 0.2), FALSE, australia.hex[,1:4])
h2o.shutdown(localH2O)
}
