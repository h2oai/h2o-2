\name{max}
\alias{max}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Maximum of Numeric Values
}
\description{
Calculates the (parallel) maximum of the input values. This method extends the \code{\link{max}} generic to deal with \code{\linkS4class{H2OParsedData}} objects.
}
\usage{
max(..., na.rm = FALSE)
}
\arguments{
  \item{...}{Numeric, character or \code{\linkS4class{H2OParsedData}} arguments. 
  }
  \item{na.rm}{Logical value where FALSE does not remove NA's in the calculation and TRUE removes NA's in the calculation.
  }
}

\value{ Returns the maximum over all the input arguments. For a \code{\linkS4class{H2OParsedData}} object, the maximum is taken over all entries in the dataset. An error will occur if any of those entries is non-numeric.
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
ausPath = system.file("extdata", "australia.csv", package="h2o")
australia.hex = h2o.importFile(localH2O, path = ausPath, key = "australia.hex")
max(australia.hex)
max(c(4000, 1234, -17), TRUE, australia.hex[,1:4])
h2o.shutdown(localH2O)
}
