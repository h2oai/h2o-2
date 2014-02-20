\name{sd}
\alias{sd}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Standard deviation of a numeric column. 
}
\description{
Calculates the standard deviation of a column of continuious real valued data. 
}
\usage{
sd(x, na.rm = FALSE)
}
\arguments{
  \item{x}{An H2O parsed data object containing numeric data. 
}
  \item{na.rm}{Logical value where FALSE does not remove NA's in the calculation and TRUE removes NA's in the calculation. 
}
}

\value{ Returns a vector of values of the standard deviations for the requested columns. 
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
sd(iris.hex[,4])
h2o.shutdown(localH2O)
}

