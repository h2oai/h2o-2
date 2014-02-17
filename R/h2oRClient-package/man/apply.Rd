\name{apply}
\alias{apply}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Applies a function over an H2O parsed data object (an array).
}
\description{
Applies a function over an H2O parsed data object (an array).
}
\usage{
apply(X, MARGIN, FUN, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{X}{
An H2O parsed data object. 
}
  \item{MARGIN}{
The margin along wich the function should be applied
}
  \item{FUN}{
The function to be applied by H2O. 
}
}
\value{
Produces a new vector or array of the output of the applied function. That vector is stored in H2O so that it can be used in subsequent H2O processes. 
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, 
+ promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
summary(apply(iris.hex, 1, sum))
}

