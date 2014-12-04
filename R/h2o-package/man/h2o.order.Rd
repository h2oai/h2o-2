\name{h2o.order}
\alias{h2o.order}
\title{Returns a permutation which rearranges its first argument into ascending or descending order.}
\description{Allows users to find the row indices of entries with the highest or lowest value. 
To limit the need to do a global search the user can choose the number of indices returned from h2o.order.}
\usage{
h2o.order(data, cols, n = 5, decreasing = T)
}
\arguments{
  \item{data}{ An\code{\linkS4class{H2OParsedData}} object.}
  \item{cols}{ A vector containing the names or indices of the data columns chosen to be removed.}
  \item{n}{ A integer. The number of indices returned, indicating the n rows ordered.}
  \item{decreasing}{ Logical. Indicates whether sort should be in increasing or decreasing order.}
}

\examples{
library(h2o)
localH2O = h2o.init()
prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile(localH2O, path = prosPath, key = "prostate.hex")

# Find ID of the 10 youngest patients in data
indices = h2o.order(data = prostate.hex$AGE, n = 10, decreasing = TRUE)
indices.R = as.matrix(indices)
youngest_patients = prostate.hex[indices.R]
}
