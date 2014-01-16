\name{colnames}
\alias{colnames}
\title{ Returns column names for a parsed H2O data object.}
\description{Returns column names for a parsed H2O data object.}
\usage{
colnames(x)
}
\arguments{
  \item{x}{ A parsed H2O data object.}
}
}
\value{Returns a vector of column names. 
}
\examples{
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
summary(iris.hex)
colnames(iris.hex)
}

