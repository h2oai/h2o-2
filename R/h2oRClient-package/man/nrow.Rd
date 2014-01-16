\name{nrow}
\alias{nrow}

\title{
Returns a count of rows for a parsed H2O data object
}
\description{
Returns a count of rows for a parsed H2O data object
}
\usage{
nrow(x)
}
\arguments{
  \item{x}{H2O parsed data object
}
}
\value{ An integer count of the number of rows 
}

\examples{
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
nrow(iris.hex)
}

