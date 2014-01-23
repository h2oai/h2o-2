\name{levels}
\alias{levels}
\title{
Returns a list of the unique values found a column of categorical data. 
}
\description{
Returns a list of the unique values found a column of categorical data. 
}
\usage{
levels(x)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
Column of categorical data in an H2O parsed data object. 
}
}

\value{
Returns a list containing one entry for each unique value found in the column of categorical data. 
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
+ silentUpgrade = TRUE, promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
levels(iris.hex[,5])
}
