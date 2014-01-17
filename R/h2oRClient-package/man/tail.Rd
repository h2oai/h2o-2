\name{tail}
\alias{tail}

\title{
Obtains the last rows of an H2O parsed data object
}
\description{
Obtains the last rows of an H2O parsed data object
}
\usage{
tail(x, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
An H2O parsed data object
}
  \item{\dots}{
The number of rows to be returned (the default count is 6).
}
}

\value{
A table displaying the last 6 rows of an H2O parsed data frame, unless the number of rows is otherwise specified to be N, in which case the last N rows are returned.
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
 + silentUpgrade = TRUE, promptUpgrade = FALSE)
ausPath = system.file("extdata", "australia.csv", package="h2oRClient")
australia.hex = h2o.importFile(localH2O, path = ausPath)
tail(australia.hex, 10)
}

