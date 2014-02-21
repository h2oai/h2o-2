\name{head}
\alias{head}
\title{Obtains the first rows of an H2O parsed data object
}
\description{
Obtains the first rows of an H2O parsed data object
}
\usage{
head(x, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{An H2O parsed data object
}
  \item{\dots}{The number of rows to be returned (the default count is 6). 
}
}

\value{ The first 6 rows of an H2O parsed data frame, unless the number of rows is otherwise specified to be N, in which case the first N rows are returned.
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
ausPath = system.file("extdata", "australia.csv", package="h2o")
australia.hex = h2o.importFile(localH2O, path = ausPath)
head(australia.hex, 10)
h2o.shutdown(localH2O)
}

