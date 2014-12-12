\name{h2o.listTimezones}
\alias{h2o.listTimezones}
\title{Prints out a list of time zone names}
\description{
\code{h2o.listTimezones}, Prints out a list of time zone names.
}
\usage{
h2o.listTimezones(client)
}

\arguments{
  \item{client}{ An \code{\linkS4class{H2OClient}} object.}
}
\details{
Prints out a list of time zones that can be used as input for \code{h2o.setTimezone}.
}
\value{
Prints out a list with the Standard Offset, Canonical ID, and Aliases of each time zone.
}
\seealso{
\code{\link{h2o.setTimezone}, \link{h2o.getTimezone}, \link{as.Date.H2OParsedData}}
}
\note{
The function does a print out of a list, the output is not a R list.
}
\examples{
library(h2o)
localH2O = h2o.init()
h2o.listTimezones(localH2O)
}
