\name{h2o.__getLogPath}
\alias{h2o.__getLogPath}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Get Path Where H2O R Logs are Saved
}
\description{
Get the file path where H2O R command and error response logs are currently being saved.
}
\usage{
h2o.__getLogPath(type)
}
\arguments{
  \item{type}{Which log file's path to get. Either \code{"Command"} for POST commands sent between R and H2O, or \code{"Error"} for errors returned by H2O in the HTTP response.}
}

\seealso{
\code{\link{h2o.__startLogging}, \link{h2o.__stopLogging}, \link{h2o.__clearLogs}, \link{h2o.__openLog}, \link{h2o.__changeLogPath}}
}
\examples{
library(h2o)
h2o.__getLogPath("Command")
h2o.__getLogPath("Error")
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
