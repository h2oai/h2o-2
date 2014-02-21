\name{h2o.__changeLogPath}
\alias{h2o.__changeLogPath}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Change Path Where H2O R Logs are Saved
}
\description{
Change the file path where H2O R command and error response logs are currently being saved.
}
\usage{
h2o.__changeLogPath(path, type)
}
\arguments{
  \item{path}{A character string indicating the new file path where logs should be saved.}
  \item{type}{Which log file's path to modify. Either \code{"Command"} for POST commands sent between R and H2O, or \code{"Error"} for errors returned by H2O in the HTTP response.}
}

\seealso{
\code{\link{h2o.__startLogging}, \link{h2o.__stopLogging}, \link{h2o.__clearLogs}, \link{h2o.__openLog}, \link{h2o.__getLogPath}}
}
\examples{
library(h2o)
h2o.__getLogPath("Command")
h2o.__changeLogPath(getwd(), "Command")
h2o.__getLogPath("Command")
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
