\name{h2o.__openLog}
\alias{h2o.__openLog}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
View H2O R Logs
}
\description{
Open existing logs of H2O R POST commands and error responses on local disk. Used primarily for debugging purposes.
}
\usage{
h2o.__openLog(type)
}
\arguments{
  \item{type}{Which log file to open. Either \code{"Command"} for POST commands sent between R and H2O, or \code{"Error"} for errors returned by H2O in the HTTP response.}
}

\seealso{
\code{\link{h2o.__startLogging}, \link{h2o.__stopLogging}, \link{h2o.__clearLogs}, \link{h2o.__getLogPath}, \link{h2o.__changeLogPath}}
}
\examples{
\dontrun{
# Skip running this to avoid windows being opened during R CMD check
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)

h2o.__startLogging()
ausPath = system.file("extdata", "australia.csv", package="h2o")
australia.hex = h2o.importFile(localH2O, path = ausPath)
h2o.__stopLogging()

h2o.__openLog("Command")
h2o.__openLog("Error")
h2o.shutdown(localH2O)
}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
