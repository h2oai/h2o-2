\name{h2o.__startLogging}
\alias{h2o.__startLogging}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Start Writing H2O R Logs
}
\description{
Begin logging H2O R POST commands and error responses to local disk. Used primarily for debugging purposes.
}
\usage{
h2o.__startLogging()
}
\seealso{
\code{\link{h2o.__stopLogging}, \link{h2o.__clearLogs}, \link{h2o.__openLog}, \link{h2o.__getLogPath}, \link{h2o.__changeLogPath}}
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
h2o.__startLogging()
ausPath = system.file("extdata", "australia.csv", package="h2o")
australia.hex = h2o.importFile(localH2O, path = ausPath)
h2o.__stopLogging()
h2o.shutdown(localH2O)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
