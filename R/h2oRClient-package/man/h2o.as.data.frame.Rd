\name{as.data.frame}
\alias{as.data.frame}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Converts a parsed H2O object to a data frame. 
}
\description{
Converts a parsed H2O object to a data frame. 
Convert a parsed H2O object to an H2O data frame, which allows subsequent data frame operations (like head(), and tail())
}
\usage{
as.data.frame(x, row.names = NULL, optional = FALSE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
An \code{\linkS4class{H2OParsedData} object.
}
  \item{row.names}{
A vector containing the row names of observations in the data frame. 
}
}

\value{
returns a data frame
}
}
\examples{
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
 + silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", 
 + package="h2oRClient")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
prostate.data.frame<- as.data.frame(prostate.hex)
summary(prostate.data.frame)
head(prostate.data.frame)
}

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
