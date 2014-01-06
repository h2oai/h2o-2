\name{as.factor}
\alias{as.factor}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Converts a column from numeric to factor
}
\description{
Specify a colum type to be factor (also called categorical or enumerative), rather than numeric. 
}
\usage{
as.factor(x)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
A column in an object of class \code{\linkS4class{H2OParsedData}}, or data frame. 
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
Returns the originalobject of class \code{\linkS4class{H2OParsedData}}, with the requested column specified as a factor, rather than numeric. 
}


\examples{
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
 + silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", 
 + package="h2oRClient")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
prostate.hex[,4]=as.factor(prostate.hex[,4])
summary(prostate.hex)
}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
