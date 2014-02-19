\name{h2o.anyFactor}
\alias{h2o.anyFactor}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Check if H2O is Running
}
\description{
Checks if H2O is running on the specified IP and port, and stops the program if it is not. Th method also compares the version number of the H2O program and the installed h2o R package. If there is a mismatch, it will warn the user.
}
\usage{
h2o.anyFactor(x)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{An \code{\linkS4class{H2OParsedData}} object.
}
}
\value{
Returns a logical value indicating whether any of the columns in \code{x} are factors.
}
\seealso{
\code{\linkS4class{H2OParsedData}}
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris_wheader.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath)
h2o.anyFactor(iris.hex)
h2o.shutdown(localH2O)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
