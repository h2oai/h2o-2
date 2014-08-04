\name{round.H2OParsedData}
\alias{round.H2OParsedData}
\title{Rounds argument.}
\description{Rounds the values in the first argument of a \code{\linkS4class{H2OParsedData}} object to the specified number of decimal places. Default is 0.}
\usage{\method{round}{H2OParsedData}(x, digits=0)}

\arguments{
  \item{x}{ An \code{\linkS4class{H2OParsedData}} object.}
  \item{digits}{ Single number specificing decimal places to round to.}
}

\details{
This method uses the IEC 60559 standard for rounding to the even digit, so 0.5 goes to 0 and -1.5 goes to -2. See the Java documentation of \code{RoundingMode.HALF_EVEN} for more details and examples.
}

\value{
Returns a \code{\linkS4class{H2OParsedData}} object with argument rounded as specified.
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
irisPath = system.file("extdata", "iris.csv", package="h2o")
iris.hex = h2o.importFile(localH2O, path = irisPath)
iris.data <- iris.hex[,1:3]
iris.rounded <- round.H2OParsedData(iris.data, digits=0)
head(iris.rounded)
}
