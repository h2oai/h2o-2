\name{h2o.exec}
\alias{h2o.exec}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Execute a Command on H2O
}
\description{
Directly send over and execute an R expression in the H2O console.
}
\usage{
h2o.exec(expr_to_execute)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{expr_to_execute}{An R expression that is supported by H2O. Currently, basic subsetting, arithmetic operations, logical filters, and simple expressions like \code{dim} are allowed.}
}
\value{
A \code{\linkS4class{H2OParsedData}} object containing the result of the expression.
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
irisPath = system.file("extdata", "iris_wheader.csv", package="h2o")
hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
res1 = h2o.exec(hex[,1] + hex[,2])
head(res1)
res2 = h2o.exec(hex[,1] + hex[, 2] + hex[, 3] * hex[,4] / hex[,1])
head(res2)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
