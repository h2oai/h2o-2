\name{h2o.runif}
\alias{h2o.runif}
\title{Produces a vector of specified length contain random uniform numbers}
\description{Produces a vector of specified length contain random uniform numbers.}
\usage{h2o.runif(x, min, max)}
\arguments{
  \item{x}{A number equal to the number of elements the vector of random numbers should have.}
  \item{min}{An integer specifying the lower bound of the distribution.}
  \item{max}{An integer specifying the upper bound of the distribution.} 
  }
\value{A vector of random, uniformly distributed numbers. The elements are btween 0 and 1 unless otherwise specified.}
}
\examples{
localH2O = h2o.init()
prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile(localH2O, path = prosPath, key = "prostate.hex")
s = runif(nrow(prostate.hex))
summary(s)
prostate.train = prostate.hex[s <= 0.8,]
prostate.train = h2o.assign(prostate.train, "prostate.train")
prostate.test = prostate.hex[s > 0.8,]
prostate.test = h2o.assign(prostate.test, "prostate.test")
nrow(prostate.train) + nrow(prostate.test)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
