\name{h2o.getTree}
\alias{h2o.getTree}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Get Tree from Random Forest Model
}
\description{
Returns the depth and number of leaves of a particular tree in the random forest ensemble.
}
\usage{
h2o.getTree(forest, k)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{forest}{
An \code{\linkS4class{H2ORForestModel}} object indicating the random forest model to examine.
}
  \item{k}{
The particular tree to retrieve. (Must be an integer between 1 and \code{ntree}).
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{h2o.randomForest}}, \code{\linkS4class{H2ORForestModel}}
}
\examples{
library(h2oWrapper)
h2oWrapper.installDepPkgs()
localH2O = h2oWrapper.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2o")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
iris.rf = h2o.randomForest(y = 4, data = iris.hex, ntree = 50, depth = 100)
h2o.getTree(forest = iris.rf, k = 5)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
