\name{plot.H2OGapStatModel}
\alias{plot.H2OGapStatModel}

\title{
  Elbow Plots and Gap Measures
}

\description{
Draw the number of clusters against the within cluster sum of squares, the expected within cluster sum of squares, and the gap statistics.
}

\usage{ \method{plot}{H2OGapStatModel}(x, ...) }

\arguments{
  \item{x}{An \code{\linkS4class{H2OGapStatModel}} object.}
  \item{\dots}{ Arguments to be passed to methods, such as graphical parameters (see \code{\link{par}} for details).}
}

\seealso{ \code{\linkS4class{H2OGapStatModel}} }


\examples{
  library(h2o)
  localH2O <- h2o.init()
  iris.hex <- as.h2o(localH2O, iris)
  gs <- h2o.gapStatistic(iris.hex, K = 5, B = 10)
  plot(gs)
}
