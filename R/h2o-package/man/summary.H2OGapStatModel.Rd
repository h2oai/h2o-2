\name{summary.H2OGapStatModel}
\alias{summary.H2OGapStatModel}
\title{ Summarizes the H2O Gap Statistic Model }
\description{
\code{summary.H2OGapStatModel}, a method for the \code{\link{summary}} generic. Gives the full output of the model created by \code{\link{h2o.gapStatistic}}.
}
\usage{\method{summary}{H2OGapStatModel}(object, ...)}
\arguments{
  \item{object}{An \code{\linkS4class{H2OGapStatModel}} object.}
  \item{...}{Additional arguments affecting the summary produced. (Currently unimplemented).}
}
\value{
A \code{data.frame} displaying the contents of the Gap Statistic model. Here we can see the Within Cluster SS, Expected Within Cluster SS, Gap Statistics
}
\examples{
library(h2o)
localH2O <- h2o.init()
iris.hex <- as.h2o(localH2O, iris)
gs <- h2o.gapStatistic(iris.hex, K = 5, B = 10)
summary(gs)  # gives all model information computed
}
