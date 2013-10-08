\name{h2o.kmeans}
\alias{h2o.kmeans}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
H2O: K-Means Clustering
}
\description{
Performs k-means clustering on a parsed data file.
}
\usage{
h2o.kmeans(data, centers, cols = "", iter.max = 10)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{
An \code{\linkS4class{H2OParsedData}} object containing the variables in the model.
}
  \item{centers}{
The number of clusters k.
}
  \item{cols}{
(Optional) A vector containing the names of the data columns on which k-means runs. If blank, k-means clustering will be run on the entire data set.
}
  \item{iter.max}{
The maximum number of iterations allowed.
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
An object of class \code{\linkS4class{H2OKMeansModel}} with slots key, data, and model, where the last is a list of the following components:
\item{centers }{A matrix of cluster centers.}
\item{cluster }{A \code{\linkS4class{H2OParsedData}} object containing the vector of integers (from 1 to k), which indicate the cluster to which each point is allocated.}
\item{size }{The number of points in each cluster.}
\item{withinss }{Vector of within-cluster sum of squares, with one component per cluster.}
\item{tot.withinss }{Total within-cluster sum of squares, i.e., sum(withinss).}
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
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
library(h2oWrapper)
h2oWrapper.installDepPkgs()
localH2O = h2oWrapper.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
h2o.kmeans(data = prostate.hex, centers = 10, cols = c("AGE", "RACE", "VOL", "GLEASON"))
covPath = system.file("extdata", "covtype.csv", package="h2o")
covtype.hex = h2o.importFile(localH2O, path = covPath)
covtype.km = h2o.kmeans(data = covtype.hex, centers = 5, cols = c(1, 2, 3))
print(covtype.km)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
