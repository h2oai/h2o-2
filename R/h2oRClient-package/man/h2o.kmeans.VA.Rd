\name{h2o.kmeans.VA}
\alias{h2o.kmeans.VA}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
H2O: K-Means Clustering (ValueArray)
}
\description{
Performs k-means clustering using the ValueArray implementation on a data set.
}
\usage{
h2o.kmeans.VA(data, centers, cols = "", iter.max = 10, normalize = FALSE, init = "none")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{
An \code{\linkS4class{H2OParsedDataVA}} object containing the variables in the model.
}
  \item{centers}{
The number of clusters k.
}
  \item{cols}{
(Optional) A vector containing the names of the data columns on which k-means runs. If blank, k-means clustering will be run on the entire data set.
}
  \item{iter.max}{
(Optional) The maximum number of iterations allowed.
}
  \item{normalize}{
(Optional) A logical value indicating whether the data should be normalized before running k-means.
}
  \item{init}{
(Optional) Method by which to select the k initial cluster centroids. Possible values are \code{"none"} for random initialization, \code{"plusplus"} for k-means++ initialization, and \code{"furthest"} for initialization at the furthest point from each successive centroid. See the \href{http://docs.0xdata.com/datascience/kmeans.html}{H2O K-means documentation} for more details.
  }
}

\details{
%%  ~~ If necessary, more details than the description above ~~
IMPORTANT: Currently, to run k-means using the ValueArray implementation, you must import data to a ValueArray object using \code{\link{h2o.importFile.VA}}, \code{\link{h2o.importFolder.VA}} or one of its variants.
}

\value{
An object of class \code{\linkS4class{H2OKMeansModelVA}} with slots key, data, and model, where the last is a list of the following components:
  \item{centers }{A matrix of cluster centers.}
  \item{cluster }{A \code{\linkS4class{H2OParsedDataVA}} object containing the vector of integers (from 1 to k), which indicate the cluster to which each point is allocated.}
  \item{size }{The number of points in each cluster.}
  \item{withinss }{Vector of within-cluster sum of squares, with one component per cluster.}
  \item{tot.withinss }{Total within-cluster sum of squares, i.e., sum(withinss).}
}

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
\code{\link{h2o.importFile.VA}, \link{h2o.importFolder.VA}, \link{h2o.importHDFS.VA}, \link{h2o.importURL.VA}, \link{h2o.uploadFile.VA}}
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile.VA(localH2O, path = prosPath)
h2o.kmeans.VA(data = prostate.hex, centers = 10, cols = c("AGE", "RACE", "VOL", "GLEASON"))
covPath = system.file("extdata", "covtype.csv", package="h2oRClient")
covtype.hex = h2o.importFile.VA(localH2O, path = covPath)
covtype.km = h2o.kmeans.VA(data = covtype.hex, centers = 5, cols = c(1, 2, 3))
print(covtype.km)
h2o.shutdown(localH2O)
}
