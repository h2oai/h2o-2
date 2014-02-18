\name{h2o.kmeans}
\alias{h2o.kmeans}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
H2O: K-Means Clustering
}
\description{
Performs k-means clustering on an \code{\linkS4class{H2OParsedData} object.
}}
\usage{
h2o.kmeans(data, centers, cols = "", iter.max = 10, normalize = FALSE, init = "none", version = 1)
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
  \item{normalize}{
(Optional) A logical value indicating whether the data should be normalized before running k-means.
}
  \item{init}{
(Optional) Method by which to select the k initial cluster centroids. Possible values are \code{"none"} for random initialization, \code{"plusplus"} for k-means++ initialization, and \code{"furthest"} for initialization at the furthest point from each successive centroid. See the \href{http://docs.0xdata.com/datascience/kmeans.html}{H2O K-means documentation} for more details.
  }
  \item{version}{
  (Optional) The version of k-means clustering to run. \code{version = 1} corresponds to the more stable ValueArray implementation, while \code{version = 2} selects the faster, but still beta stage FluidVecs implementation.
  }
}

\value{
An object of class \code{\linkS4class{H2OKMeansModel}} with slots key, data, and model, where the last is a list of the following components:
  \item{centers }{A matrix of cluster centers.}
  \item{cluster }{A \code{\linkS4class{H2OParsedData}} object containing the vector of integers (from 1 to k), which indicate the cluster to which each point is allocated.}
  \item{size }{The number of points in each cluster.}
  \item{withinss }{Vector of within-cluster sum of squares, with one component per cluster.}
  \item{tot.withinss }{Total within-cluster sum of squares, i.e., sum(withinss).}
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
h2o.kmeans(data = prostate.hex, centers = 10, cols = c("AGE", "RACE", "VOL", "GLEASON"))
covPath = system.file("extdata", "covtype.csv", package="h2oRClient")
covtype.hex = h2o.importFile(localH2O, path = covPath)
covtype.km = h2o.kmeans(data = covtype.hex, centers = 5, cols = c(1, 2, 3))
print(covtype.km)
}
