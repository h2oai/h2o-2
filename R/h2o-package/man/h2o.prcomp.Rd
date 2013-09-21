\name{h2o.prcomp}
\alias{h2o.prcomp}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Principal Components Analysis
}
\description{
Performs principal components analysis on the given data set.
}
\usage{
h2o.prcomp(data, tol = 0, standardize = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{
An \code{\linkS4class{H2OParsedData}} object on which to run principal components analysis.
}
  \item{tol}{
A value indicating the magnitude below which components should be omitted. (Components are omitted if their standard deviations are less than or equal to tol times the standard deviation of the first component.) With the default setting \code{tol = 0}, no components are omitted.
}
  \item{standardize}{
A logical value indicating whether the variables should be shifted to be zero centered and scaled to have unit variance before the analysis takes place.
}
}
\details{
The calculation is done by a singular value decomposition of the (possibly standardized) data set. 
}
\value{
An object of class \code{\linkS4class{H2OPCAModel}} with slots key, data, and model, where the last is a list of the following components:
\item{standardized }{A logical value indicating whether the data was centered and scaled.}
\item{sdev }{The standard deviations of the principal components (i.e., the square roots of the eigenvalues of the covariance/correlation matrix).}
\item{rotation }{The matrix of variable loadings (i.e., a matrix whose columns contain the eigenvectors).}
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
The signs of the columns of the rotation matrix are arbitrary, and so may differ between different programs for PCA.
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
localH2O = new("H2OClient")
h2o.checkClient(localH2O)
prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile(localH2O, path = prosPath)
prostate.pca = h2o.prcomp(data = prostate.hex, standardize = TRUE)
print(prostate.pca)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
