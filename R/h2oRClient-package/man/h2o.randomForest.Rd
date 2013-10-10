\name{h2o.randomForest}
\alias{h2o.randomForest}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
H2O: Random Forest
}
\description{
Performs random forest classification on a parsed data set.
}
\usage{
h2o.randomForest(y, x, data, ntree = 50, depth = 2147483647, classwt = as.numeric(NA))
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{y}{
The name or index of the response variable. If the data does not contain a header, this is the column index, designated by increasing numbers from left to right. (The response must be either an integer or a categorical variable).
}
\item{x}{
A vector containing the names or indices of the predictor variables to use in building the random forest model.
}
  \item{data}{
An \code{\linkS4class{H2OParsedData}} object containing the variables in the model.
}
  \item{ntree}{
Number of trees to grow. (Must be a nonnegative integer).
}
  \item{depth}{
  Maximum depth to grow the tree.
  }
\item{classwt}{
  (Optional) Priors of the classes. Need not add up to one. If missing, defaults to all weights set at 1.0.}
}
\details{
Currently, only classification trees are supported. Note that indexing begins at zero, so for example, to specify the first column as the response variable, set \code{y = 0}.
}
\value{
An object of class \code{\linkS4class{H2ORForestModel}} with slots key, data, and model, where the last is a list of the following components:
\item{type }{The type of the tree, which at this point must be classification.}
\item{ntree }{Number of trees grown.}
\item{oob_err }{Out of bag error rate.}
\item{forest }{A matrix giving the minimum, mean, and maximum of the tree depth and number of leaves.}
\item{confusion }{Confusion matrix of the prediction.}
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
library(h2o)
h2o.installDepPkgs()
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
h2o.randomForest(y = 5, x = c(2,3,4), data = iris.hex, ntree = 50, depth = 100, 
classwt = c("Iris-versicolor" = 20.0, "Iris-virginica" = 30.0))
covPath = system.file("extdata", "covtype.csv", package="h2oRClient")
covtype.hex = h2o.importFile(localH2O, path = covPath, key = "covtype.hex")
h2o.randomForest(y = "Cover_Type", x = setdiff(colnames(covtype.hex), c("Cover_Type", "Aspect", "Hillshade_9am")), data = covtype.hex, ntree = 50, depth = 150)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
