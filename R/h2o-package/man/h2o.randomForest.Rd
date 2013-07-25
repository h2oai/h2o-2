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
h2o.randomForest(y, data, ntree, depth, classwt = as.numeric(NA))
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{y}{
The name of the response variable. If the data does not contain a header, this is the column index. (This must be either an integer or a categorical variable).
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
Currently, only classification regression trees are supported, and there is no way to ignore predictor variables during growth of the tree.
}
\value{
An object of class \code{\linkS4class{H2ORForestModel}} with slots key, data, and rf, where the last is a list of the following components:
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
# Assuming your working directory is h2o/R
iris.hex = importFile(h2o, "../smalldata/iris/iris.csv", "iris.hex")
h2o.randomForest(y = "4", data = iris.hex, ntree = 50, depth = 100, 
   classwt = c("Iris-versicolor" = 20.0, "Iris-virginica" = 30.0))
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
