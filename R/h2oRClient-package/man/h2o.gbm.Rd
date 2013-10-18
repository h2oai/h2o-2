\name{h2o.gbm}
\alias{h2o.gbm}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
H2O: GBM
}
%%  ~~function to do ... ~~

\description{Builds gradient boosed classification trees on a parsed data set.
}
\usage{
h2o.gbm(x, y, distribution = "multinomial", data, n.trees = 10, interaction.depth = 8, n.minobsinnode = 10, shrinkage = 0.2)
}
\arguments{
  \item{x}{
A vector containing the names or indices of the predictor variables to use in building the GBM model.
}
  \item{y}{
The name or index of the response variable. If the data does not contain a header, this is the column index number starting at 0, and increasing from left to right. (The response must be either an integer or a categorical variable).
}
  \item{distribution}{
The type of GBM model to be produced, categorization is "multinomial" (default), "gaussian" used for regression.
}
  \item{data}{
An \code{\linkS4class{H2OParsedData}} object containing the variables in the model.
}
  \item{n.trees}{
Number of trees to grow. Must be a nonnegative integer.
}
  \item{interaction.depth}{
Maximum depth to grow the tree.
}
  \item{n.minobsinnode}{
Minimum number of rows to assign to teminal nodes.
}
  \item{shrinkage}{
A learning-rate parameter defining step size reduction.
}   
  }
\value{
An object of class \code{\linkS4class{H2OGBM}} with slots key, data, and model, where the last is a list of the following components:
\item{type }{The type of the tree, which currently must be classification.}
\item{n.trees }{Number of trees grown.}
\item{oob_err }{Out of bag error rate.}
\item{forest }{A matrix giving the minimum, mean, and maximum of the tree depth and number of leaves.}
\item{confusion }{Confusion matrix of the prediction.}
}
\references{

1. Elith, Jane, John R Leathwick, and Trevor Hastie. "A Working Guide to
Boosted Regression Trees." Journal of Animal Ecology 77.4 (2008): 802-813

2. Friedman, Jerome, Trevor Hastie, Saharon Rosset, Robert Tibshirani,
and Ji Zhu. "Discussion of Boosting Papers." Ann. Statist 32 (2004): 
102-107

3. Hastie, Trevor, Robert Tibshirani, and J Jerome H Friedman. The
Elements of Statistical Learning.
Vol.1. N.p.: Springer New York, 2001. 
http://www.stanford.edu/~hastie/local.ftp/Springer/OLD//ESLII_print4.pdf
}


\seealso{
For more information see: http://docs.0xdata.com
}
\examples{
library(h2o)
h2o.installDepPkgs()
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
# Run classification GBM on CAPSULE ~ AGE + RACE + PSA + DCAPS
prostate.hex = h2o.importURL(localH2O, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
h2o.gbm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, n.trees = 100, interaction.depth = 8, n.minobsinnode = 10, shrinkage = 0.2)
# Run regression GBM on CAPSULE ~ AGE + RACE + PSA + DCAPS
myX = setdiff(colnames(prostate.hex), c("ID", "DPROS", "DCAPS", "VOL"))
h2o.gbm(y = "VOL", x = myX, distribution = "gaussian", data = prostate.hex, n.trees = 10, interaction.depth = 5, shrinkage = 0.1)
}
