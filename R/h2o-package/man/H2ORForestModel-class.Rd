\name{H2ORForestModel-class}
\Rdversion{1.1}
\docType{class}
\alias{H2ORForestModel-class}
\alias{h2o.getTree,H2ORForestModel,numeric-method}
\alias{show,H2ORForestModel-method}

\title{Class \code{"H2ORForestModel"}}
\description{
A class for representing random forest ensembles.
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("H2ORForestModel", ...)}.
%%  ~~ describe objects here ~~ 
}
\section{Slots}{
  \describe{
    \item{\code{key}:}{Object of class \code{"character"}, representing the unique hex key that identifies the model.}
    \item{\code{data}:}{Object of class \code{\linkS4class{H2OParsedData}}, which is the input data used to build the model.}
    \item{\code{model}:}{Object of class \code{"list"} containing the following elements:
    \itemize{
    \item{\code{type}: The type of the tree, which at this point must be classification.}
    \item{\code{ntree}: Number of trees grown.}
    \item{\code{oob_err}: Out of bag error rate.}
    \item{\code{forest}: A matrix giving the minimum, mean, and maximum of the tree depth and number of leaves.}
    \item{\code{confusion}: Confusion matrix of the prediction.}
    }}
  }
}
\section{Methods}{
  \describe{
    \item{h2o.getTree}{\code{signature(forest = "H2ORForestModel", k = "numeric")}: ... }
    \item{show}{\code{signature(object = "H2ORForestModel")}: ... }
	 }
}
\references{
%%  ~~put references to the literature/web site here~~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%%  ~~objects to See Also as \code{\link{~~fun~~}}, ~~~
%%  ~~or \code{\linkS4class{CLASSNAME}} for links to other classes ~~~
}
\examples{
showClass("H2ORForestModel")
}
\keyword{classes}
