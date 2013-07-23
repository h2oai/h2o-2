\name{H2OParsedData-class}
\Rdversion{1.1}
\docType{class}
\alias{H2OParsedData-class}
\alias{colnames,H2OParsedData-method}
\alias{h2o.glm,character,character,H2OParsedData,character,ANY,ANY,ANY-method}
\alias{h2o.glm,character,character,H2OParsedData,character,numeric,numeric,numeric-method}
\alias{h2o.kmeans,H2OParsedData,numeric,ANY,ANY-method}
\alias{h2o.kmeans,H2OParsedData,numeric,character,numeric-method}
\alias{h2o.randomForest,character,H2OParsedData,numeric-method}
\alias{show,H2OParsedData-method}
\alias{summary,H2OParsedData-method}

\title{Class \code{"H2OParsedData"}}
\description{
%%  ~~ A concise (1-5 lines) description of what the class is. ~~
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("H2OParsedData", ...)}.
%%  ~~ describe objects here ~~ 
}
\section{Slots}{
  \describe{
    \item{\code{h2o}:}{Object of class \code{"H2OClient"}, which is the client object that was passed into the function call.}
    \item{\code{key}:}{Object of class \code{"character"}, which is the hex key assigned to the imported data.}
  }
}
\section{Methods}{
  \describe{
    \item{colnames}{\code{signature(x = "H2OParsedData")}: ... }
    \item{h2o.glm}{\code{signature(x = "character", y = "character", data = "H2OParsedData", 
    family = "character", nfolds = "ANY", alpha = "ANY", lambda = "ANY")}: ... }
    \item{h2o.glm}{\code{signature(x = "character", y = "character", data = "H2OParsedData", 
    family = "character", nfolds = "numeric", alpha = "numeric", lambda = "numeric")}: ... }
    \item{h2o.kmeans}{\code{signature(data = "H2OParsedData", centers = "numeric", cols = "ANY", 
    iter.max = "ANY")}: ... }
    \item{h2o.kmeans}{\code{signature(data = "H2OParsedData", centers = "numeric", cols = "character", 
    iter.max = "numeric")}: ... }
    \item{h2o.randomForest}{\code{signature(y = "character", data = "H2OParsedData", ntree = "numeric")}: ... }
    \item{show}{\code{signature(object = "H2OParsedData")}: ... }
    \item{summary}{\code{signature(object = "H2OParsedData")}: ... }
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
showClass("H2OParsedData")
}
\keyword{classes}
