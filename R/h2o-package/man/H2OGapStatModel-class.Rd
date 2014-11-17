\name{H2OGapStatModel-class}
\Rdversion{1.1}
\docType{class}
\alias{H2OGapStatModel-class}
\alias{show,H2OGapStatModel-method}

\title{Class \code{"H2OGapStatModel"}}

\description{
  A class for representing gap statistic models.
}

\section{Objects from the Class}{
  Objects can be created by calls of the form \code{new("H2OGapStatModel", ...)}.
}

\section{Slots}{
  \describe{
    \item{\code{key}:}{Object of class \code{"character"}, representing the unique hex key that identifies the model.}
    \item{\code{data}:}{Object of class \code{\linkS4class{H2OParsedData}}, which is the input data used to build the model.}
    \item{\code{model}:}{Object of class \code{"list"}}
  }
}

\section{Extends}{
  Class \code{"\linkS4class{H2OModel}"}, directly.
}

\section{Methods}{
  \describe{
    \item{show}{\code{signature(object = "H2OGapStatModel")}: ... }
	 }
}

\seealso{
  \code{\link{h2o.naiveBayes}}
}

\examples{
  showClass("H2OGapStatModel")
}

\keyword{classes}
