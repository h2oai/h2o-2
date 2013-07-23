\name{H2OGLMModel-class}
\Rdversion{1.1}
\docType{class}
\alias{H2OGLMModel-class}
\alias{show,H2OGLMModel-method}

\title{Class \code{"H2OGLMModel"}}
\description{
A class for representing generalized linear models.
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("H2OGLMModel", ...)}.
%%  ~~ describe objects here ~~ 
}
\section{Slots}{
  \describe{
    \item{\code{key}:}{Object of class \code{"character"}, representing the unique hex key that identifies the model.}
    \item{\code{data}:}{Object of class \code{\linkS4class{H2OParsedData}}, which is the input data used to build the model.}
    \item{\code{glm}:}{Object of class \code{"list"} containing the following elements:
    \itemize{
    \item{\code{coefficients}: A named vector of the coefficients estimated in the model.}
    \item{\code{rank}: The numeric rank of the fitted linear model.}
    \item{\code{family}: The family of the error distribution.}
    \item{\code{deviance}: The deviance of the fitted model.}
    \item{\code{aic}: Akaike's Information Criterion for the final computed model.}
    \item{\code{null.deviance}: The deviance for the null model.}
    \item{\code{iter}: Number of algorithm iterations to compute the model.}
    \item{\code{df.residual}: The residual degrees of freedom.}
    \item{\code{df.null}: The residual degrees of freedom for the null model.}
    \item{\code{y}: The response variable in the model.}
    \item{\code{x}: A vector of the predictor variable(s) in the model.}
    }
  }
}}
\section{Methods}{
  \describe{
    \item{show}{\code{signature(object = "H2OGLMModel")}: ... }
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
showClass("H2OGLMModel")
}
\keyword{classes}
