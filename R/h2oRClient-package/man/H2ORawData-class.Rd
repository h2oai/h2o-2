\name{H2ORawData-class}
\Rdversion{1.1}
\docType{class}
\alias{H2ORawData-class}
\alias{show,H2ORawData-method}

\title{Class \code{"H2ORawData"}}
\description{
A class for representing imported FluidVecs data sets that have not been parsed.
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("H2ORawData", ...)}.
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
    \item{show}{\code{signature(object = "H2ORawData")}: ... }
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
showClass("H2ORawData")
}
\keyword{classes}
