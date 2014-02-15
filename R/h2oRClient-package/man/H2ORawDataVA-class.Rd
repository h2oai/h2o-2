\name{H2ORawDataVA-class}
\Rdversion{1.1}
\docType{class}
\alias{H2ORawDataVA-class}
\alias{show,H2ORawDataVA-method}

\title{Class \code{"H2ORawDataVA"}}
\description{
A class for representing imported ValueArray data sets that have not been parsed.
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("H2ORawDataVA", ...)}.
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
    \item{show}{\code{signature(object = "H2ORawDataVA")}: ... }
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
showClass("H2ORawDataVA")
}
\keyword{classes}
