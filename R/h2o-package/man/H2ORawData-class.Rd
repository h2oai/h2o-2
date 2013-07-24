\name{H2ORawData-class}
\Rdversion{1.1}
\docType{class}
\alias{H2ORawData-class}
\alias{parseRaw,H2ORawData,character-method}
\alias{parseRaw,H2ORawData,missing-method}
\alias{show,H2ORawData-method}

\title{Class \code{"H2ORawData"}}
\description{
%%  ~~ A concise (1-5 lines) description of what the class is. ~~
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("H2ORawData", ...)}.
%%  ~~ describe objects here ~~ 
}
\section{Slots}{
  \describe{
    \item{\code{h2o}:}{Object of class \code{"H2OClient"} ~~ }
    \item{\code{key}:}{Object of class \code{"character"} ~~ }
  }
}
\section{Methods}{
  \describe{
    \item{parseRaw}{\code{signature(data = "H2ORawData", key = "character")}: ... }
    \item{parseRaw}{\code{signature(data = "H2ORawData", key = "missing")}: ... }
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
