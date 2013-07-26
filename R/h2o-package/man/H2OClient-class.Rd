\name{H2OClient-class}
\Rdversion{1.1}
\docType{class}
\alias{H2OClient-class}
\alias{importFile,H2OClient,character,character,logical-method}
\alias{importFolder,H2OClient,character,logical-method}
\alias{importURL,H2OClient,character,character,logical-method}
\alias{show,H2OClient-method}

\title{Class \code{"H2OClient"}}
\description{
%%  ~~ A concise (1-5 lines) description of what the class is. ~~
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("H2OClient", ...)}.
%%  ~~ describe objects here ~~ 
}
\section{Slots}{
  \describe{
    \item{\code{ip}:}{Object of class \code{"character"} representing the IP address of the H2O server. }
    \item{\code{port}:}{Object of class \code{"numeric"} representing the port number of the H2O server. }
  }
}
\section{Methods}{
  \describe{
    \item{importFile}{\code{signature(object = "H2OClient", path = "character", key = "character", parse = "logical")}: ... }
    \item{importFolder}{\code{signature(object = "H2OClient", path = "character", parse = "logical")}: ... }
    \item{importURL}{\code{signature(object = "H2OClient", path = "character", key = "character", parse = "logical")}: ... }
    \item{show}{\code{signature(object = "H2OClient")}: ... }
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
showClass("H2OClient")
}
\keyword{classes}
