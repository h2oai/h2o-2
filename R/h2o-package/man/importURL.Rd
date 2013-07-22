\name{importURL}
\alias{importURL}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Import Data from URL
}
\description{
Imports a file from the URL and parses it, returning an object containing the identifying hex key.
}
\usage{
importURL(object, path, key = "")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
}
  \item{path}{
The complete URL of the file to be imported. Each row of data appears as one line of the file.
}
  \item{key}{
(Optional) The unique hex key assigned to the imported file. If none is given, a key will automatically be generated based on the URL path.
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
An object of class \code{\linkS4class{H2OParsedData}}, which contains the following components:
\item{h2o }{The \code{\linkS4class{H2OClient}} object that was passed into the function call.}
\item{key }{The hex key assigned to the imported data.}
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
h2o = new("H2OClient", ip="localhost", port=54321)
prostate.hex = importURL(h2o, "https://raw.github.com/0xdata/h2o/master/smalldata/
logreg/prostate.csv", "prostate.hex")
summary(prostate.hex)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
