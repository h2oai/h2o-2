\name{checkH2OClient}
\alias{checkH2OClient}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Check if H2O is Running
}
\description{
Checks if H2O is running on the specified IP and port, and stops the program if it is not. Th method also compares the version number of the H2O program and the installed h2o R package. If there is a mismatch, it will warn the user.
}
\usage{
checkH2OClient(object)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
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
myClient = new("H2OClient", ip = "localhost", port = 54321)
checkH2OClient(myClient)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
