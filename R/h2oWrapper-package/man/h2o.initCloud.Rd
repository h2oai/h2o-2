\name{h2o.initCloud}
\alias{h2o.initCloud}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Connect to Remote H2O Instance
}
\description{
Connects to H2O on remote cloud and installs the appropriate R package for communication.
}
\usage{
h2o.initCloud(ip, port)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{ip}{Object of class \code{"character"} representing the IP address of the H2O server.
}
  \item{port}{Object of class \code{"numeric"} representing the port number of the H2O server.
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
h2o.initCloud(ip = "192.168.1.164", port = 54321)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
