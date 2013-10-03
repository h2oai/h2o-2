\name{h2oWrapper.installDepPkgs}
\alias{h2oWrapper.installDepPkgs}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Install H2O R Package Dependencies
}
\description{
Installs all R packages that the H2O R package requires for correct functionality. This method must be executed first on every new machine before attempting to communicate with H2O from R.
}
\usage{
h2oWrapper.installDepPkgs(optional = FALSE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{optional}{
A logical value indicating whether to install optional packages for visualizing data.
}
}
\details{
The R packages required to run H2O are RCurl, rjson, and tools. All packages are installed from CRAN. The package RCurl needs the latest version of libcurl. See the \href{http://www.omegahat.org/RCurl/FAQ.html}{RCurl FAQ} for details.

If \code{optional = TRUE}, the packages fpc and cluster, along with all associated dependencies, will be installed. Note that this requires R version 3.0 or higher.
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
h2oWrapper.installDepPkgs()
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line