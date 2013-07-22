\name{h2o.__writeToFile}
\alias{h2o.__writeToFile}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
}
\usage{
h2o.__writeToFile(res, fileName)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{res}{
%%     ~~Describe \code{res} here~~
}
  \item{fileName}{
%%     ~~Describe \code{fileName} here~~
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
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (res, fileName) 
{
    cat("Writing JSON response to", fileName)
    fileConn = file(fileName)
    formatVector = function(vec) {
        result = rep(" ", length(vec))
        for (i in 1:length(vec)) result[i] = paste0(names(vec)[i], 
            ": ", vec[i])
        paste(result, collapse = "\n")
    }
    writeLines(formatVector(unlist(res)), fileConn)
    close(fileConn)
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
