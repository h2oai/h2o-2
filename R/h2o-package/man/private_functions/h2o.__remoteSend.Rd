\name{h2o.__remoteSend}
\alias{h2o.__remoteSend}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
}
\usage{
h2o.__remoteSend(client, page, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{client}{
%%     ~~Describe \code{client} here~~
}
  \item{page}{
%%     ~~Describe \code{page} here~~
}
  \item{\dots}{
%%     ~~Describe \code{\dots} here~~
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
function (client, page, ...) 
{
    ip = client@ip
    port = client@port
    url = paste0("http://", ip, ":", port, "/", page)
    temp = postForm(url, style = "POST", ...)
    after = gsub("NaN", "\"NaN\"", temp[1])
    after = gsub("Inf", "\"Inf\"", after)
    res = fromJSON(after)
    if (!is.null(res$error)) {
        myTime = gsub(":", "-", date())
        myTime = gsub(" ", "_", myTime)
        h2o.__writeToFile(res, paste0("error_json_", myTime, 
            ".log"))
        stop(paste(url, " returned the following error:\n", h2o.__formatError(res$error)))
    }
    res
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
