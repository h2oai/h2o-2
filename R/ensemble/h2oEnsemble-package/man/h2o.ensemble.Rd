\name{h2o.ensemble}
\alias{h2o.ensemble}
\title{
H2O Ensemble
}
\description{
This function creates a "super learner" ensemble using the H2O base learning algorithms specified by the user.
}
\usage{
h2o.ensemble(x, y, data, family = "binomial", 
  learner, metalearner = "h2o.glm.wrapper", 
  cvControl = list(V=5, shuffle=TRUE), 
  seed = 1, parallel = "seq")
}
\arguments{
  \item{x}{
A vector containing the names of the predictors in the model.
}
  \item{y}{
The name of the response variable in the model.
}
  \item{data}{
An \code{\linkS4class{H2OParsedData}} object containing the variables in the model.
}
  \item{family}{
A description of the error distribution and link function to be used in the model.  This must be a character string.  Currently supports \code{"binomial"} and \code{"gaussian"}.  
}
  \item{learner}{
A string or character vector naming the prediction algorithm(s) used to train the base models for the ensemble.  The functions must have the same format as the h2o wrapper functions.
}
  \item{metalearner}{
A string specifying the prediction algorithm used to learn the optimal combination of the base learners.  Supports both h2o and SuperLearner wrapper functions.
}
  \item{cvControl}{
A list of parameters to control the cross-validation process. The \code{V} parameter is an integer representing the number of cross-validation folds and defaults to 10. Other parmeters are \code{stratifyCV} and \code{shuffle}, which are not yet enabled. 
}
  \item{seed}{
A random seed to be set (integer); defaults to 1. If \code{NULL}, then a random seed will not be set.  The seed is set prior to creating the CV folds and prior to model training for base learning and metalearning.
}
  \item{parallel}{
A character string specifying optional parallelization. Use \code{"seq"} for sequential computation (the default) of the cross-validation and base learning steps. Use \code{"multicore"} to perform the V-fold (internal) cross-validation step as well as the final base learning step in parallel over all available cores. Or parallel can be a snow cluster object. Both parallel options use the built-in functionality of the R core "parallel" package.  Currently, only \code{"seq"} is compatible with the parallelized H2O algorithms, so this argument may be removed or modified in the future.
}
}

\value{

\item{x}{
A vector containing the names of the predictors in the model.
}
\item{y}{
The name of the response variable in the model.
}
\item{family}{
Returns the \code{family} argument from above.  
}
\item{cvControl}{
Returns the \code{cvControl} argument from above.
}
\item{folds}{
A vector of fold ids for each observation, ordered by row index.  The number of unique fold ids is specified in \code{cvControl$V}.   
}
\item{ylim}{
Returns range of \code{y}.
}
\item{seed}{
An integer. Returns \code{seed} argument from above.
}
\item{parallel}{
An character vector. Returns \code{character} argument from above.
}
\item{basefits}{
A list of H2O models, each of which are trained using the \code{data} object.  The length of this list is equal to the number of base learners in the \code{learner} argument.
}
\item{metafit}{
The predictive model which is learned by regressing \code{y} on \code{Z} (see description of \code{Z} below).  The type of model is specified using the \code{metalearner} argument.
}
\item{Z}{
The Z matrix (the cross-validated predicted values for each base learner).  In the stacking ensemble literature, this is known as the "level-one" data and is the design matrix used to train the metalearner.
}
\item{runtime}{
A list of runtimes for various steps of the algorithm.  The list contains \code{cv}, \code{metalearning}, \code{baselearning} and \code{total} elements.  The \code{cv} element is the time it takes to create the \code{Z} matrix (see above).  The \code{metalearning} element is the training time for the metalearning step.  The \code{baselearning} element is a list of training times for each of the models in the ensemble.  The time to run the entire \code{h2o.ensemble} function is given in \code{total}.
}


}
\references{
van der Laan, M. J., Polley, E. C. and Hubbard, A. E. (2007) Super Learner, Statistical Applications of Genetics and Molecular Biology, 6, article 25. \cr
\url{http://dx.doi.org/10.2202/1544-6115.1309}\cr
\url{http://biostats.bepress.com/ucbbiostat/paper222}\cr
\cr
Breiman, L. (1996) Stacked Regressions, Machine Learning, 24:49–64.\cr
\url{http://dx.doi.org/10.1007/BF00117832}\cr
\url{http://statistics.berkeley.edu/sites/default/files/tech-reports/367.pdf}
}
\author{
Erin LeDell \email{ledell@berkeley.edu}
}
\note{
Using an h2o algorithm wrapper function as the metalearner is not yet producing good results.  For now, it is recommended to use the \code{\link[SuperLearner:SL.glm]{SL.glm}} function as the metalearner.
}


\seealso{
\code{\link[SuperLearner:SuperLearner]{SuperLearner}}, \code{\link[subsemble:subsemble]{subsemble}}
}
\examples{
\dontrun{
    
# An example of binary classification using h2o.ensemble

library(h2oEnsemble)
library(SuperLearner)  # For metalearner such as "SL.glm"
library(cvAUC)  # Used to calculate test set AUC (requires version >=1.0.1 of cvAUC)
localH2O <-  h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, nthreads = -1)


# Import a sample binary outcome train/test set into R
train <- read.table("http://www.stat.berkeley.edu/~ledell/data/higgs_5k.csv", sep=",")
test <- read.table("http://www.stat.berkeley.edu/~ledell/data/higgs_test_5k.csv", sep=",")


# Convert R data.frames into H2O parsed data objects
data <- as.h2o(localH2O, train)
newdata <- as.h2o(localH2O, test)
y <- "V1"
x <- setdiff(names(data), y)
family <- "binomial"


# Create a custom base learner library & specify the metalearner
h2o.randomForest.1 <- function(..., ntrees = 1000, nbins = 100, seed = 1) h2o.randomForest.wrapper(..., ntrees = ntrees, nbins = nbins, seed = seed)
h2o.deeplearning.1 <- function(..., hidden = c(500,500), activation = "Rectifier", seed = 1)  h2o.deeplearning.wrapper(..., hidden = hidden, activation = activation, seed = seed)
h2o.deeplearning.2 <- function(..., hidden = c(200,200,200), activation = "Tanh", seed = 1)  h2o.deeplearning.wrapper(..., hidden = hidden, activation = activation, seed = seed)
learner <- c("h2o.randomForest.1", "h2o.deeplearning.1", "h2o.deeplearning.2")
metalearner <- c("SL.glm")


# Train the ensemble using 4-fold CV to generate level-one data
# More CV folds will take longer to train, but should increase performance
fit <- h2o.ensemble(x = x, y = y, data = data, family = family, 
                    learner = learner, metalearner = metalearner,
                    cvControl = list(V=4))


# Generate predictions on the test set
pred <- predict(fit, newdata)
labels <- as.data.frame(newdata[,c(y)])[,1]


# Ensemble test AUC 
AUC(predictions=as.data.frame(pred$pred)[,1], labels=labels)
# 0.7681649


# Base learner test AUC (for comparison)
L <- length(learner)
sapply(seq(L), function(l) AUC(predictions = as.data.frame(pred$basepred)[,l], labels = labels)) 
# 0.7583084 0.7145333 0.7123253


# Note that the ensemble results above are not reproducible since 
# h2o.deeplearning is not reproducible when using multiple cores.
# For reproducible results, use h2o.init(nthreads = 1)

}
}
