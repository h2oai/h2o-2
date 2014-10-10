\name{h2o.ensemble}
\alias{h2o.ensemble}
\title{
H2O Ensemble
}
\description{
This function creates a super learner ensemble using the H2O base learning algorithms specified by the user.
}
\usage{
h2o.ensemble(x, y, data, family = "binomial", 
  learner, metalearner = "h2o.glm.wrapper", 
  cvControl = list(), seed = 1, parallel = "seq")
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
A random seed to be set (integer); defaults to 1. If \code{NULL}, then a random seed will not be set.
}
  \item{parallel}{
A character string specifying optional parallelization. Use \code{"seq"} for sequential computation (the default). Use \code{"multicore"} to perform the V-fold (internal) cross-validation step as well as the final base learning step in parallel over all available cores. Or parallel can be a snow cluster object. Both parallel options use the built-in functionality of the R core "parallel" package.
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
\url{http://dx.doi.org/10.2202/1544-6115.1309}
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
    
library(h2o)
library(caret)  #Used to generate data
library(cvAUC)  #Used to calculate test set AUC
library(SuperLearner)  #For metalearner such as 'SL.glm'
localH2O <-  h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)


# Create sample train and test sets.
n_train <- 1000
n_test <- 1000

set.seed(1)
train <- twoClassSim(n = n_train, intercept = -13)
test <- twoClassSim(n = n_test, intercept = -13)
train[,c("Class")] <- ifelse(train[,c("Class")]=="Class2", 1, 0)
test[,c("Class")] <- ifelse(test[,c("Class")]=="Class2", 1, 0)

data <- as.h2o(localH2O, train)
newdata <- as.h2o(localH2O, test)
y <- "Class"
x <- setdiff(names(data), y)
family <- "binomial"


# Set up the ensemble by choosing a base learner library and metalearer
learner <- c("h2o.randomForest.wrapper", "h2o.gbm.wrapper", 
    "h2o.glm.wrapper", "h2o.deeplearning.wrapper")

metalearner <- "SL.glm"


# Train the ensemble
fit <- h2o.ensemble(x = x, y = y, data = data, family = family, 
                     learner = learner, metalearner = metalearner) 


# Generate predictions on the test set
pred <- predict(fit, newdata)


# Ensemble test AUC 
cvAUC(predictions=as.data.frame(pred$pred)[,1], labels=as.data.frame(newdata$Class)$Class)$cvAUC
# 0.9745787

# Base learner test set AUC (for comparison)
sapply(seq(ncol(pred$basepred)), function(i) cvAUC(as.data.frame(pred$basepred)[,i], as.data.frame(newdata$Class)$Class)$cvAUC) 
# 0.9519398 0.8957430 0.8838828 0.9604108

# Note that the ensemble results above are not reproducible since h2o.deeplearning is not reproducible when using multiple cores.  The results from the other three base learners should be reproducible, however.

}
}