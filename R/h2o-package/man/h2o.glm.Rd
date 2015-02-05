\name{h2o.glm}
\alias{h2o.glm}
\title{
H2O: Generalized Linear Models
}
\description{
Fit a generalized linear model, specified by a response variable, a set of
predictors, and a description of the error distribution.
}
\usage{
h2o.glm(x, y, data, key = "", offset = NULL, family, link,
        tweedie.p = ifelse(family == "tweedie", 1.5, NA_real_),
        prior = NULL, nfolds = 0, alpha = 0.5, lambda = 1e-5,
        lambda_search = FALSE, nlambda = -1, lambda.min.ratio = -1,
        max_predictors = -1, return_all_lambda = FALSE,
        strong_rules = TRUE, standardize = TRUE, intercept = TRUE,
        non_negative = FALSE, use_all_factor_levels = FALSE,
        variable_importances = FALSE, epsilon = 1e-4, iter.max = 100,
        higher_accuracy = FALSE, beta_constraints = NULL, 
        disable_line_search = FALSE)
}
\arguments{
  \item{x}{A character vector containing the column names of the predictors in
    the model.}
  \item{y}{A character string representing the response variable in the model.}
  \item{data}{An \code{\linkS4class{H2OParsedData}} object containing the
    variables in the model.}
  \item{key}{An optional unique hex key assigned to the resulting model.
    If none is given, a key will automatically be generated.}
  \item{offset}{An optional character string representing the offset term in
    the model.}
  \item{family}{A character string specifying the error distribution of the
    model; one of \code{"gaussian"}, \code{"binomial"}, \code{"poisson"},
    \code{"gamma"}, and \code{"tweedie"}.}
  \item{link}{A character string specifying the link function. The default is
    the canonical link for the \code{family}. The supported links for each of
    the \code{family} specifications are:\cr
    \cr
    \code{"gaussian"}: \code{"identity"}, \code{"log"}, \code{"inverse"}\cr
    \code{"binomial"}: \code{"logit"}, \code{"log"}\cr
    \code{"poisson"}: \code{"log"}, \code{"identity"}\cr
    \code{"gamma"}: \code{"inverse"}, \code{"log"}, \code{"identity"}\cr
    \code{"tweedie"}: \code{"tweedie"}\cr
  }
  \item{tweedie.p}{A numeric specifying the power for the variance function
    when \code{family = "tweedie"}.}
  \item{prior}{An optional numeric specifying the prior probability of class 1
    in the response when \code{family = "binomial"}. The default prior is the
    observational frequency of class 1.}
  \item{nfolds}{A non-negative integer specifying the number of folds for
    cross-validation and \code{nfolds = 0} indicates no cross-validation.}
  \item{alpha}{A numeric in [0, 1] specifying the elastic-net mixing parameter.
    The elastic-net penalty is defined to be
    \deqn{P(\alpha,\beta) = (1-\alpha)/2||\beta||_2^2 + \alpha||\beta||_1 = \sum_j [(1-\alpha)/2 \beta_j^2 + \alpha|\beta_j|]},
    making \code{alpha = 1} the lasso penalty and \code{alpha = 0} the ridge
    penalty.}
  \item{lambda}{A non-negative shrinkage parameter for the elastic-net, which
    multiplies \eqn{P(\alpha,\beta)} in the objective. When \code{lambda = 0},
    then no elastic-net penalty is applied and ordinary generalized linear
    models are fit.}
  \item{lambda_search}{A logical value indicating whether to conduct a search
    over the space of lambda values starting from the \code{lambda} argument
    to \code{lambda.min.ratio} times the smallest lambda that produces zeros
    for all the coefficient estimates.}
  \item{nlambda}{The number of lambda values to use when
    \code{lambda_search = TRUE}.}
  \item{lambda.min.ratio}{A non-negative number that specifies the minimum
    value for lambda as a fraction of smallest lambda that yields the zero
    vector for the coefficient estimates.}
  \item{max_predictors}{When \code{lambda_search = TRUE}, a non-negative
    integer specifying an early stopping rule for the maximum number of
    predictors in the model.}
  \item{return_all_lambda}{A logical value indicating whether to return every
    model built during the lambda search. If \code{return_all_lambda = FALSE},
    then only the model corresponding to the optimal lambda will be returned.}
  \item{strong_rules}{A logical value indicating whether to use strong rules to
    remove predictors with gradients near zero at the starting solution
    \emph{prior} to model training.}
  \item{standardize}{A logical value indicating whether the numeric predictors
    should be standardized to have a mean of 0 and a variance of 1 prior to
    training the models.}
  \item{intercept}{A logical value indicating whether to include the intercept
    term in the models. This will only have a practical effect in the presence
    of all numeric predictors.}
  \item{non_negative}{A logical value indicating whether the coefficient
    estimates will be constrained to be non-negative.}
  \item{use_all_factor_levels}{A logical value indicating whether dummy
    variables should be used for all factor levels of the categorical predictors.
    When \code{TRUE}, results in an over parameterized models.}
  \item{variable_importances}{A logical value indicating whether the variable
    importances should be computed.}
  \item{epsilon}{A non-negative number specifying the magnitude of the maximum
    difference between the coefficient estimates from successive iterations.
    Defines the convergence criterion for \code{h2o.glm}.}
  \item{iter.max}{A non-negative integer specifying the maximum number of
    iterations.}
  \item{higher_accuracy}{A logical value indicating whether to use line search
    to produce more accurate estimates.}
  \item{beta_constraints}{
    A data.frame or H2OParsedData object with the columns ["names", "lower_bounds", "upper_bounds", "beta_given"],
    where each row corresponds to a predictor in the GLM. "names" contains the predictor names, "lower"/"upper_bounds",
    are the lower and upper bounds of beta, and "beta_given" is some supplied starting values for the coefficients.
  }
  \item{disable_line_search}{A logical value indicating whether line search should be disabled.}
}
\value{
  An object of class \code{\linkS4class{H2OGLMModel}} with slots \code{key},
  \code{data}, \code{model}, and \code{xval}. The \code{model} slot is a list of
  the following components:
  \item{coefficients}{A named vector of the coefficients estimated in the model.}
  \item{rank}{The numeric rank of the fitted linear model.}
  \item{family}{The family of the error distribution.}
  \item{deviance}{The deviance of the fitted model.}
  \item{aic}{Akaike's Information Criterion for the final computed model.}
  \item{null.deviance}{The deviance for the null model.}
  \item{iter}{Number of algorithm iterations to compute the model.}
  \item{df.residual}{The residual degrees of freedom.}
  \item{df.null}{The residual degrees of freedom for the null model.}
  \item{y}{The response variable in the model.}
  \item{x}{A vector of the predictor variable(s) in the model.}
  \item{auc}{Area under the curve.}
  \item{training.err}{Average training error.}
  \item{threshold}{Best threshold.}
  \item{confusion}{Confusion matrix.}
  The \code{xval} slot is a list of \code{\linkS4class{H2OGLMModel}} objects
  representing the cross-validation models. (Each of these objects themselves
  has xval equal to an empty list).
}
\seealso{
  \code{\link{h2o.gbm}}, \code{\link{h2o.randomForest}}
}
\examples{
# -- CRAN examples begin --
library(h2o)
localH2O = h2o.init()

# Run GLM of CAPSULE ~ AGE + RACE + PSA + DCAPS
prostatePath = system.file("extdata", "prostate.csv", package = "h2o")
prostate.hex = h2o.importFile(localH2O, path = prostatePath, key = "prostate.hex")
h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, 
        family = "binomial", nfolds = 0, alpha = 0.5, lambda_search = FALSE, 
        use_all_factor_levels = FALSE, variable_importances = FALSE,
        higher_accuracy = FALSE)

# Run GLM of VOL ~ CAPSULE + AGE + RACE + PSA + GLEASON
myX = setdiff(colnames(prostate.hex), c("ID", "DPROS", "DCAPS", "VOL"))
h2o.glm(y = "VOL", x = myX, data = prostate.hex, family = "gaussian",
        nfolds = 0, alpha = 0.1, lambda_search = FALSE,
        use_all_factor_levels = FALSE, variable_importances = FALSE,
        higher_accuracy = FALSE)
# -- CRAN examples end --

\dontrun{
# GLM variable importance
# Also see:
#   https://github.com/h2oai/h2o/blob/master/R/tests/testdir_demos/runit_demo_VI_all_algos.R
data.hex = h2o.importFile(
  localH2O,
  path = "https://raw.github.com/h2oai/h2o/master/smalldata/bank-additional-full.csv",
  key = "data.hex")
myX = 1:20
myY="y"
my.glm = h2o.glm(x=myX, y=myY, data=data.hex, family="binomial",
                 standardize=TRUE, use_all_factor_levels=TRUE,
                 higher_accuracy=TRUE, lambda_search=TRUE,
                 return_all_lambda=TRUE, variable_importances=TRUE)
best_model = my.glm@best_model
n_coeff = abs(my.glm@models[[best_model]]@model$normalized_coefficients)
VI = abs(n_coeff[-length(n_coeff)])
glm.VI = VI[order(VI,decreasing=T)]
print(glm.VI)
}
}
