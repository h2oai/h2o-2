## Load h2o package
library(h2o)

## Start a local H2O cluster
h2oHandle <- h2o.init(max_mem_size = "2g", nthreads = -1)

## Data Set Columns
##  1 CallCenterPostalCode    Call center postal code (5)
##  2 Date                    Exact date of filing
##  3 Demographics_Age        Age (in years) of policy holder
##  4 Demographics_AgeGroup   Age cohort for policy holder (5)
##  5 Demographics_Gender     Biological gender of policy holder (2)
##  6 FirstClaim              TRUE/FALSE indicator for first claim
##  7 INCID                   Incident identifier
##  8 INSID                   Insurance identifier
##  9 INStype                 Insurance type (3)
## 10 LossCode                Loss code (31)
## 11 MonthlyDate             Month and year of filing
## 12 PostalCode              Postal code for policy holder
## 13 Region                  Geographic region for policy holder (10)
## 14 ResponseStatus          Status of claim (10)
## 15 SourceCode              Source of filing (3)
## 16 StateName               US State of policy holder
## 17 Deduct                  Policy deductible
## 18 Fees                    Policy fees
## 19 TotalClaim              Total claim
## 20 TotalPaid               Total payout

dataPath <- file.path("..", "..", "smalldata", "insurance_claims.gz")
claims.dat <- h2o.importFile(h2oHandle, path = dataPath,
                             key = "claims.dat")

## Compare H2O compression versus gzip compression
h2oKVStore <- h2o.ls(h2oHandle)
h2oSize <- h2oKVStore$Bytesize[h2oKVStore$Key == "claims.dat"]
gzSize  <- file.info(dataPath)$size
h2oSize / gzSize # H2O roughly 1.8x larger than gzip

## Create high-level summary of the data
summary(claims.dat)
nrow(claims.dat)

## Find missing value counts
missingCount <- as.matrix(apply(claims.dat, 2, function(x) {sum(is.na(x))}))[,1L]
names(missingCount) <- colnames(claims.dat)
missingCount

## Create indicator variables for claim payout percentages
invisible(
  h2o.exec(claims.dat$ClaimPaidRatio <- claims.dat$TotalPaid / claims.dat$TotalClaim)
)

payoutThreshold <- 1
invisible(
  h2o.exec(claims.dat$HighClaimPaid <- claims.dat$ClaimPaidRatio > payoutThreshold)
)

## Create log transformed variables
invisible(
  h2o.exec(claims.dat$logDeduct <- log(claims.dat$Deduct + 1))
)

invisible(
  h2o.exec(claims.dat$logFees <- log(claims.dat$Fees + 1))
)

invisible(
  h2o.exec(claims.dat$logTotalClaim <- log(claims.dat$TotalClaim + 1))
)

invisible(
  h2o.exec(claims.dat$NonNegPaid <- ifelse(claims.dat$TotalPaid < 0,
                                           0, claims.dat$TotalPaid))
)

invisible(
  h2o.exec(claims.dat$logTotalPaid <- log(claims.dat$NonNegPaid + 1))
)

## Convert some numeric columns to factor
for (j in c("CallCenterPostalCode"))
  claims.dat[[j]] <- as.factor(claims.dat[[j]])

## Fun with h2o.table
stateByCallCenter <-
  h2o.table(claims.dat[c("StateName", "CallCenterPostalCode")],
            return.in.R = TRUE)
stateByCallCenter

# Limit analysis to "Claim Complete"
completed.claims.dat <-
  claims.dat[claims.dat$ResponseStatus == "Claim Complete", ]
completed.claims.dat <-
  h2o.assign(completed.claims.dat, "completed.claims.dat")

## Predict non-negative payout using a tweedie model
xvars <- c("CallCenterPostalCode", "Demographics_AgeGroup",
           "Demographics_Gender", "FirstClaim", "INStype",
           "LossCode", "MonthlyDate", "SourceCode",
           "StateName", "logDeduct", "logFees",
           "logTotalClaim")

system.time(
  tweedie.1.1 <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                         key = "tweedie.1.1", family = "tweedie",
                         tweedie.p = 1.1, alpha = 0.5,
                         lambda_search = TRUE, nlambda = 20)
)
system.time(
  tweedie.1.25 <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                         key = "tweedie.1.25", family = "tweedie",
                         tweedie.p = 1.25, alpha = 0.5,
                         lambda_search = TRUE, nlambda = 20)
)
system.time(
  tweedie.1.5 <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                         key = "tweedie.1.5", family = "tweedie",
                         tweedie.p = 1.5, alpha = 0.5,
                         lambda_search = TRUE, nlambda = 20)
)
system.time(
  tweedie.1.75 <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                          key = "tweedie.1.75", family = "tweedie",
                          tweedie.p = 1.75, alpha = 0.5,
                          lambda_search = TRUE, nlambda = 20)
)
system.time(
  tweedie.1.9 <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                         key = "tweedie.1.9", family = "tweedie",
                         tweedie.p = 1.9, alpha = 0.5,
                         lambda_search = TRUE, nlambda = 15)
)

N <- nrow(completed.claims.dat)
y <- completed.claims.dat$NonNegPaid

mu.1.1  <- h2o.predict(tweedie.1.1,  completed.claims.dat)
mu.1.25 <- h2o.predict(tweedie.1.25, completed.claims.dat)
mu.1.5  <- h2o.predict(tweedie.1.5,  completed.claims.dat)
mu.1.75 <- h2o.predict(tweedie.1.75, completed.claims.dat)
mu.1.9  <- h2o.predict(tweedie.1.9,  completed.claims.dat)

## Calculate saddlepoint estimation of phi
require(tweedie)
sum(tweedie.dev(y, mu.1.1,  1.1),  na.rm = TRUE) / N
sum(tweedie.dev(y, mu.1.25, 1.25), na.rm = TRUE) / N
sum(tweedie.dev(y, mu.1.5,  1.5),  na.rm = TRUE) / N
sum(tweedie.dev(y, mu.1.75, 1.75), na.rm = TRUE) / N
sum(tweedie.dev(y, mu.1.9,  1.9),  na.rm = TRUE) / N

## Generate tweedie models based upon the power transformations of 1.75
system.time(
  tweedie.ridge <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                           key = "tweedie.ridge", family = "tweedie",
                           tweedie.p = 1.75, alpha = 0,
                           lambda_search = TRUE, nlambda = 20)
)
table(coef(tweedie.ridge@model) != 0)
tweedie.ridge@model[c("lambda", "deviance", "null.deviance")]
system.time(
  tweedie.mix <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                         key = "tweedie.mix", family = "tweedie",
                         tweedie.p = 1.75, alpha = 0.5,
                         lambda_search = TRUE, nlambda = 20)
)
table(coef(tweedie.mix@model) != 0)
tweedie.mix@model[c("lambda", "deviance", "null.deviance")]
system.time(
  tweedie.lasso <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                           key = "tweedie.lasso", family = "tweedie",
                           tweedie.p = 1.75, alpha = 1,
                           lambda_search = TRUE, nlambda = 20)
)
table(coef(tweedie.lasso@model) != 0)
tweedie.lasso@model[c("lambda", "deviance", "null.deviance")]

findDroppedTerms <- function(object)
{
  coefs    <- coef(object@model)
  zeros    <- names(which(coefs == 0))
  nonzeros <- names(which(coefs != 0))
  zeros    <- unique(sapply(strsplit(zeros, "\\."), `[`, 1L))
  nonzeros <- unique(sapply(strsplit(nonzeros, "\\."), `[`, 1L))
  sort(as.character(setdiff(zeros, nonzeros)))
}

findDroppedTerms(tweedie.ridge)
findDroppedTerms(tweedie.mix)
findDroppedTerms(tweedie.lasso)

## Generate the final tweedie model
system.time(
  tweedie.final <- h2o.glm(xvars, "NonNegPaid", completed.claims.dat,
                           key = "tweedie.final", family = "tweedie",
                           tweedie.p = 1.75, alpha = 0, lambda = 12,
                           variable_importances = TRUE)
)

completed.claims.dat$predicted <-
  h2o.predict(tweedie.final,  completed.claims.dat)
completed.claims.dat$deviance <-
  tweedie.dev(y  = completed.claims.dat$NonNegPaid,
              mu = completed.claims.dat$predicted,
              power = 1.75)

claims.tweediedev.dat <-
  completed.claims.dat[!is.na(completed.claims.dat$deviance), ]
claims.tweediedev.dat <-
  h2o.assign(claims.tweediedev.dat, "claims.tweediedev.dat")

## Calcualte group by aggregates using h2o.ddply
devAggrFun <- function(x)
{
  cbind(N = nrow(x),
        MinDeviance  = min(x$deviance),
        MeanDeviance = mean(x$deviance),
        MaxDeviance  = max(x$deviance))
}
h2o.addFunction(h2oHandle, devAggrFun)
as.data.frame(h2o.ddply(claims.tweediedev.dat, "SourceCode", devAggrFun))
as.data.frame(h2o.ddply(claims.tweediedev.dat, "LossCode", devAggrFun))
as.data.frame(h2o.ddply(claims.tweediedev.dat,
                        c("MonthlyDate", "LossCode"), devAggrFun))
