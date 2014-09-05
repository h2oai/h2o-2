# L-BFGS for Least Square estimate in GLM 
#
# INPUT:
#      bfgs will take the input data:     X = [x_1, x_2...x_n]' where x_i \in R^m
#                    observed output:     Y \in R^n
#            initial guess of params:     beta0 \in R^n
#    the number of steps we remember:     m
#
# OUTPUT:
#            the optimal params: beta
# according to the loss function: L = 0.5 (X beta - Y)' (X beta - Y) + 0.5 lambda beta' beta
#
#
# STORAGE:
#       in the algorithm we only maintain ss[k] ... ss[k-m+1] which are the history of steps
#                                         ys[k] ... ys[k-m+1] which are the history of change of gradients
rm(list = ls())

LBFGS <- function(X, Y, beta0, m) {
  # X,Y are only needed for computing gradient
  converge = FALSE
  k <- 1
  x_cur = beta0
  x_old = beta0
  g_cur = g(x_old)
  g_old = g(x_old)
  ss = list()
  ys = list()
  step = 1
  # init ss ys in global workspace
  while (k<1000) {
    q = g_old
    p <- getPk(ss,ys,q, m, k)
#    p <- -1 * q
    step = getStepSize(100, p, x_old) # by some other line search
    x_cur = x_old + step * p
    g_cur = g(x_cur)
    # not restricting space here
    ss[[k]] = x_cur - x_old
    ys[[k]] = g_cur - g_old
    # clean ss[k-m] ys[k-m]
    converge=(f(x_old) - f(x_cur))<0.0001
    x_old = x_cur
    g_old = g_cur
    k <- k+1
    print(paste("Iteration:",k,"  Objective:", f(x_cur), "  Step Size: ", step, " Max Gradient Component: ", max(abs(g_cur))))
#    print(x_cur)
#    print(g(x_cur))
  }
  return(x_cur)
}

# params: @q: the current gradient
#         
getPk <- function(ss,ys,q, m, k) {
  alpha = c()
  for (i in (k-1):(k-m)) {
    if ( i>=1 ) {
      alpha[i] = rho(i,ss,ys) * t(ss[[i]]) %*% q
      q = q - alpha[i] * ys[[i]]
    }
  }
  Hk0 <- getHk0(ss,ys,k)
  r <- Hk0 %*% q
  for (i in (k-m):(k-1)) {
    if ( i>=1 ) {
      b = rho(i,ss,ys) * t(ys[[i]]) %*% r
      r = r+  (alpha[i] - b[1,1]) * ss[[i]]
    }
  }
  return(-1*r)
}

getHk0 <- function(ss,ys,k) {
  if (k==1) {
    return(diag(N))
  } else {
    I = diag(N) # identity of length n
    niu = t(ss[[k-1]]) %*% ys[[k-1]] / (t(ys[[k-1]]) %*% ys[[k-1]])
    return(niu[1,1] * I)
  }
}

g <- function(beta) {
  # returns the gradient
  return(-1 * t(X) %*% Y + gram %*% beta)
}

f <- function(beta) {
  return( t(X %*% beta - Y) %*% (X %*% beta - Y) + lambda * t(beta) %*% beta)
}

getStepSize <- function(step, p, x_old) {
  alpha = 0.2 # the rate everytime we shrink
  c1 = 0.2 # parameter to prevent too short step
  iter = 0
  f_o = f(x_old)
  gdt = g(x_old)/M
  while (iter < 20) {
    obj_old = f_o + c1* step * t(gdt) %*% p
    obj_new = f(x_old + step * p)
    if (obj_new < obj_old) {
      return(step)
    } else {
      step = step*alpha
    }
    iter = iter+1
  }
  return(step)
}

rho <- function(k,ss,ys) {
  return(1/(t(ys[[k]]) %*% ss[[k]]))
}

normalData=FALSE
source <- read.csv("~/Documents/workspace/h2o/smalldata/airlines/AirlinesTrain.csv")
X <- model.matrix(data=source, IsDepDelayed_REC~.)
if (normalData) {
  X <- scale(X)
}
# or we can write X = source[, names(source)!="RACE" || ...]
Y = source$IsDepDelayed_REC
M = length(Y)
if (normalData) {
  X[,1] = rep(1,M) # after scale, force the intercept to be 1
}
N = length(X[1,])
m = 10 # size of the history we want to use
beta0 = runif(N,-1,1)
lambda = 10000
stepSize = 1
gram <- t(X) %*% X + lambda * diag(N)
solution = solve(gram, t(X)%*%Y)
print(f(solution))
prediction = LBFGS(X,Y,beta0,m)
print(f(solution))

