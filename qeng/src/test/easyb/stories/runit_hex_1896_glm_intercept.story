tags "runit_hex_1896_glm_intercepts"

"""
This story creates the acceptance criteria for the GLM Intercept as described in HEX 1896
"""

description
"""
  Using prostate data set we check results from h2o.glm against glmnet
  """
scenario "prostate dataset compare", {
    given "import prostate dataset"
    when "create h2o cluster(s) with 5 nodes to run the following in parallel"
    and "chunks:[1,2,10,100], intercept:[true, false], folds:[0,1,10,50], alpha:[0, 0.5, 1]"
    and "we use the family in [binomial, poisson, gamma, gaussian"
    then "coefficients are computed with less then epsilon in [0.06, 0.07, 0.08, 0.09, 0.1]"
}

description
"""
  Using airlines data set we check results from h2o.glm against glmnet
  """
scenario "airlines dataset compare", {
    given "import airlines dataset"
    when "create h2o cluster(s) with 5 nodes to run the following in parallel"
    and "chunks:[1,2,10,100], intercept:[true, false], folds:[0,1,10,50], alpha:[0, 0.5, 1]"
    and "we use the family in [binomial, poisson, gamma, gaussian"
    then "coefficients are computed with less then epsilon in [0.06, 0.07, 0.08, 0.09, 0.1]"
}

description
"""
  Using arcene data set we check results from h2o.glm against glmnet
  """
scenario "arcene dataset compare", {
    given "import arcene dataset"
    when "create h2o cluster(s) with 5 nodes to run the following in parallel"
    and "chunks:[1,2,10,100], intercept:[true, false], folds:[0,1,10,50], alpha:[0, 0.5, 1]"
    and "we use the family in [binomial, poisson, gamma, gaussian"
    then "coefficients are computed with less then epsilon in [0.06, 0.07, 0.08, 0.09, 0.1]"
}

description
"""
  Using collective data set we check results from h2o.glm against glmnet
  """
scenario "collective dataset compare", {
    given "import collective dataset"
    when "create h2o cluster(s) with 5 nodes to run the following in parallel"
    and "chunks:[1,2,10,100], intercept:[true, false], folds:[0,1,10,50], alpha:[0, 0.5, 1]"
    and "we use the family in [binomial, poisson, gamma, gaussian"
    then "coefficients are computed with less then epsilon in [0.06, 0.07, 0.08, 0.09, 0.1]"
}
