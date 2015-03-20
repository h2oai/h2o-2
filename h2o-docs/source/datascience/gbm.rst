.. _GBMmath:

Gradient Boosted Regression and Classification
================================================

Gradient Boosted Regression and Gradient Boosted Classification are
forward learning ensemble methods. The guiding heuristic is that good
predictive results can be obtained through increasingly refined approximations. 

""""

Defining a GBM Model
""""""""""""""""""""

**Destination Key:**

  A user-defined name for the model. 

**Source:**

  The .hex key associated with the parsed data to be used in the model.

**Response:**

  The dependent variable to use for the model. Dependent variables can be
  binomial indicators or multinomial classes.

**Ignored Columns:**

  By default, all of the information submitted in a data set is
  used to build the GBM model. To omit attributes from analysis, highlight them.
  
**Classification:**

  Check this checkbox to treat the outcome variable as categorical, or 
  uncheck it to treat the outcome variable as continuous. If a
  continuous real variable is defined for the response, H2O returns an error if a classification model is requested. 

**Validation:** 

  A .hex key associated with data to be used in validation of the
  model built using the data specified in **Source**.

**N folds**

  Number of folds for cross-validation (if no validation data is specified)
  
**Holdout fraction**

  Fraction of training data (from end) to hold out for validation (if no validation data is specified).
  
**Keep cross validation dataset splits**

  Preserve cross-validation dataset splits.
  
**NTrees:**

  The number of trees to build. To specify models with different total numbers
  of trees, enter the values as a
  comma-separated list. For example, to specify different models with
  200, 100 and 50 trees respectively, enter "200, 100, 50".


**Max Depth:** 

  The maximum number of edges to generate between the first node
  and the terminal node. To test different depths, values can be
  specified in a comma-separated list.  

**Min Rows:** 

  The minimum number of observations to include in a terminal
  leaf. For example, to ensure a classification consists of no fewer than five
  elements, set the min rows value to five. 

**NBins:**

  The number of bins used for data partitioning before the best split
  point is determined. A high number of bins for a low number
  of observations has a small number of observations in each
  bin. As the number of bins approaches the number of unique values in
  a column, the analysis approaches evaluation of all possible split
  points. 

**Score Each Iteration:** 

  Return error rate information after each tree in the
  requested set is built. This option allows users to evaluate the
  marginal gain in fit from building that tree so they can interrupt the model when the gain for building the next tree isn't
  substantial enough to continue building. 
  This option can slow the model building process, depending on the
  size and shape of both the training data and the testing data. 

**Importance:**

  Return information about each variable's importance
  in training the specified model. 

**Balance classes**

  For imbalanced data, balance training data class counts via over/under-sampling for improved predictive accuracy.
    
**Class sampling factors**

  Specify the over/under-sampling ratios per class (lexicographic order). 
  
**Max after balance size**

  If classes are balanced, limit the resulting dataset size to the specified multiple of the original dataset size.

**Checkpoint**

   A model key associated with a previously trained model. Use this option to build a new model as a continuation of a previously generated model (e.g., by a grid search).      

**Overwrite checkpoint**

  Overwrite the checkpoint. 
  
  
**Family**

  Select the family (auto or bernoulli).  


**Learn Rate:**

  A number between 0 and 1 that specifies the rate at which the
  algorithm should converge. Learning rate is inversely related to the
  number of iterations required for the algorithm to complete. 

**Grid Parallelism:** 

  When multiple models are requested through the grid search options,
  such as specification of multiple learning rates, selecting this
  option will build the set of models in parallel, rather than
  sequentially.

**Seed**

  The random seed controls sampling and initialization. Reproducible results are only expected with single-threaded operation (i.e., when running on one node, turning off load balancing and providing a small dataset that fits in one chunk).  In general, the multi-threaded asynchronous updates to the model parameters will result in (intentional) race conditions and non-reproducible results. Note that deterministic sampling and initialization might still lead to some weak sense of determinism in the model.

**Group split**

  Perform group splitting categoricals. 


  
""""  

Treatment of Factors
"""""""""""""""""""""

  When the specified GBM model includes factors, those factors are
  analyzed by assigning an integer to each distinct factor level, and
  then binning the ordered integers according to the user-specified
  number of bins (N Bins). Split points are determined by considering the end points of each bin and the one versus many split for each bin. 

  For example, if the factor is split into 5 bins, H2O orders the bins by 
  bin number, then considers the split between the first and second bin, then the 
  second and third, then the third and fourth, and the fourth and fifth. 
  Additionally the split that results from splitting the first
  bin from the other four and all analogous splits for the other four
  bins are considered. To specify a model that considers all
  factors individually, set the value for N
  Bins equal to the number of factor levels. This can be done for over 1024 levels (the maximum number of levels that can be
  handled in R), though this increases the time to fully generate a
  model. 

""""

Interpreting Results
"""""""""""""""""""""

The GBM results for classification models are comprised of a confusion
matrix and the mean squared error of each tree. When the MSE for 
each tree is returned, the first and second MSE values are the same. 
The initial MSE is calculated for the dependent variable, and is given 
as a baseline for evaluating the predictive performance of 
each next basis function. The first MSE value given is the MSE for the 
data set before any trees are built. 

An example of a confusion matrix is given below:

The highlighted fields across the diagonal indicate the number the
number of true members of the class who were correctly predicted as
true. The overall error rate is shown in the bottom right field. It reflects
the proportion of incorrect predictions overall.  

.. Image:: GBMmatrix.png
   :width: 70 %

|

**MSE**

  Mean squared error is an indicator of goodness of fit. It measures
  the squared distance between an estimator and the estimated parameter. 

**Cost of Computation**

  The cost of computation in GBM is bounded above in the following way:

  :math:`Cost = bins\times (2^{leaves}) \times columns \times classes`

""""

GBM Algorithm
""""""""""""""

H2O's Gradient Boosting Algorithms follow the algorithm specified by Hastie et
al (2001):


Initialize :math:`f_{k0} = 0,\: k=1,2,…,K`

:math:`For\:m=1\:to\:M:`
	:math:`(a)\:Set\:`
	:math:`p_{k}(x)=\frac{e^{f_{k}(x)}}{\sum_{l=1}^{K}e^{f_{l}(x)}},\:k=1,2,…,K`


	:math:`(b)\:For\:k=1\:to\:K:`

	:math:`\:i.\:Compute\:r_{ikm}=y_{ik}-p_{k}(x_{i}),\:i=1,2,…,N.`

	:math:`\:ii.\:Fit\:a\:regression\:tree\:to\:the\:targets\:r_{ikm},\:i=1,2,…,N`
	
	:math:`giving\:terminal\:regions\:R_{jim},\:j=1,2,…,J_{m}.`

	:math:`\:iii.\:Compute`

		:math:`\gamma_{jkm}=\frac{K-1}{K}\:\frac{\sum_{x_{i}\in R_{jkm}}(r_{ikm})}{\sum_{x_{i}\in R_{jkm}}|r_{ikm}|(1-|r_{ikm})},\:j=1,2,…,J_{m}.`

	:math:`\:iv.\:Update\:f_{km}(x)=f_{k,m-1}(x)+\sum_{j=1}^{J_{m}}\gamma_{jkm}I(x\in\:R_{jkm}).`
	      

Output :math:`\:\hat{f_{k}}(x)=f_{kM}(x),\:k=1,2,…,K.` 

**BETA: Standalone Scoring:**

  To download a generated GBM model in Java code format, click the **Java Model** button in the upper
  right corner. If the model is small enough, the Java code for the
  model can be inspected in the GUI; larger
  models can be inspected after downloading the model. 

  To download the model:
  
  #. Open the terminal window.
  #. Create a directory location for the model.
  #. Set the new directory as the working directory. 
  #. Follow the curl and java compile commands displayed in the instructions at the top of the Java model.  


.. Image:: GBMjavaout.png
   :width: 70 %  
   
""""""""   

Reference
"""""""""

Dietterich, Thomas G, and Eun Bae Kong. "Machine Learning Bias,
Statistical Bias, and Statistical Variance of Decision Tree
Algorithms." ML-95 255 (1995).

Elith, Jane, John R Leathwick, and Trevor Hastie. "A Working Guide to
Boosted Regression Trees." Journal of Animal Ecology 77.4 (2008): 802-813

Friedman, Jerome H. "Greedy Function Approximation: A Gradient
Boosting Machine." Annals of Statistics (2001): 1189-1232.

Friedman, Jerome, Trevor Hastie, Saharon Rosset, Robert Tibshirani,
and Ji Zhu. "Discussion of Boosting Papers." Ann. Statist 32 (2004): 
102-107

Friedman, Jerome, Trevor Hastie, and Robert Tibshirani. "Additive
Logistic Regression: A Statistical View of Boosting (With Discussion
and a Rejoinder by the Authors)." The Annals of Statistics 28.2
(2000): 337-407
http://projecteuclid.org/DPubS?service=UI&version=1.0&verb=Display&handle=euclid.aos/1016218223

Hastie, Trevor, Robert Tibshirani, and J Jerome H Friedman. The
Elements of Statistical Learning.
Vol.1. N.p., page 339: Springer New York, 2001. 
http://www.stanford.edu/~hastie/local.ftp/Springer/OLD//ESLII_print4.pdf


""""""






