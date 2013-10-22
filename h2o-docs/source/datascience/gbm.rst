Gradient Boosted Regression and Classification
----------------------------------------------
Gradient boosted regression and gradient boosted classification are forward learning ensemble methods. The guiding principal is that good predictive results can be obtained through increasingly refined approximations. 

Defining a GBM Model
""""""""""""""""""""

**Destination Key:**

  A user defined name for the model. 

**Source**

  The .hex key associated with the parsed data to be used in the model.

**Response**

  The response variable.

**Ignored Columns**

  By default all of the information submitted in a data frame will be used in building the   GBM model. Users specify those attributes that should be omitted from analysis. 

**Classification**

  A tic-box option that, when checked, treats the outcome variable as categorical, and when unchecked treats the outcome variable as continuous and normally distributed. 

**Validation** 

  A .hex key associated with data to be used in validation of the model built using the data specified in **Source**.

**NTrees**

  The number of trees to be built. 

**Max Depth** 

  The maximum number of edges to be generated between the first node and the terminal node. 

**Min Rows** 

  The minimum number of observations to be included in a terminal leaf. If any classification must consist of no fewer than five elements, min rows should be set to five. 

**N Bins**

  The number of bins data are partitioned into before the best split point is determined. A high number of bins relative to a low number of observations will have a small number of observations in each bin. 

**Learn Rate**

  A number between 0 and 1 that specifies the rate at which the algorithm should converge. Learning rate is inversely related to the number of iterations taken for the algorithm to complete. 

Interpreting Results
"""""""""""""""""""""

GBM results are comprised of a confusion matrix and the mean squared error of each tree. 

An example of a confusion matrix is given below:

The highlighted fields across the diagonal indicate the number the
number of true members of the class who were correctly predicted as
true. The overall error rate is shown in the bottom right field. It reflects
the proportion of incorrect predictions overall.  

.. image:: GBMmatrix.png
   :width: 70 %

**MSE**

  Mean squared error is an indicator of goodness of fit. It measures the squared distance between an estimator and the estimated parameter. 


GBM Algorithm
""""""""""""""

H\ :sub:`2`\ O's Gradient Boosting Algorithm follows the standard set by Hastie et
al (2001):
GBM
-----

H2O's gradient boosted regression and gradient boosted classification follow the algorithm specified in Elements of Statistical Learning (year). 

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
Vol.1. N.p.: Springer New York, 2001. 
http://www.stanford.edu/~hastie/local.ftp/Springer/OLD//ESLII_print4.pdf








