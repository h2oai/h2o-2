.. _R_Tutorial:

R Tutorial
==========

This tutorial provides a sample workflow for new users of H2O's R API.
Readers will learn the basic syntax of H2O, including importing and parsing files,
specifying a model, and obtaining model output.

New H2O users should refer to the `quick start guide <http://s3.amazonaws.com/h2o-release/h2o/master/1532/docs-website/newuser/top.html>`_ for additional
instructions on how to run H2O. The following tutorial assumes that H2O is installed in R.

""""

Getting Started
"""""""""""""""

R uses an REST API to send functions to H2O, so a reference object  in R to the H2O instance is required.
You can start H2O outside of R and connect to it. You can also launch directly from R, but if you close the R session, the H2O instance is closed as well. The client object is used to direct R to datasets and models located in
H2O.

**Launch From R**

By default, if the argument **max_mem_size** is not specified when running **h2o.init()**, the heap size of the H2O running on 32-bit Java is
1g. On 64-bit Java, the heap size is 1/4 of the total memory available on the machine. For a 32-bit version, the function  runs a check
and suggests an upgrade.

::

 > library(h2o)
 > localH2O <- h2o.init(ip = 'localhost', port = 54321, max_mem_size = '4g')
  Successfully connected to http://localhost:54321 
	R is connected to H2O cluster:
    H2O cluster uptime:         11 minutes 35 seconds 
    H2O cluster version:        2.7.0.1497 
    H2O cluster name:           H2O_started_from_R 
    H2O cluster total nodes:    1 
    H2O cluster total memory:   3.56 GB 
    H2O cluster total cores:    8 
    H2O cluster allowed cores:  8 
    H2O cluster healthy:        TRUE 

	


**Launch From Command Line**

Follow one of the `deployment tutorials <http://docs.0xdata.com/index.html?highlight=deployment>`_ to launch an instance from the command line:
	* on your desktop
	* on ec2 instances
	* on Hadoop servers
After launching the H2O cluster, initialize the connection by taking one node in the cluster and run **h2o.init** with the node's 
IP Address and port in the parentheses.
Note that the IP Address must be on your local machine. For the following example, change **192.168.1.161** to your local host. 
::

 > library(h2o)
 > localH2O <- h2o.init(ip = '192.168.1.161', port =54321)

.. WARNING::
  If the version of the current H2O instance is not the same as the package version loaded in R,
  a "version mismatch" warning message displays. To fix this issue, update the R package
  or launch an H2O instance using the jar file from the installed package.

::

	Error in h2o.init():
	Version mismatch! H2O is running version # but R package is version # 
  	


**Cluster Info**

To check the status and health of the H2O cluster, use **h2o.clusterInfo()** to display an easy-to-read
summary of information about the cluster.

::

  > library(h2o)
  > localH2O = h2o.init(ip = 'localhost', port = 54321)
  > h2o.clusterInfo(localH2O)
  R is connected to H2O cluster:
    H2O cluster uptime:         43 minutes 43 seconds 
    H2O cluster version:        2.7.0.1497 
    H2O cluster name:           H2O_started_from_R 
    H2O cluster total nodes:    1 
    H2O cluster total memory:   3.56 GB 
    H2O cluster total cores:    8 
    H2O cluster allowed cores:  8 
    H2O cluster healthy:        TRUE 

""""


Importing Data
""""""""""""""

**Import File**

The H2O package consolidates all of the various supported import functions. Although **h2o.importFolder** and **h2o.importHDFS** will still work, these functions are deprecated and should be updated to **h2o.importFile**.

::

  ## To import small iris data file from H2O's package 
  > irisPath = system.file("extdata", "iris.csv", package="h2o")
  > iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
	|=================================================| 100%

  ## To import an entire folder of files as one data object
  > pathToFolder = "/Users/Amy/0xdata/data/airlines/"
  > airlines.hex = h2o.importFile(localH2O, path = pathToFolder, key = "airlines.hex")
	|=================================================| 100%  

  ## To import from HDFS
  > pathToData = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
  > airlines.hex = h2o.importFile(localH2O, path = pathToData, key = "airlines.hex")
	|=================================================| 100%
  

**Upload File**

To upload a file from your local disk, **importFile** is recommended. However, you can still run **upload file**.

::

  > irisPath = system.file("extdata", "iris.csv", package="h2o")
  > iris.hex = h2o.uploadFile(localH2O, path = irisPath, key = "iris.hex")
  |====================================================| 100%


""""

Data Manipulation and Description
"""""""""""""""""""""""""""""""""
**Any Factor**

  Determine if any column in a data set is a factor.

::

  > irisPath = system.file("extdata", "iris_wheader.csv", package="h2o")
  > iris.hex = h2o.importFile(localH2O, path = irisPath)
  |===================================================| 100%
  > h2o.anyFactor(iris.hex)
  [1] TRUE


**As Data Frame**

  Convert an H2O parsed data object into an R data frame
  that can be manipulated using R calls. While this can be very useful, be careful with **as.data.frame** when
  converting H2O Parsed Data objects. Data sets that are easily and
  quickly handled by H2O are often too large to be treated
  equivalently well in R. 

::

  > prosPath <- system.file("extdata", "prostate.csv", package="h2o")
  > prostate.hex = h2o.importFile(localH2O, path = prosPath)
   |===================================================| 100%

  > prostate.data.frame<- as.data.frame(prostate.hex)
  > summary(prostate.data.frame)
        ID            CAPSULE            AGE             RACE      
 Min.   :  1.00   Min.   :0.0000   Min.   :43.00   Min.   :0.000  
 1st Qu.: 95.75   1st Qu.:0.0000   1st Qu.:62.00   1st Qu.:1.000  
 	....
  > head(prostate.data.frame)
  	  ID CAPSULE AGE RACE DPROS DCAPS  PSA  VOL GLEASON
	1  1       0  65    1     2     1  1.4  0.0       6
	2  2       0  72    1     3     2  6.7  0.0       7
		....


**As Factor**

  Convert an integer into a non-ordered factor (also
  called an enum or categorical).

::

  > prosPath = system.file("extdata", "prostate.csv", package="h2o")
  > prostate.hex = h2o.importFile(localH2O, path = prosPath)
  |===================================================| 100%
  > prostate.hex[,4] = as.factor(prostate.hex[,4])
  > summary(prostate.hex)
  	ID               CAPSULE          AGE             RACE    DPROS          
 	Min.   :  1.00   Min.   :0.0000   Min.   :43.00   1 :341  Min.   :1.000  
 	1st Qu.: 95.75   1st Qu.:0.0000   1st Qu.:62.00   2 : 36  1st Qu.:1.000  
 		....



**As H2O** 

  Pass a data frame from inside the R environment to the H2O instance.

::

  > data(iris)
  > summary(iris)
   	Sepal.Length    Sepal.Width     Petal.Length    Petal.Width   
	 Min.   :4.300   Min.   :2.000   Min.   :1.000   Min.   :0.100  
 	1st Qu.:5.100   1st Qu.:2.800   1st Qu.:1.600   1st Qu.:0.300 
 		....
  > iris.r <- iris
  > iris.h2o <- as.h2o(localH2O, iris.r, key="iris.h2o")
    |===================================================| 100%
  > class(iris.h2o)
  	[1] "H2OParsedData"
	attr(,"package")
	[1] "h2o"



**Assign H2O**

  Create a hex key on the server running H2O for data sets manipulated in R. 
  For instance, in the example below, the prostate data set was
  uploaded to the H2O instance and manipulated to remove
  outliers. To save the new data set on the H2O server so that it can
  be subsequently be analyzed with H2O without overwriting the original
  data set, use **h2o.assign**.

::
 
  > prosPath = system.file("extdata", "prostate.csv", package="h2o")
  > prostate.hex = h2o.importFile(localH2O, path = prosPath)
      |===================================================| 100%      
  > prostate.qs = quantile(prostate.hex$PSA)
  > PSA.outliers = prostate.hex[prostate.hex$PSA 
  <= prostate.qs[2] | prostate.hex$PSA >=   prostate.qs[10],]
  > PSA.outliers = h2o.assign(PSA.outliers, "PSA.outliers")
  > nrow(prostate.hex) 
  [1] 380 
  > nrow(PSA.outliers)
  [1] 380


**Colnames**

  Obtain a list of the column names in a data set. 

::

  > irisPath = system.file("extdata", "iris.csv", package="h2o")
  > iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
        |===================================================| 100%      
  > colnames(iris.hex)
  [1] "C1" "C2" "C3" "C4" "C5"
  

**Extremes**

 Obtain the maximum and minimum values in real-valued columns. 

::

  > ausPath = system.file("extdata", "australia.csv", package="h2o")
  > australia.hex = h2o.importFile(localH2O, path = ausPath, key = "australia.hex")
   |===================================================| 100%
  > min(australia.hex)
  [1] 0
  > min(c(-1, 0.5, 0.2), FALSE, australia.hex[,1:4])
  [1] -1


**Quantile**

  Request quantiles for an H2O parsed data set. To request a quantile for a single numeric column, use the column name (for example, **$AGE**). When you request
  for a full parsed data set, **quantile()** returns a matrix that displays
  quantile information for all numeric columns in the data set.
 

::

  > prosPath = system.file("extdata", "prostate.csv", package="h2o")
  > prostate.hex = h2o.importFile(localH2O, path = prosPath)
     |===================================================| 100%
  > quantile(prostate.hex$AGE)


**Summary**

  Generate an R-like summary for each of the columns in a data
  set. For continuous real functions, this produces a summary that includes
  information on quartiles, min, max, and mean. For factors, this
  produces information about counts of elements within each factor
  level. For information on the Summary algorithm, see :ref:`SUMmath`.

::

   > prosPath = system.file("extdata", "prostate.csv", package="h2o")
   > prostate.hex = h2o.importFile(localH2O, path = prosPath)
        |===================================================| 100%
   > summary(prostate.hex)
           ID            CAPSULE            AGE             RACE      
 Min.   :  1.00   Min.   :0.0000   Min.   :43.00   Min.   :0.000  
 1st Qu.: 95.75   1st Qu.:0.0000   1st Qu.:62.00   1st Qu.:1.000  
 	....
   > summary(prostate.hex$GLEASON)
   GLEASON        
 Min.   :0.000  
 1st Qu.:6.000  
 Median :6.000  
 Mean   :6.384  
 3rd Qu.:7.000  
 Max.   :9.000  
   > summary(prostate.hex[,4:6])
   RACE            DPROS           DCAPS          
 Min.   :0.000   Min.   :1.000   Min.   :1.000  
 1st Qu.:1.000   1st Qu.:1.000   1st Qu.:1.000  
 Median :1.000   Median :2.000   Median :1.000  
 Mean   :1.087   Mean   :2.271   Mean   :1.108  
 3rd Qu.:1.000   3rd Qu.:3.000   3rd Qu.:1.000  
 Max.   :2.000   Max.   :4.000   Max.   :2.000  


**H2O Table**

  Summarize information in data. Because H2O handles such large data sets, 
  it is possible to generate tables that are larger than R's
  capacity. To minimize this risk and enable uninterrupted work,
  **h2o.table** is called inside of a call for **head()** or **tail()**. Within
  **head()** and **tail()**, specify the number of rows in
  the table to return. 

::

  > head(h2o.table(prostate.hex[,3]))
 	   row.names Count
	1        43     1
	2        47     1
	3        50     2
	4        51     3
	5        52     2
	6        53     4

  > head(h2o.table(prostate.hex[,c(3,4)]))
 	  row.names X0 X1 X2
	1        43  1  0  0
	2        47  0  1  0
	3        50  0  2  0
	4        51  0  3  0
	5        52  0  2  0
	6        53  0  3  1


**Generate Random Uniformly Distributed Numbers**

  **h2o.runif()** appends a column of random numbers to an H2O data
  frame and facilitates creating testing/training data splits for
  analysis and validation in H2O. 

::

  > prosPath = system.file("extdata", "prostate.csv", package="h2o")
  > prostate.hex = h2o.importFile(localH2O, path = prosPath, key = "prostate.hex")
       |===================================================| 100%
  > s = h2o.runif(prostate.hex)
  > summary(s)
  rnd               
	Min.   :0.001434  
    1st Qu.:0.241275  
    Median :0.496995  
    Mean   :0.489468  
    3rd Qu.:0.740592  
    Max.   :0.994894  

  > prostate.train = prostate.hex[s <= 0.8,]
  > prostate.train = h2o.assign(prostate.train, "prostate.train")
  > prostate.test = prostate.hex[s > 0.8,]
  > prostate.test = h2o.assign(prostate.test, "prostate.test")
  > nrow(prostate.train) + nrow(prostate.test)
  [1] 380


**Split Frame**

  Generate two subsets from an existing H2O data set, according to user-specified ratios that can be used as testing/training sets.
  This is the preferred method of splitting a data frame because it's faster and more stable than running **runif** across entire the data set. However, **runif**
  can be used for customized frame splitting.

::

  > prosPath = system.file("extdata", "prostate.csv", package="h2o")
  > prostate.hex = h2o.importFile(localH2O, path = prosPath, key = "prostate.hex")
         |===================================================| 100%
  > prostate.split = h2o.splitFrame(data = prostate.hex , ratios = 0.75)
  > prostate.train = prostate.split[1]
  > prostate.test = prostate.split[2]
  > summary(prostate.train)
     Length Class         Mode
	[1,] 9      H2OParsedData S4  
  > summary(prostate.test)
    Length Class         Mode
	[1,] 9      H2OParsedData S4  

""""

Running Models
""""""""""""""

**GBM**

  Generate Gradient Boosted Models (GBM), which are used to develop forward-learning ensembles. For information on the GBM algorithm, see :ref:`GBMmath`.

::

  > ausPath = system.file("extdata", "australia.csv", package="h2o")
  > australia.hex = h2o.importFile(localH2O, path = ausPath)
     |===================================================| 100%
  > independent <- c("premax", "salmax","minairtemp", "maxairtemp",
  "maxsst", "maxsoilmoist", "Max_czcs")
  > dependent <- "runoffnew"
  > h2o.gbm(y = dependent, x = independent, data = australia.hex,
  > n.trees = 10, interaction.depth = 3, 
     n.minobsinnode = 2, shrinkage = 0.2, distribution= "gaussian")
       |======================================================| 100%
	IP Address: 127.0.0.1 
	Port      : 54321 
	Parsed Data Key: australia1.hex 

	GBM Model Key: GBM_a3ae2edf5dfadbd9ba5dc2e9560c405d 

	Mean-squared Error by tree:
	 [1] 230760.11 166957.80 124904.30  94031.17  72367.01  57180.17  47092.85
 	[8]  39168.05  34456.00  31095.86  28397.10



*Run multinomial classification GBM on abalone data*

To generate a classification model that uses labels, use a **multinomial** distribution. 

::

  > h2o.gbm(y = dependent, x = independent, data = australia.hex, n.trees
  = 15, interaction.depth = 5,
   n.minobsinnode = 2, shrinkage = 0.01, distribution= "multinomial")
	IP Address: 127.0.0.1 
	Port      : 54321 
	Parsed Data Key: australia1.hex 

	GBM Model Key: GBM_8e4591a9b413407b983d73fbd9eb44cf 

	Confusion matrix:
	Reported on australia1.hex 
        Predicted
	Actual     0 3 6 7 14 16 17 19 20 25 38 43 61 75 82 107 138 150 167 191 200
	  0      115 0 0 0  0  0  0  0  0  0  0  0  0  0  0   0   0   0   0   0   0
	  3        0 1 0 0  0  0  0  0  0  0  0  0  0  0  0   0   0   0   0   0   0
	  6        0 0 1 0  0  0  0  0  0  0  0  0  0  0  0   0   0   0   0   0   0
	  7        0 0 0 2  0  0  0  0  0  0  0  0  0  0  0   0   0   0   0   0   0
	....
	 Totals 120 1 1 2  1  2  2  2  2 31  1  1  1  6  1   1   1   6   1   1   1
        Predicted
	Actual   210 245 300 343 396 400 462 480 514 533 545 600 750 764 840 933 960
 	 0        0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0
	 3        0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0
  	 6        0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0
  	 7        0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0
  	 14       0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0
  	 16       0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0
	....
	 Totals   1   1  20   1   1   1   1   1   1   1   1   8   1   1   1   1   1
        Predicted
	Actual   1154 1200 2000 2400 Error
	  0         0    0    0    0 0.000
 	  3         0    0    0    0 0.000
  	  6         0    0    0    0 0.000
 	  7         0    0    0    0 0.000
	....
	Mean-squared Error by tree:
	 [1] 0.9529478 0.9337646 0.9157476 0.8985756 0.8818316 0.8654845 0.8497011
 	[8] 0.8341974 0.8187867 0.8036760 0.7887764 0.7741757 0.7594546 0.7452223
	[15] 0.7309634 0.7168317


**GLM**

  Generate Generalized Linear Models, which are used to develop linear models
  for exponential distributions. Regularization can be applied. For
  information on the GLM algorithm, see :ref:`GLMmath`.


::

  > prostate.hex = h2o.importFile(localH2O, path =
  "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", 
  key = "prostate.hex")
         |===================================================| 100%
  > h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data =
  prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
   |=====================================================================| 100%
	IP Address: 127.0.0.1 
	Port      : 54321 
	Parsed Data Key: prostate.hex 

	GLM2 Model Key: GLMModel__a2fdb4e3fdd92e0325141cdbd1bd43e1

	Coefficients:
      AGE      RACE     DCAPS       PSA Intercept 
	 -0.01104  -0.63136   1.31888   0.04713  -1.10896 

	Normalized Coefficients:
      AGE      RACE     DCAPS       PSA Intercept 
	 -0.07208  -0.19495   0.40972   0.94253  -0.33707 

	Degrees of Freedom: 379 Total (i.e. Null);  375 Residual
	Null Deviance:     514.9
	Residual Deviance: 461.3  AIC: 471.3
	Deviance Explained: 0.10404 
	AUC: 0.68875  Best Threshold: 0.328

	Confusion Matrix:
        Predicted
	Actual   false true Error
 	false    127  100 0.441
  	true      51  102 0.333
  	Totals   178  202 0.397

	Cross-Validation Models:
         Nonzeros       AUC Deviance Explained
	Model 1         4 0.6532738        0.048419803
	Model 2         4 0.6316527       -0.006414532
	Model 3         4 0.7100840        0.087779178
	Model 4         4 0.8268698        0.243020554
	Model 5         4 0.6354167        0.153190735
	Model 6         4 0.6888889        0.041892118
	Model 7         4 0.7366071        0.164717509
	Model 8         4 0.6711310        0.004897310
	Model 9         4 0.7803571        0.200384622
	Model 10        4 0.7435897        0.114548543



::

  > myX = setdiff(colnames(prostate.hex), c("ID", "DPROS", "DCAPS", "VOL"))
  > h2o.glm(y = "VOL", x = myX, data = prostate.hex, family = "gaussian", nfolds = 5, 
  alpha = 0.1)
	 |=========================================================| 100%

	IP Address: 127.0.0.1 
	Port      : 54321 
	Parsed Data Key: prostate.hex 

	GLM2 Model Key: GLMModel__b8339af00fbe8951ba0871611c9e42eb

	Coefficients:
  	CAPSULE       AGE      RACE       PSA   GLEASON Intercept 
 	-4.29014   0.29787   4.35557   0.04946  -0.51274  -4.35359 

	Normalized Coefficients:
  	CAPSULE       AGE      RACE       PSA   GLEASON Intercept 
 	-2.10678   1.94424   1.34488   0.98908  -0.55989  15.81292 

	Degrees of Freedom: 379 Total (i.e. Null);  374 Residual
	Null Deviance:     126623.9
	Residual Deviance: 127402  AIC: 11059.1
	Deviance Explained: -0.00615 

	Cross-Validation Models:
        Nonzeros      AIC Deviance Explained
	Model 1        5 685.6101        -0.02827868
	Model 2        5 660.3719        -0.15397511
	Model 3        5 658.0768         0.05826293
	Model 4        5 665.8665         0.05117173
	Model 5        5 683.6276         0.01333543


**K-Means**

  Generate a K-means model, which is a clustering algorithm that allows users to characterize
  data. This algorithm does not rely on a dependent variable. For
  information on the K-Means algorithm, see :ref:`KMmath`

::

  > prosPath = system.file("extdata", "prostate.csv", package="h2o")
  > prostate.hex = h2o.importFile(localH2O, path = prosPath)
  |=========================================================| 100%
  > prostate.km = h2o.kmeans(data = prostate.hex, centers = 10, 
  cols = c("AGE", "RACE", "VOL", "GLEASON"))
    |=========================================================| 100%
  print(prostate.km)
  IP Address: 127.0.0.1 
  Port      : 54321 
  Parsed Data Key: prostate6.hex 

  K-Means Model Key: KMeans2_99fea55be4a22f741df74532d7844bb4

  K-means clustering with 10 clusters of sizes 41, 27, 59, 17, 21, 47, 26, 61, 47, 34

	Cluster means:
        AGE     RACE         VOL  GLEASON
	1  69.73171 1.024390 37.99756098 6.512195
	2  54.48148 1.111111  0.32222222 6.518519
	3  62.59322 1.067797  0.19322034 5.966102
	.....



**Principal Components Analysis**

  Map a set of variables onto a
  subspace using linear transformations. Principle Components Analysis (PCA) is the first step in
  Principal Components Regression. For more information on PCA, 
  see :ref:`PCAmath`.

::

  > ausPath = system.file("extdata", "australia.csv", package="h2o")
  > australia.hex = h2o.importFile(localH2O, path = ausPath)
    |=========================================================| 100%
  > australia.pca = h2o.prcomp(data = australia.hex, standardize = TRUE)
    |=========================================================| 100%
  > print(australia.pca)
  IP Address: 127.0.0.1 
   Port      : 54321 
   Parsed Data Key: australia2.hex 

   PCA Model Key: PCA_90d7162c6d4855392ba1272c2f314bec

   Standard deviations:
   1.750703 1.512142 1.031181 0.8283127 0.6083786 0.5481364 0.4181621 0.2314953
	....

  summary(australia.pca)
  Importance of components:
  ....

**Principal Components Regression**

  Map a set of variables to a
  new set of linearly independent variables. The new set of variables
  are linearly independent linear combinations of the original
  variables and exist in a subspace of lower dimension. This
  transformation is then prepended to a regression model, often
  improving results. For more information on PCA, see :ref:`PCAmath`.

::

  > prostate.hex = h2o.importFile(localH2O, path =
    "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv",
  key = "prostate.hex")
      |=========================================================| 100%
  > h2o.pcr(x = c("AGE","RACE","PSA","DCAPS"), y = "CAPSULE", data =
  prostate.hex, family = "binomial", 
  nfolds = 10, alpha = 0.5, ncomp = 3)
   |==========================================================| 100%

	IP Address: 127.0.0.1 
	Port      : 54321 
	Parsed Data Key: PCAPredict_80069467adfe441c92282ac766f9de7e 

	GLM2 Model Key: GLMModel__a1454a5b8a212d1069376356543a4887

	Coefficients:
      PC0       PC1       PC2 Intercept 
  	3.76219   1.26824  -1.35455  -0.36271 
  	....

""""
  
Obtaining Predictions
"""""""""""""""""""""

**Predict**

  Apply an H2O model to a holdout set to obtain predictions
  based on model results. 
  In the examples below, models are generated first, and then the
  predictions for that model are displayed. 

::

  > prostate.hex = h2o.importFile(localH2O, path =
    "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", 
    key = "prostate.hex")
    |==========================================================| 100%
  > prostate.glm = h2o.glm(y = "CAPSULE", x =
  c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, 
  family = "binomial", nfolds = 10, alpha = 0.5)
      |==========================================================| 100%

  > prostate.fit = h2o.predict(object = prostate.glm, newdata = prostate.hex)
  > (prostate.fit)
  IP Address: 127.0.0.1 
	Port      : 54321 
	Parsed Data Key: GLM2Predict_8b6890653fa743be9eb3ab1668c5a6e9 

  predict        X0        X1
	1       0 0.7452267 0.2547732
	2       1 0.3969807 0.6030193
	3       1 0.4120950 0.5879050
	4       1 0.3726134 0.6273866
	5       1 0.6465137 0.3534863
	6       1 0.4331880 0.5668120


""""

Other Useful Functions
""""""""""""""""""""""

**Get Frame**

  For users that alternate between using the web interface and the R API, or for multiple users accessing the same H2O,
  this function gives the user the option to create a reference object for a data frame sitting in H2O  (assuming there's a
  **prostate.hex** in the KV store).

::

  > prostate.hex = h2o.getFrame(h2o = localH2O, key = "prostate.hex")
  


**Get Model**

  For users that alternate between using the web interface and the R API, this function gives the user the option to create a reference object
  for a data frame sitting in H2O (assuming there's a **GLMModel__ba724fe4f6d6d5b8b6370f776df94e47** model in the KV store).

::

  > glm.model = h2o.getModel(h2o = localH2O, key = "GLMModel__ba724fe4f6d6d5b8b6370f776df94e47")
  > glm.model


**List all H2O Objects**

  Generate a list of all H2O objects generated
  during a work session, along with each object's byte size. 

::

  > prostate.hex = h2o.importFile(localH2O, path = prosPath, key = "prostate.hex")
      |==========================================================| 100%
  > prostate.split = h2o.splitFrame(prostate.hex , ratio = 0.8)
  > prostate.train = prostate.split[[1]]
  > prostate.train = h2o.assign(prostate.train, "prostate.train")
  > h2o.ls(localH2O)
                                                     Key Bytesize
	1               GBM_8e4591a9b413407b983d73fbd9eb44cf    40617
	2               GBM_a3ae2edf5dfadbd9ba5dc2e9560c405d     1516


**Remove an H2O object from the server where H2O is running**
  
  To remove an H2O object on the server 
  associated with an object in the R environment, we recommend also removing the object from the R environment.

::

  > h2o.ls(localH2O)
                 Key Bytesize
 1      Last.value.39      448
 2      Last.value.42       73
 3       prostate.hex     4874
 4     prostate.train     4028
 5 prostate_part0.hex     4028
 6 prostate_part1.hex     1432
 
 > h2o.rm(object= localH2O, keys= "prostate.train")
 > h2o.ls(localH2O)
                 Key Bytesize
 1      Last.value.39      448
 2      Last.value.42       73
 3       prostate.hex     4874
 4 prostate_part0.hex     4028
 5 prostate_part1.hex     1432

""""  

