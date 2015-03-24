# Nunes Release (2.8.6.2) - 3/16/15

(*includes changes from Novikov release [2.8.5.3] - 3/7/15)

####New Features
These changes represent features that have been added since the previous release: 

- Create mechanism to move VMs between Hypervisor [(HEX-1965)](https://0xdata.atlassian.net/browse/HEX-1965)


####Enhancements

These changes are improvements to existing features (which includes changed default values):

#####Algorithms 
- Enforce `min_rows` input parameter for leaf nodes in tree builder [(github)](https://github.com/h2oai/h2o/commit/ff6b7f024e642acf05a8e34d0bb8fcd900644a4c)
- Enable variable importances for autoencoder [(github)](https://github.com/h2oai/h2o/commit/4f392e8afcd61083c3e553a044480ff5853e5ad6)
- GLM coefficient constraints [(HEX-1961)](https://0xdata.atlassian.net/browse/HEX-1961)
- added grid search across all relevant parameters that might be affected by beta constraints [(github)](https://github.com/h2oai/h2o/commit/c481f40dd422d7fdca3959b355af789168d93b0f)
- added stricter threshold in checkmodel, and added higher accuracy by default [(github)](https://github.com/h2oai/h2o/commit/f17c7ee41f239596e2f67fd7e3d5d268bff6f397) 
- Added proximal penalty to objective value used in line search [(github)](https://github.com/h2oai/h2o/commit/d8605e76f4566dc5fbe33bd9c61c37efbbe76904)
- try different setting for importance [(github)](https://github.com/h2oai/h2o/commit/cfbcf3479fca79111f5c4a2ef86b421ba8e01f79)
- add some tolerance [(github)](https://github.com/h2oai/h2o/commit/c3f6ce942010b7f4613aa32340edae2a5e42b86c)
- Write trees to individual files for easier debugging [(github)](https://github.com/h2oai/h2o/commit/9ba0e12332115857d296a9b3042a03d9bf6f303a)

#####API
- Add h2o.setMaxLastValue() [(github)](https://github.com/h2oai/h2o/commit/c4f64c4682f02293a0d7ecca46e8c49619cef9d0)
- adding as.numeric mehtod to R package [(github)](https://github.com/h2oai/h2o/commit/67f4295a86e4fd4e656a85c83001566f34b2cae3)

#####System
- add UUID field to generated POJOs in h2o1 [(HEX-2014)](https://0xdata.atlassian.net/browse/HEX-2014) [(github)](https://github.com/h2oai/h2o/commit/903f229eae8df9f7781edbada530fbc5d28c2e54)
- allow column type setting [(HEX-1291)](https://0xdata.atlassian.net/browse/HEX-1291)
- Need a way to set a specific level on a factor col [(PUB-1153)](https://0xdata.atlassian.net/browse/PUB-1153)[(github)](https://github.com/h2oai/h2o/commit/69515ebe5915f6266a840ca0914d36bfd9bfed98)
- Bind both TCP and UDP ports before clustering [(github)](https://github.com/h2oai/h2o/commit/b9fd80f339cf7727142d6a3a71a9651f2ea1845f)
- Removes all GA messages [(github)](https://github.com/h2oai/h2o/commit/7e6c7c98693b55b3fa1999bf59e1725a963fc686)


####Bug Fixes 
These changes are to resolve incorrect software behavior: 

#####Algorithms
- GLM : the proximal penalty was not being added to the objective value used during line-search, which would cause line search to stop the solver prematurely. [(HEX-2028)](https://0xdata.atlassian.net/browse/HEX-2028)
- GLM : With some Bayesian priors, the coeff estimates are off from our expectation: [(HEX-2026)](https://0xdata.atlassian.net/browse/HEX-2026)
- GLM : Modeling with constraints delivers unexpected results [(HEX-2025)](https://0xdata.atlassian.net/browse/HEX-2025)
- GLM in R: h2o.glm with Beta Constraints overwrites input training frame [(HEX-2021)](https://0xdata.atlassian.net/browse/HEX-2021)
- GLM: linear regression fails to run with beta constraints [(HEX-2020)](https://0xdata.atlassian.net/browse/HEX-2020) [(github)](https://github.com/h2oai/h2o/commit/a4fedfd0d61756b051afd3c5261322ff50ff0952)
- java.lang.ArrayIndexOutOfBoundsException while running GBM [(HEX-2011)](https://0xdata.atlassian.net/browse/HEX-2011)
- i pass "do_classification: True" parameter to ModelBuilders/gbm. no error. but it's not echoed and the result is regression [(PUB-1134)](https://0xdata.atlassian.net/browse/PUB-1134)
- Fixed proximal interface in glm [(github)](https://github.com/h2oai/h2o/commit/3562bd7091b457e94ee8e975034eae8ed92b495d)
- GLM bug fix [(github)](https://github.com/h2oai/h2o/commit/b28566e8d2d415e8c3cc57d3d4a78adb53c319a8)
- Bug fix in glm to protect against (throw IAE) user supplying beta_given but no vector of penalties in the beta_constraints [(github)](https://github.com/h2oai/h2o/commit/8e0e38300d5bdd9b293f3872b799502619d216aa)
- fixed LR beta constraints, took out beta given since no rho is given [(github)](https://github.com/h2oai/h2o/commit/7fab0264492b383de5f36008f608f6f7ad8ddbb9)
- Fix in GLM [(github)](https://github.com/h2oai/h2o/commit/5617c6e95885dd3dd2d30bb3bf7cfd6d2e0aec8d)



#####API 
- as.h2o() broken, cannot import date column into h2o [(HEX-2027)](https://0xdata.atlassian.net/browse/HEX-2027) [(github)](https://github.com/h2oai/h2o/commit/c27f9a3c152f3c17c4fbfb6e7fb16534779488d5)
- R: as.numeric missing from R package [(HEX-2024)](https://0xdata.atlassian.net/browse/HEX-2024)
- Fix bug in as.h2o. h2o.exec call (for turning cols into enums) was somehow overwriting h2odataset object in outer scope. [(github)](https://github.com/h2oai/h2o/commit/b63c42b6838b630a5e9ad042737c4d00b61597b0)


#####System

- handle GA not able to call home gracefully [(HEX-2030)](https://0xdata.atlassian.net/browse/HEX-2030)
- Rapids apply: java.lang.AssertionError at water.fvec.Frame.<init>(Frame.java:100) at water.fvec.Frame.<init>(Frame.java:72) at water.rapids.ASTApply.apply(ASTApply.java:63) at water.rapids.AST.treeWalk(AST.java:50) [(PUB-1145)](https://0xdata.atlassian.net/browse/PUB-1145)
- NPE in fvec parse on 164 on multiple gz files [(PUB-441)](https://0xdata.atlassian.net/browse/PUB-441)
- 



#Noether Release (2.8.4.1) - 1/30/15

####Enhancements 

#####UI

- Java POJO scoring implemented for unsupervised models (especially DL Autoencoder) [(PUB-1118)](https://0xdata.atlassian.net/browse/PUB-1118)
- Added perfbar to Cluster Status [(PUB-296)](https://0xdata.atlassian.net/browse/PUB-296)
- Raised default stack trace depth for Profiler to 10 [(github)](https://github.com/h2oai/h2o/commit/08604c6d9663cbf0c9d955fff137faf68048eb7d)
- Reduced linewidth to <90 chars. [(github)](https://github.com/h2oai/h2o/commit/b91aad3bcb4db4428d5e5600a3d17040cf7167fcO)
- Added warning and listed remedies if the input layer is bigger than 100k neurons [(github)](https://github.com/h2oai/h2o/commit/ae78448b9dabb4dd10ad099cdae7abd5d73c65dd)
- Added license info [(github)](https://github.com/h2oai/h2o/commit/8f51b83f671fa63ef4e2118836a8f20648c0b282)

#####API
- Added extra options to `CreateFrame` to make (potentially sparse) binary columns [(github)](https://github.com/h2oai/h2o/commit/7e0d6509fde1733fcfe17f92f5c65abc8597fa8a)
- Ported changes to `createFrame` to R [(github)](https://github.com/h2oai/h2o/commit/f5b2a4d60076dff61414e8dbc246780ade1132ad)
- Added chunk summary after `CreateFrame` [(github)](https://github.com/h2oai/h2o/commit/6e83e39ca6797e03db97ce925afe6ad71bcba56f)
- Added `makeModel` call to R/H2O to allow for hand-made glm models with efficient scoring [(github)](https://github.com/h2oai/h2o/commit/4603531579f9e890df1d0fdaf5b57de3719c5e8e)
- Raised default value of `-data_max_factor_levels` from 65k to 1M [(github)](https://github.com/h2oai/h2o/commit/452392e17a08489103b2063955761702e637d905)
- Updated `h2o.createFrame` with optional response column [(github)](https://github.com/h2oai/h2o/commit/372bb4a94a1b404328c4c50ad780a480bf49efa8)
- Added `many_cols` and `chunk_bytes` to h2o.init [(github)](https://github.com/h2oai/h2o/commit/5edb09f8e74544847f063f17a2ec654803aabada)
- Added `data_max_factor_levels` to h2o.init() in R [(github)](https://github.com/h2oai/h2o/commit/f7a7defc3411d5e16cd8d9ab6690119c5b94a363)
- Added `-many_cols` (same as `-chunk_bits` 24), renamed `-chunk_bits` to `-chunk_bytes` [(github)](https://github.com/h2oai/h2o/commit/524bef3102e43a2d09ec337f2c6ebf5e75ebab38)
- Added glm `disable_line_search` flag to R [(github)](https://github.com/h2oai/h2o/commit/e1211d487f15885f76c1bf9a56c1f37180f8a0e7)
- Added (hidden) explicit `disable_line_search` flag to allow user to disable line search [(github)](https://github.com/h2oai/h2o/commit/7b346d76f2b474656d2c72bf7c37a8d778acbdac)
- Added `-data_max_factor_levels` and `-chunk_bits` to Hadoop command line [(github)](https://github.com/h2oai/h2o/commit/e0cb1a8fdfd251d8c27a14dfc295ac24af1b7098)
- Made chunk size an argument. Default is `-chunk_bits` 24 -> 16MB [(github)](https://github.com/h2oai/h2o/commit/014dda94e2e055dff42e99f9e03897da08d51080)
- Updated parameters for py API for createFrame [(github)](https://github.com/h2oai/h2o/commit/53ab3196983ffae0df0f73679c0bdbe53052c357)
- Updated create frame arguments [(github)](https://github.com/h2oai/h2o/commit/ddf0da8abddeba5a1da5d71a1490d0deb465f8b8)
- Added `runifSplit` to Frame [(github)](https://github.com/h2oai/h2o/commit/8a3b7d5cfa02ed32ea454293b79677bb66cda8fa)
- Added support for `holdout_fraction` to SRF/DRF/GBM/DL in R [(github)](https://github.com/h2oai/h2o/commit/47a491070bd111c1904eca5a0f3dbf00b8f6e890)
- Backported rbind v2 from h2o-dev [(github)](https://github.com/h2oai/h2o/commit/f0ff6f027e4aa9e3faebcd5f192bd8781d47c5f1)
- Added `frozenType()` override to speed up histogram deserialization [(github)](https://github.com/h2oai/h2o/commit/b19d95774d008990441d27bcbc88df4be4eb0e79)

#####Algorithms

- Automatically call SplitFrame from the ModelBuilder page [(PUB-1103)](https://0xdata.atlassian.net/browse/PUB-1103)
- PUB-1103: Added option to automatically call SplitFrame from model builder age if holdout_fraction is > 0 [(github)](https://github.com/h2oai/h2o/commit/e054707a2c627944d8441a6f5562bf2ec216a459)
- Increased epochs to 30 and increased tolerance to .4 [(github)](https://github.com/h2oai/h2o/commit/8ceb7acf88fbe059f5f6034b297aeb74de9f6454)
- Enabled variable importance by default [(github)](https://github.com/h2oai/h2o/commit/de5457971a956318c52e6c15b06b95932e3d4d30)
- Changed GLM printout to avoid errors with modified glm models [(github)](https://github.com/h2oai/h2o/commit/13d4c73887976e4ed2fdc25e249a86e4e9c63f11)
- Added survfit class designation to h2o.coxph survfit output [(github)](https://github.com/h2oai/h2o/commit/b504526916179aa4265f87767a548d7978ea7ccb)
- Changed predict method for CoxPH to return the risk as opposed to the cumulative hazard and associated standard error [(github)](https://github.com/h2oai/h2o/commit/fa8af63a02b5ba9a0bbe1df5f1e1e1cea955fcd2)
- Added N-fold CV, use GBM for MNIST [(github)](https://github.com/h2oai/h2o/commit/60a1957c5f4a0734a1d179b93c77bf5de323fde0)
- Added grid parallelism parameter to h2o.gbm [(github)](https://github.com/h2oai/h2o/commit/6e9a3577351e606a6da5543580ddca7ff864622c)
- Updated HDFS deletion command, and run GBM before DRF [(github)](https://github.com/h2oai/h2o/commit/28c499084019bc7566826a2b933c3720091b12df)
- Added script for running GLM/RF/GBM on 15M rows, 2.2k cols [(github)](https://github.com/h2oai/h2o/commit/b01ea0d5b61d7687b0eae12f15fed3b54689a2b1)
- Updated R docs for SpeeDRF [(github)](https://github.com/h2oai/h2o/commit/e79c9589efad3d930dc05a69a34aeb4dc4e16db8)
- Added `class_sampling_factors` to GBM/DRF [(github)](https://github.com/h2oai/h2o/commit/cdd4cad0c748d19874c057f4a9d9aad3bc167588)

#####System 

- Added -extraJavaOpts option to the Hadoop H2O Driver [(github)](https://github.com/h2oai/h2o/commit/77bf6728a791fe4a7ad517ef1a88df9c0a755759)
- Background the deletion of ICE directory at startup [(github)](https://github.com/h2oai/h2o/commit/814c82217ff3c02cf72523fe7756e7fe1d163cd7)
- Added Hadoop options (as baseline) [(github)](https://github.com/h2oai/h2o/commit/e8c97e32f7ca1f5e3f6b1c295d1c77275db84e22)
- Added Hadoop cut and paste example for each distribution [(github)](https://github.com/h2oai/h2o/commit/8e224483df4db29e342abf35fe6f008d0affa2e4)
- Added R script for Next.ML [(github)](https://github.com/h2oai/h2o/commit/69c1d614d3a37e4428b516f2121d19caa3ded922)
- Use output of `boot2docker ip` instead of localhost [(github)](https://github.com/h2oai/h2o/commit/9687cd0b443e4ac23a772535bed2bd16e9bf25b7)


####Bug Fixes


#####UI
- Fixed a bug in an example due to createFrame change [(github)](https://github.com/h2oai/h2o/commit/afc93a9734ea22efa9afcec46bbc7e26b2faee98)
- Import file hangs at 99% for past 45 mins [(HEX-2003)](https://0xdata.atlassian.net/browse/HEX-2003)

#####API
- as.h2o returns NA when character field has many unique values [(PUB-1108)](https://0xdata.atlassian.net/browse/PUB-1108)
- Fixed R for createFrame updates [(github)](https://github.com/h2oai/h2o/commit/f52f6a4679a2f5cacc0825c271f3c899cca43c03)

#####Algorithms
- Removed restrictions on glm upper/lower bounds to be non-negative/non-positive. The only restriction is lb <= ub [(github)](https://github.com/h2oai/h2o/commit/5980d62c436922e6c3c73cbfa0b83c1bcebff7d8)
- Fixed PUB-1118 - AutoEncoder POJO scoring. Also fixed a bug in initializing the activation layer for categoricals. [(github)](https://github.com/h2oai/h2o/commit/6505a1ffc0a99c211a6e14807ff280058ab9c0e7)
- Variable Importance for DRF slow for large numbers of columns [(PUB-1101)](https://0xdata.atlassian.net/browse/PUB-1101)
- Fix PUB-1101 by using GBM's variable importance method for DRF [(github)](https://github.com/h2oai/h2o/commit/0026200d4ba1e76f759cb4df5743555855b92c42)
- Bugfix for case where N is not divisible by num CV folds [(github)](https://github.com/h2oai/h2o/commit/34e271760b70fe6f384e106d84f18c7f0adb8210)
- Fixed typo in parameter validation [(github)](https://github.com/h2oai/h2o/commit/f6c6996594894c5ecdcd2ad112953b92844a3848)
- Don't shuffle intra-chunk after stratified sampling [(github)](https://github.com/h2oai/h2o/commit/f0ff6f027e4aa9e3faebcd5f192bd8781d47c5f1)
- Fixed intercept adjustment by prior probability in logistic regression [(github)](https://github.com/h2oai/h2o/commit/cdd8a6b54a35e83a5f9aebb419ada476593ab144)
- DL: DL build model running for past 12 hrs doesn't complete [(HEX-1997)](https://0xdata.atlassian.net/browse/HEX-1997)
- GLM fails simpleCheckGLM in multinode when cross-validation is on [(HEX-1867)](https://0xdata.atlassian.net/browse/HEX-1867)

#####System

- Parse creates too many keys for large numbers of columns [(PUB-1102)](https://0xdata.atlassian.net/browse/PUB-1102)
- Don't allow pure number enums [(github)](https://github.com/h2oai/h2o/commit/fb95e198db3a6186ef5ed408bd31f29dadae60a5)
- Fixed argument order [(github)](https://github.com/h2oai/h2o/commit/2a174063ca5ffdc4bd117a7206e45d89fa56317b)
- Excluded factors that contain lower-case UUIDs that look like they are in scientific notation with exponent: `^\d+e\d+$`[(github)](https://github.com/h2oai/h2o/commit/7fc6f1c23f254156a60ce343df4ca6b5b73f3156)
- Prevented deadlock during startup by adding UniqueFrameId to TypeMap [(github)](https://github.com/h2oai/h2o/commit/7e6fb375cd1c45c5a771b79add73f697409a312a)
- Fixed sparse chunk to avoid binary search for prefix/suffix zeros [(github)](https://github.com/h2oai/h2o/commit/48d4dfa9ad12461970c7d3c2f75907a27cb7d6f0)



Release 2.6.1.1 (Lambert build 1)
==================================

* [Download this release](http://s3.amazonaws.com/h2o-release/h2o/rel-lambert/1/index.html)
* [Query JIRAs for this release](https://0xdata.atlassian.net/issues/?jql=fixVersion%20%3D%20lambert%20and%20resolution%20is%20not%20empty)



Technical task
--------------
    * [PUB-666] - Make sure that MVN/Gradle is using correct Javassist version witch works with Java8
    * [PUB-806] - Create anomaly detection app

Bug
---
    * [PUB-219] - add hdp2.1 for -hdfs_version support (originally was about cdh5 support)
    * [PUB-302] - k-means broken on categoricals
    * [PUB-342] - svm light broken in import2/parse2
    * [PUB-430] - sum(X, na.rm=T) doesn't work inside h2o
    * [PUB-450] - ddply on all_yr_airlines dataset gives, Array index out of bound exception
    * [PUB-474] - exec2 functions shouldn't return 0 when they return nothing
    * [PUB-482] - ava.lang.ArrayIndexOutOfBoundsException: 3 	at hex.ConfusionMatrix.add(ConfusionMatrix.java:72) maybe all NaNs in dataset?
    * [PUB-492] - Heartbeat assertion error on startup
    * [PUB-503] - GLM2: on smalldata/airlines nfold = 10 get java.lang.AssertionError at water.H2O.submitTask(H2O.java:668)
    * [PUB-521] - GLM2: seems like ROC curve is inverted on model page. A bad model is giving good predictions.
    * [PUB-549] - ddply on all yrs airlines data gives - java.lang.NullPointerException
    * [PUB-552] - exception in at water.TypeMap.newInstance(TypeMap.java:88) is showing up a lot in multi-machine fvec parses
    * [PUB-564] - (During parse) java.lang.NullPointerException: null while mapping key
    * [PUB-595] - ddply: two successive identical don't get same answer? (largish # of groups) (most do). Appears that sometimes I get nacnt!=0 in the result (58), when normally I don't.
    * [PUB-761] - fvec import/parse using s3 and zip or gz file has stack trace. oddly s3n seems okay (they do blocks differently. maybe a factor?)
    * [PUB-808] - R: h2o.glm does not have an option to use_all_factor_levels and on the help page, there is no explanation for return_all_lambda and max_predictors options
    * [PUB-877] - h2o.glm inconsistency in flags:The flags for var_importance and use_all_factor_level is set to 0,1 instead of a T or F.
    * [PUB-892] - h2o. perf: Reporting incorrect Accuracy and error
    * [PUB-894] - H20 not compatible with YARN(hadoop 2.4.1)
    * [PUB-898] - SRF: java.lang.AssertionError at water.AutoBuffer.put3(AutoBuffer.java:574) on airlines_all
    * [PUB-923] - kmeans sometimes gets NaN in cluster center (zero members) and doesn't recover. When I ask for 3 clusters, I should get 3 non-empty clusters
    * [PUB-930] - runit_DeepLearning_imbalance_large.R fails with worse results using balance_classes
    * [PUB-937] - h2o.glm:  when run with lambda search, shows  number of predictors = -1 in the summary table for the first model. Looks fine in the browser.
    * [PUB-939] - SVMLight parse problem
    * [PUB-940] - KMeans2 re-inits clusters forever on this trivial dataset
    * [PUB-943] - Binary Ops Online in EQ3
    * [PUB-953] - SpeedRF cancel takes way too long
    * [PUB-957] - SRF: When run on ecology dataset with validation set specified, gives Illegal argument: java.lang.IllegalArgumentException: Enum conversion only works on integer columns
    * [PUB-961] - Illegal argument: Duplicate name 'predict' in Frame
    * [PUB-964] - RuntimeException while running DL on a dataset..
    * [PUB-972] - H2O Steam: the show more tab for listing the params does not work on steam's scoring page
    * [PUB-975] - Warning about variable importance and factor levels occurs for default GLM parameters
    * [PUB-976] - water.fvec.NewChunk.append2slow ArrayIndexOutOfBoundsException
    * [PUB-977] - GBM predictions are unexpectedly centered at 0.
    * [PUB-980] - summary on key with no values (after filter by exec) gets NPE (probably the enum?)
    * [PUB-987] - Giving -ice_root of c:/ results in an exception instead of a message
    * [PUB-990] - AIOOBE during rebalance
    * [PUB-991] - GLM warns about variable importance too much
    * [PUB-993] - R Client throws on running algos: "Error: Internal Server Error"
    * [PUB-999] - Incomplete Deeplearning Grid Results in R



Improvement
-----------
    * [PUB-265] - Arno idea: to have a small set of performance benchmarks executed on the start of H2O
    * [PUB-294] - Should models have sticky error messages?  Errors during training, do they just output in jobs list or training completion. Or should all "bad' GLM models be deleted (bad due to error)
    * [PUB-624] - starting h2o from R, if some dirs used already exist with wrong permissions...nothing good happens. maybe could test/report
    * [PUB-681] - Add variance or standard deviation to inspect and summary pages
    * [PUB-837] - On-demand cluster performance check
    * [PUB-927] - Add a warning message to h2o.init() if your H2O is crippled with only one core
    * [PUB-997] - Document use of variableImportance from h2o.randomForest h2o.gbm ..
    * [PUB-998] - New user experience with default heap size and big datasets

New Feature
-----------
    * [PUB-531] - h2o exec incorrectly says there's a missing paren in the middle of this expression
    * [PUB-962] - NaiveBayes does not compute variable importances
    * [PUB-963] - SpeeDRF variable importances not appearing in Steam
    * [PUB-989] - Add automatic determination of train_samples_per_iteration

Story
-----
    * [PUB-727] - Transfer data from Spark to H2O
    * [PUB-730] - Transfer results from H2O to Spark

Task
----
    * [PUB-642] - Parse2 failed on parsing empty file
    * [PUB-942] - Set default logVerbose to false in DRemoteTask
    * [PUB-956] - Re-enable linpack and memory bandwidth tests

Technical task
--------------
    * [HEX-1781] - Create RUnit tests for imbalanced data handling.
    * [HEX-1861] - Add tests for mean imputation of missing values in Deep Learning
    * [HEX-1862] - Add InsertMissingValues app (Beta) to R

Bug
---
    * [HEX-642] - The job is shown as cancelled even if it crashed - we need additional Job state "INVALID"
    * [HEX-800] - return unable to solve as json/browser error/warning not stack trace
    * [HEX-1358] - Separator Tab was being picked instead of Space 
    * [HEX-1723] - Looping multifile parse results in YARN killing an H2O node (too much mem)
    * [HEX-1756] - GLM: cross validation is broken
    * [HEX-1759] - R: can't mix column indexing types
    * [HEX-1768] - exec2 needs sapply + lexically scoped functions
    * [HEX-1783] - R <simpleError: C stack usage is too close to the limit> when running GLM
    * [HEX-1799] - GLM nfolds not passing along model params
    * [HEX-1850] - NPE inside NonBlockingHashMap called by /2/Models in multi-jvm
    * [HEX-1852] - h2o.predict on Naive Bayes model fails with java.lang.RuntimeException: org.apache.commons.math3.exception.NotStrictlyPositiveException: standard deviation (0)
    * [HEX-1853] - model training time is off (either 0 or >>>> 0) for many model types
    * [HEX-1854] - The SpeeDRF -> Random Forest / DRF -> BigData RF name change isn't in Steam
    * [HEX-1855] - steam fails to show when an ill-formed model or unsupported model is built
    * [HEX-1870] - test_model_management.py: ERRR WATER: Missing vector #1 (fMonth) during Frame fetch: $04ff02000000ffffffff$_bcd826a77fc96e2fda1c0a6396164154
    * [HEX-1873] - Cross validated GBM save/restore needs a test



Improvement
-----------
    * [HEX-635] - interrogating the result from the json Jobs, it's not clear how you tell a job has completed?
    * [HEX-871] - Remove VA->FVec converter wiring that I wrote
    * [HEX-1676] - Need to be able to get a checksum of all the Vecs in a Frame to detect changes.
    * [HEX-1860] - Add automatic mean imputation for missing values in Deep Learning

New Feature
-----------
    * [HEX-1841] - Need a way to handle customer-pp's (and hopefully others') date values
    * [HEX-1858] - Add jira.0xdata.com to the bottom of h2o web ui

Story
-----
    * [HEX-1645] - Collect cpu, memory, network, disk in perf runs

Task
----
    * [HEX-1680] - Test GLM2 model persistence



Release 2.6.0.2 (Lagrange build 2)
==================================

* [Download this release](http://s3.amazonaws.com/h2o-release/h2o/rel-lagrange/2/index.html)
* [Query JIRAs for this release](https://0xdata.atlassian.net/issues/?jql=fixVersion%20%3D%20lagrange%20and%20resolution%20is%20not%20empty)

Technical task
--------------
    * [PUB-729] - Remove hard-coded path to demo files
    * [PUB-783] - Add support for categorical data for auto-encoder
    * [PUB-843] - SpeeDRF Tests on large datasets (singlenode)
    * [PUB-844] - SpeeDRF Tests on large datasets (multinode)
    * [PUB-846] - Enhance SpeeDRF Predictive Performance
    * [PUB-847] - SpeeDRF comparison with scikit-learn
    * [PUB-848] - SpeeDRF comparison with R's randomForest
    * [PUB-849] - Distributed System Test
    * [PUB-850] - SpeeDRF local mode 
    * [PUB-900] - Replace GLM Model Getter (glm, grid,  all lambdas, single lambda)
    * [PUB-911] - Rename SpeeDRF(beta)

Bug
---
    * [PUB-145] - parse turns factors/strings into zeros
    * [PUB-166] - Gamma vs. gamma in R
    * [PUB-168] - can't operate on factors from R
    * [PUB-171] - R: assigning colnames broken
    * [PUB-172] - R/h2o doesn't correctly handle logicals
    * [PUB-174] - R/cut is broken
    * [PUB-175] - R/table return value is in the wrong data type
    * [PUB-189] - There is no lock on the dataset while a grid job is in progress, so when i delete the dataset when the grid is in progress get -java.lang.IllegalArgumentException: Nothing to lock!
    * [PUB-212] - R: h2o key assigned to the wrong data
    * [PUB-213] - R: ifelse, >, < operators wrong: don't handle NA
    * [PUB-220] - header not automatically deduced for case with single quotes on header and data. single_quotes=0. header apparently not deduced
    * [PUB-222] - single_quotes=1 should strip single quotes from tokens before creating the value (it doesn't). both header and data
    * [PUB-281] - constant assign to a row is unsupported in Exec
    * [PUB-286] - does h2o exec get TRUE and FALSE passed thru from R?
    * [PUB-346] - h2o.R breaks R's internal list datastructure
    * [PUB-382] - exec: this exec seems to result in 'empty' which gives us bad inspect results, okay if tweaked so result is not empty..i.e. what's the exec result supposed to be, if num rows = 0 
    * [PUB-386] - K means: won't model on factors (good); won't take correctly specified numeric columns - claims numbers are factors (bad)
    * [PUB-387] - summary stops working in R after data munging, str(data) says df is OK
    * [PUB-388] - str returns non-specific VA/FV for data frames
    * [PUB-397] - GLM2 job getting cancelled. Polling says 'user cancelled'. Jobs list says CANCELLED. I didn't cancel it. Think it's getting cancelled during cross-validation
    * [PUB-404] - Exec2: seems like if result is 'empty' and the result type is data frame when not empty, that num_cols should be 0 (num_rows is 0). cols also should be empty?
    * [PUB-415] - [shalala] java.lang.IncompatibleClassChangeError: JLine incompatibility detected for sbt 0.12
    * [PUB-417] - GLM2: case_mode '=' case_val '-1' ...response C13.. ok if gaussian doesn't like it if binomial
    * [PUB-426] - GBM failing on AIOOBE for large number of trees
    * [PUB-436] - R: reloading datasets broken
    * [PUB-440] - AAIOBE during fvec parse on 164
    * [PUB-451] - Separate AUC values from AUC rest call. 
    * [PUB-455] - exec: testing is.na() and ifelse() on covtype..eventually get a AIOOBE
    * [PUB-456] - Exec assertion ...column vector == scalar being used in the select of ?
    * [PUB-466] - If play with the dataset a bit and try to overwrite or delete prediction key, action fails and prediction key remains locked.
    * [PUB-486] - nested ifelse gets AIOOBE r[,1]=ifelse(r[,1],r[,2], ifelse(r[,1],r[,2], r[,1]))
    * [PUB-487] - exec: binary op on frame or row, doesn't work; binary op on cols works. java.lang.AssertionError: 10 at water.fvec.Frame.add(Frame.java:120) 
    * [PUB-505] - big number incorrect in h2o. (R shown for comparison)
    * [PUB-506] - adding some (two to ten) numbers thru col sum gets different answer than adding them as scalars (in console). R gets exact same answer in both cases.
    * [PUB-507] - extending .000 on data to force a parse to reals, can cause wrong number into h2o (sign wrong, value wrong by 10x)
    * [PUB-508] - parser is off by one on the exponent for a parsed number (it's a big number, but no trailing zeros)
    * [PUB-513] - curl to get DRF2 model produces ArrayIndexOutOfBoundsException: null
    * [PUB-532] - exec: seems like it doesn't take a ^ -b  ..it doesn't like the - in the exponent (R says fine)
    * [PUB-534] - Exec doesn't support R modulo, integer division or the alternate '**' exponentiation operator
    * [PUB-536] - variable importance graph for one variable shows nothing
    * [PUB-547] - score validation appears twice in DL GUI
    * [PUB-553] - exec: java.util.IllegalFormatConversionException: e != java.lang.Long .......................is it thinking the - is part of the frame name (no spaces)
    * [PUB-554] - exec. NPE in this expression. not sure why (part of a longer test)
    * [PUB-555] - exec: AIOOBE. is ifelse() legal? I'm assuming isna() is
    * [PUB-556] - exec, a different AIOOBE ..maybe it's around the user of a ? b : c
    * [PUB-558] - GLM2 gets this AAIOBE in py/testdir_multi_jvm/test_GLM2_many_cols_int2cat.py
    * [PUB-562] - When click on Store view page from browser, after driving H2O from R for quite some time, get : error Not enough keys - request offset is 8 but K/V contains 0 keys.
    * [PUB-568] - We need to be able to specify the model key from the R calls 
    * [PUB-570] - GLM2 silent regularization
    * [PUB-575] - exec ternary op with assign (lhs) in the selected phrase, gets poppush assertion
    * [PUB-589] - doing a list of functions with ddply. stack trace on "is.factor"
    * [PUB-599] - seem to have issues more than 1 ternary expression in a exec str=
    * [PUB-600] - FrameSplitPage should cause error to browser/json, not assertion when 0 rows for the split (also 50% split of 18 rows shouldn't be a failure?)
    * [PUB-604] - DownloadDataset of non-existing hex gets NPE
    * [PUB-606] - exec getting AIOOBE in this expression for some reason
    * [PUB-607] - Not sure why this Exec is getting NPE. uses ternary op, maybe apply is not legal?
    * [PUB-621] - return value of H2O objective function in GUI and R
    * [PUB-633] - DRF fvec  Got exception 'class java.lang.AssertionError', with msg 'col_data Infinity out of range FirstName	:-Infinity-Infinity step=Infinity nbins=20 isInt=0
    * [PUB-634] - GLM2 apparently is reporting a best_threshold=0.175 when the values in threshold are 0.17 then 0.18, no 0.175
    * [PUB-636] - exec AAIOBE on a case with interesting column reference
    * [PUB-637] - exec: adding two 10x10 data frames doesn't work (single jvm)
    * [PUB-668] - R: ifelse() with apply() causes Error in rep(no, length.out = length(ans)) :    attempt to replicate an object of type 'S4'
    * [PUB-669] - Installation of H2O package in R on Windows failing on Statmod dependency
    * [PUB-671] - should throw error (descriptive) when you select a non-existent col
    * [PUB-682] - head() of empty data frame (in h2o) causes exec2 exception
    * [PUB-685] - H2O doesn't support c(1,2) for apply as R suggests, should return unsupported
    * [PUB-696] - cbind() of something that was created by rep_len() fails, but cbind() of something created with c() works?
    * [PUB-697] - bad key name in exec causes assertion (4.1$C1)
    * [PUB-698] - %*% ...exec illegal argumetn says Matrices do not match: [581012x1] * [581012x1]",  which doesn't make sense. Error is probably that single col is not supported?
    * [PUB-699] - row exclude (negatives) doesn't seem to work (exec), works for columns
    * [PUB-704] - 1B rep_len(), then runif() I think the runif is getting the stack trace here (exec)
    * [PUB-705] - null/empty frame in h2o can't pass to R 
    * [PUB-706] - push csv from R to h2o, strings are not double-quoted..so comma in string doesn't work
    * [PUB-712] - the small files that are the backing for chunks in the ice dir, show up in the storeview (view all)
    * [PUB-719] - SHA256 slows down vec rollups too much
    * [PUB-760] - Create h2o.objectFromKey
    * [PUB-766] - Speed RF with Stratified sampling and VI  on covtype throws :java.util.concurrent.ExecutionException: java.lang.ArrayIndexOutOfBoundsException: -1'
    * [PUB-774] - Random Forest confusion matrix for multiclass has actual count for class 0 way too high
    * [PUB-775] - SpeedRF: model links on grid page are broken and grid page does not display prediction error
    * [PUB-776] - cbind(df, df) hits assertion failure
    * [PUB-782] - h2o.deeplearning should return the parameter list 
    * [PUB-786] - DException did not produce full Stack trace on HTML or goto the logfile
    * [PUB-793] - NewChunk: Array index out of range: 8388608
    * [PUB-795] - GLM2: using lambda_search=1 seems to cause hang
    * [PUB-797] - GLM2 binomial with covtype20k (small data) java.lang.AssertionError: mu out of bounds<0,1>:1.3432060071386123
    * [PUB-809] - GLM2: Gaussian on Airlines Data with IsDepDelayed as Y fails "Invalid response variable, trying to run regression with categorical response!"
    * [PUB-810] - Predict on GLM2 produces null prediction for airlines test dataset
    * [PUB-817] - DeepLearning via R needs grid search over hidden layer combinations
    * [PUB-821] - SpeedRF: regression seems broken. On British car insurance data get ArrayIndexOutOfBoundsException
    * [PUB-822] - GLM From R : return all lambdas returns the lambda values but does not correctly chooses the right model when try to predict on a model with a selected lambda value
    * [PUB-823] - h2o.glm: nfold default should be set to 0.
    * [PUB-825] - performance test for classification error on weather data for all algos
    * [PUB-829] - Training data is used as validation data in DeepLearning when called from R
    * [PUB-831] - GLM through R sometimes runs but never finishes
    * [PUB-832] - Cbind raises vector group error
    * [PUB-835] - DRF: model (on california housing data) with 1000 trees finishes fine but hangs and does not display 
    * [PUB-838] - Inaccurate error message: h2o.performance() 
    * [PUB-840] - MSE not retrieved from h2o.confusionMatrix for regression models
    * [PUB-842] - Promote SpeeDRF from Beta
    * [PUB-852] - h2o.performance in Help raises error in R
    * [PUB-853] - Early termination in glm resulting in underfitting
    * [PUB-856] - NullPointerException but model still created
    * [PUB-857] - GLM models differ on data after shuffling
    * [PUB-858] - assertion error in AutoBuffer.java/gbm tree model tree score (DTree.java)
    * [PUB-862] - ddply should support anonymous functions
    * [PUB-863] - disallow addFunction of functions that already exist
    * [PUB-864] - allow addFunction functions to be executed more than once
    * [PUB-869] - SRF reports wrong tree statistics on the model page
    * [PUB-870] - Multi model scoring does not work on SRF grid job
    * [PUB-872] - SRF: for multi class classification, error rate on model page and confusion matrix page does not match
    * [PUB-873] - GLM with Cross Validation: ArrayIndexOutOfBoundsException: 89
    * [PUB-874] - Discrepancy Reporting GLM Cross Validation Models in R
    * [PUB-875] - R: parameter names inconsistency in h2o.gbm when print the grid job summary
    * [PUB-876] - h2o.glm with cross val, stops polling even when the model in still building in h2o
    * [PUB-880] - AIOOBE in Parse
    * [PUB-881] - h2o.glm: grid search summary table does not display best lambda and displays wrong iteration numbers
    * [PUB-882] - auc and cm fields in /2/Models recently became empty, breaking multi-model scoring
    * [PUB-883] - SRF: does not display the java model 
    * [PUB-884] - SRF grid with cross validation, reports NANs for the prediction error on grid page
    * [PUB-889] - 16 JVMS on 4 machines && 4 JVMs per machine 
    * [PUB-890] - GLM: With lambda search, the parameter summary table gives a nonzero predictors =  -1 for the first model . 
    * [PUB-891] - GLM: wrong AIC reporting on adult_income dataset for the 1st model
    * [PUB-895] - Exec2 loop transforming a dataset columns into factors is failing on 3JVMs.
    * [PUB-897] - GLM: class java.lang.AssertionError', with msg 'beta.length != dinfo.fullN(), beta = 721 dinfo = 744' java.lang.AssertionError: beta.length != dinfo.fullN(), beta = 721 dinfo = 744 on airlines_all
    * [PUB-899] - Parse fails with  water.parser.DParseTask.pow10i(DParseTask.java:697)
    * [PUB-907] - Logging time in copy operator
    * [PUB-913] - GLM: grid search with lambda search, when try to set a lambda value throws 'key already in use' error 
    * [PUB-914] - java.lang.AssertionError', with msg 'GLMModel cannot be unlocked
    * [PUB-916] - Make memory specification in h2o.init() a bit more friendly
    * [PUB-917] - Different predict error on training and test with same dataset as both GLM
    * [PUB-918] - ArrayIndexOutOfBoundsException at SpeeDRFModel.java:237
    * [PUB-925] - DeepLearning R unit test sees worse results with balance_classes=T
    * [PUB-928] - When run GLM with xval from R, h2o takes too long to fetch all the lambda models

Improvement
-----------
    * [PUB-700] - Might be nice to have R trunc/round/signif functions for precision control in exec (both for compression reasons and customer code issues?)
    * [PUB-754] - Add bitset based splitting of categorical columns in RF modelling
    * [PUB-755] - Add bitset based splitting of categorical columns for RF in-h2o scoring
    * [PUB-756] - Add bitset based splitting of categorical columns for gbm pojo scoring
    * [PUB-757] - Test bitset based splitting of categorical columns in gbm
    * [PUB-767] - speedrf should do something about non-continous output response ranges (maybe just better warning?)
    * [PUB-781] - Return Models for all Lambda when lambda_search = TRUE
    * [PUB-824] - Return complete DeepLearning model parameters in JSON
    * [PUB-836] - Improve collection of logs
    * [PUB-841] - Add extensive JUnit tests for Chunks
    * [PUB-855] - error messaging should be more explicit when a key of the wrong kind is fed to a parameter (data frame where model key is needed)
    * [PUB-865] - Add -single_precision flag
    * [PUB-866] - Add summary of created chunks after parsing
    * [PUB-885] - Warn R user if they have gcj java

New Feature
-----------
    * [PUB-525] - PCA: Standard Deviations are possibly incorrect
    * [PUB-526] - PCA: Exceeded timeoutSecs: 300 secs while polling.
    * [PUB-677] - Deprecate VA from H2O
    * [PUB-741] - Zookeeper Integration
    * [PUB-745] - Parse JSON AST passed in from front end
    * [PUB-746] - Evaluate a simple AST
    * [PUB-813] - Auto-Encoder for Anomoly Detection
    * [PUB-814] - Scoring Engine for DeepLearning
    * [PUB-830] - Add R wrapper for Anomaly model
    * [PUB-833] - Add use_all_factor_levels flag to DeepLearning


Task
----
    * [PUB-651] - Test predictions on SpeeDRF
    * [PUB-791] - Remove all VA References in R
    * [PUB-792] - Remove VA References in R Docs and Tests
    * [PUB-819] - Add use_all_factor_levels option for GLM in R
    * [PUB-910] - h2o.SpeedRF does not report  seed used by the model from R. It just reports a -1
    * [PUB-924] - h2o.dl is missing variable importance

Technical task
--------------
    * [HEX-1698] - Update Java POJO to do probability correction for balanced_classes = true

Bug
---
    * [HEX-1714] - Typeahead performance is poor for large FV datasets
    * [HEX-1720] - NullPointerException for bad exec expression (non-existent col name on rhs?)
    * [HEX-1734] - Perf harness should alert when failures occur
    * [HEX-1743] - Implement GLM2 model persistence
    * [HEX-1745] - UUIDs in the data are not imported if the # of unique values is larger than ~64000
    * [HEX-1748] - java.lang.RuntimeException: java.lang.IllegalArgumentException: Found 1112 classes: Response column must be an integer in the interval [2,254]
    * [HEX-1755] - GLM: When run collective data without lambda search, throws error  Too many predictors! 
    * [HEX-1757] - auc calculation from R broken
    * [HEX-1778] - UUID parsing/ special case missing
    * [HEX-1779] - cbind of h2o object and non-h2o object: no error raised
    * [HEX-1780] - Assertion hit in score code in DRF
    * [HEX-1782] - Add R Vignette
    * [HEX-1789] - UUID column is preventing a test / train split.
    * [HEX-1791] - Class Balancing crashed H2O for GBM
    * [HEX-1792] - Cannot calculate AUC on collective dataset as columns with >10% NAs get ignored    
    * [HEX-1793] - Strong_rules error even with lambda_search on
    * [HEX-1794] - is.na() inconsistent behavior on UUID col (sometimes crashing)
    * [HEX-1797] - parse ArrayIndexOutOfBoundsException
    * [HEX-1801] - H2O cluster dies in hadoop running deep learning
    * [HEX-1817] - RF/GBM/SpeeDRF/GLM2/KMeans2 model parameters always in "Running" mode
    * [HEX-1819] - Vec.makeConSeq breaking
    * [HEX-1820] - Parse interpreting NAs wrongly
    * [HEX-1821] - Parse2 json: Failed to guess parser setup
    * [HEX-1824] - NullPointerException when testing for need line search
    * [HEX-1826] - Figure out how to handle windows paths from R
    * [HEX-1832] - Parse hits exception failing to find joda timezone resource
    * [HEX-1838] - Unlock validation dataset
    * [HEX-1839] - CRAN submission:: Windows bug
    * [HEX-1840] - Parse fails  water.parser.DParseTask.pow10i (DParseTask.java:697)
    * [HEX-1845] - Cloud formation times out in Jenkins with >= 5 node clouds
    * [HEX-1846] - Jenkins shows green when there are failing tests

Improvement
-----------
    * [HEX-1652] - Parity with R glm and glmnet model accuracy.
    * [HEX-1796] - Add Java POJO scoring code for DeepLearning
    * [HEX-1823] - Handle special parse requirement

New Feature
-----------
    * [HEX-1653] - Reasonable memory requirements on large, sparse datasets.
    * [HEX-1679] - Add exportHDFS alias to mimic exportFile("hdfs://")
    * [HEX-1711] - Design and scope capability to save and deploy models in the H2O environment.

Task
----
    * [HEX-1727] - Bernoulli GBM testing complete
    * [HEX-1750] - Add RUnit test for Strong Rules Memory
    * [HEX-1751] - Add RUnit test for Strong Rules time
    * [HEX-1773] - Add RUnit test for UUID column
    * [HEX-1774] - Add R bindings for model save restore.
    * [HEX-1775] - Add RUnit test for model save and restore.
    * [HEX-1776] - Add R bindings for GLM2 to get models for each lambda in a lambda search
    * [HEX-1825] - Create RUnit test demonstrating how to access lambda search model auc



Release 2.4.4.3 (Kolmogorov build 3)
====================================

* [Download this release](http://s3.amazonaws.com/h2o-release/h2o/rel-kolmogorov/3/index.html)
* [Query JIRAs for this release](https://0xdata.atlassian.net/issues/?jql=fixVersion%20%3D%20kolmogorov%20and%20resolution%20is%20not%20empty)

Technical task
--------------
    * [PUB-667] - Fix a bug in exceptionalCompletion which currently expects that no other tasks are running (=which is wrong assumption)
    * [PUB-716] - Display MCC in Steam

Bug
---
    * [PUB-52] - GLM Tweedie on AllState Dataset throws AssertionError
    * [PUB-160] - GBM: Regression on Kaggle Allstate gives java.lang.AssertionError at hex.gbm.DRealHistogram.scoreMSE
    * [PUB-167] - DRF1: confusion matrix from model built and prediction on the same dataset do not match for covtype
    * [PUB-173] - can't even run *head* from R after manipulating a data frame
    * [PUB-177] - R/h2o.rm fails
    * [PUB-184] - va parse of the  synthetic gz multi-file parse on 8 nodes ..stack trace (fails all the time)
    * [PUB-187] - GLM1: seems like it's not using the model key ('destination_key') I specify as a param, exactly.
    * [PUB-202] -  GBM grid: get java.lang.ArrayIndexOutOfBoundsException on AirlinesTest.csv
    * [PUB-235] - datasets with reals outside the range of single precision FP exponents cause DRF1 to ignore cols. No warning in browser or json about the ignored cols (h2o stdout log WARN only)
    * [PUB-247] - (DOCUMENTATION) IE9/10 may, IE11 definitely, not able to point to 127.0.0.1. IE11 seems to fail with localhost.  I couldn't get IE11 (on win8.1) to point to 127.0.0.1:54321  (the web confirms)
    * [PUB-268] - deviance explained should not be a negative number. 
    * [PUB-274] - standalone-mode hdfs_version=mapr2.1.3 doesn't work anymore. plus I need 3.0.1
    * [PUB-323] - KmeansGrid: java.lang.AssertionError on Airlines data
    * [PUB-338] - Need per-class error, and total error in the DRF2 json like the browser
    * [PUB-371] - Quantiles on random uniform features incorrect 
    * [PUB-379] - parse1: Enable single quotes as field quotation character doesn't work
    * [PUB-391] - Parsing set of gzipped files - the last file is empty - parser is complaing about incompatible files
    * [PUB-400] -  h2o.clusterStatus(client object) can appear healthy even when cloud is sick 
    * [PUB-409] - h2o converts to FV behind R's back
    * [PUB-424] - summary produces weird error when passed nonexistent data
    * [PUB-452] - Errors in intercept term with GLM2 under ridge regression
    * [PUB-453] - AUC on DRF model page displays incorrect results 
    * [PUB-475] - DRF1 on airlines 7M rows with default params throws java.lang.AssertionError', with msg 'Value size=0xd5ba8f'
    * [PUB-476] - RF1 on airlines(120M) uses only ~11% of the available Cpus on both single and multi node
    * [PUB-483] - Deep Learning: If I stop the model midway and score on the same data I was validating the model on, the confusion matrices on the model page and on the score page  do not match 
    * [PUB-484] - summary1 probably still has bounds issues on histograms (summary2 was fixed) this  time parse to *E12 sized numbers
    * [PUB-497] - H2O doesn't work on Java8.
    * [PUB-502] - h2o.deeplearing throwing Error in matrix(unlist(cm), nrow = categories) :    'data' must be of a vector type, was 'NULL'
    * [PUB-516] - h2o.perf needs an R doc page
    * [PUB-535] - h2o gets into a mode where it prints 0 for answer on console? are nulls causing it?
    * [PUB-540] - Multiple simultaneous uploads of same file not possible due to (wrong or right?) lock detection (even if different src_key name is used)
    * [PUB-542] - Get a blank GBM grid page on the webUI , when doing  a multi class classification , on a  3 nodes cluster, Throws : Exception in thread "NanoHTTPD Session"  java.lang.AssertionError  	at hex.ConfusionMatrix.precision(ConfusionMatrix.java:145)  
    * [PUB-546] - ddply and quantile functions in h2o R missing R-help doc
    * [PUB-569] - Apply hits env poppush assertion error when doing quantile
    * [PUB-576] - package ‘h2oRClient’ is not available (for R version 3.0.2) 
    * [PUB-579] - is this a legal ddply: dply(r.hex, c(2,3), function(x) { cbind( mean(x[,1]), mean(x[,2]) ) }
    * [PUB-582] - h2o/target/index.html gives incorrect workflow
    * [PUB-601] - Frame split should be accurate to within 1 row. I'm not getting a 50% split when I ask for one. Is it an approximate split (documentation?)
    * [PUB-605] - refcnt issue with ifelse()...maybe doesn't like lhs assigns in both clauses of the ifelse(), or maybe because one is adding a column?
    * [PUB-618] - GLM2 called from R incorrectly returns model when standardize switched on
    * [PUB-625] - R is probing cloud status before cloud is stable/healthy. results in unhealthy report after h2o.init
    * [PUB-638] - Test GLM2 demo mode on airlines in EC2
    * [PUB-639] - Glm2 grid over alphas on (subset) airlines dataset throws JobCancelledException
    * [PUB-645] - Grid search causes "Value is not a valid number sequence." error
    * [PUB-646] - 3 way Frame split (array) does not work
    * [PUB-657] - assigning to a length 4 column with c(0) doesn't work where assigning with 0 works (and R says both should work)
    * [PUB-659] - Fix LoadDataset usage of FrameSplitter tasks
    * [PUB-660] - Split frame on kddcup data throws java.lang.ArrayIndexOutOfBoundsException 
    * [PUB-663] - Add bernoulli distribution for GBM
    * [PUB-665] - Cannot start H2O from inside R
    * [PUB-676] - FrameSplitTask dies on empty chunks
    * [PUB-684] - ifelse() is kind of a merge function in R (not a if/else ternary)...doesn't seem to work in h2o
    * [PUB-686] - Multi Model Scoring returns error every time
    * [PUB-687] - exec: using my own function with apply, getting weird error message about col 2. Using function defn. that works in ddply, not apply
    * [PUB-688] - 26GB file from hdfs (cdh3), 4 jvms (40GB) on 164, import/parse (fvec) gets NPE  Missing vector during Frame fetch
    * [PUB-691] - cbind(a,b,d) (named h2o vectors/frames) in console got assertion error (refcnt error, then assertion on Env.tos_into_slot. Can't do anything in Exec afterwards
    * [PUB-694] - is "e" special in exec? got weird java formatting error message
    * [PUB-695] - For variable importance, getting weird plots in safari.
    * [PUB-707] - R: there is no param in h2o_init() for starting h2o with assertion checking enabled
    * [PUB-714] - Cannot access  grid page from jobs page
    * [PUB-731] - Merge tachyon support into H2O master
    * [PUB-736] - GBM variable importance logging says INFO DRF__
    * [PUB-738] - Pattern Matching in Parse2 Broken on Windows
    * [PUB-739] - Frame extractor for n-fold cross validation
    * [PUB-764] - SpeeDRF gives wrong OOB error for multiple nodes
    * [PUB-765] - Predict gives NPE with SpeeDRF model on multinode
    * [PUB-768] - N-fold cross-validation combine with Balance Classes Function throws IllegalArgumentException
    * [PUB-769] - SpeedRF: Confusion matrix looks confused. For binary classification, when run with oob data, reports two CMs , one with total rows = 2X dataset size
    * [PUB-771] - Parse throws AIOB - water.parser.DParseTask.pow10i(DParseTask.java:697)
    * [PUB-777] - Assertion failure trying to close NewChunk during parse
    * [PUB-778] - SpeedRF:  throws java.lang.ArrayIndexOutOfBoundsException: -32768 on hepatitis_data



Improvement
-----------
    * [PUB-111] - Hadoop collect logs
    * [PUB-271] - the h2o on hadoop yarn driver should return "application id" so logs can be grabbed from the command line
    * [PUB-334] - glm gamma not reporting aic
    * [PUB-463] - Add AUC to R interface
    * [PUB-478] - && instead of & in row cut expressions. Exec takes it. (but it's user error)...is it legal? or should an error be caught
    * [PUB-500] - Distributed chunk load balancing
    * [PUB-527] - GLM2 demo work
    * [PUB-528] - Rechunking sparse dataset without inflating it (svmlight)
    * [PUB-578] - Exec still getting auto-frame of keys that weren't parsed, just imported (folder was imported, 1 key parsed) . supposed to only be parsed keys
    * [PUB-628] - ImportFiles2 doesn't seem to support maprfs:/
    * [PUB-655] - Add option to set seed to runif
    * [PUB-658] - Unlock All
    * [PUB-661] - Make Confusion Matrix domains consistent for binary classification in GBM, DRF and GLM2.
    * [PUB-723] - Error message from h2o.importFile should be better when the file exists on just one node
    * [PUB-726] - Warn user that h2o is creating ice_root directories as root

New Feature
-----------
    * [PUB-307] - `levels' needed to query an enum Vec in exec2
    * [PUB-434] - Add Tutorial for GBM on main page.
    * [PUB-511] - Add a tutorial for GBM
    * [PUB-533] - Port DRF1 to FV
    * [PUB-610] - Export Frame to HDFS
    * [PUB-622] - Need exportToHDFS from R.
    * [PUB-650] - Add AUC page to speedrf
    * [PUB-652] - Unify SpeeDRF model JSON output with DRF2
    * [PUB-653] - Regression for SpeeDRF
    * [PUB-654] - Variable importance for SpeeDRF
    * [PUB-703] - Create a random Frame in H2O from R
    * [PUB-715] - Matthews Correlation Coefficient
    * [PUB-720] - Tools: A profiler on H2O Nodes

Story
-----
    * [PUB-727] - Transfer data from Spark to H2O

Task
----
    * [PUB-87] - Need FVec to VA converter testplan
    * [PUB-192] - RF1 : Verify if you can increase the limit for response column 
    * [PUB-640] - Finish GBM/DRF model checkpointing
    * [PUB-641] - StoreView provides wrong links for a key



Release 2.4.1.7 (Kalman build 7)
=================================

* [Download this release](http://s3.amazonaws.com/h2o-release/h2o/rel-kalman/7/index.html)
* [Query JIRAs for this release](https://0xdata.atlassian.net/issues/?jql=fixVersion%20%3D%20kalman%20and%20resolution%20is%20not%20empty)

Bug
---
    * [PUB-27] - Failure Recovery when GLM2 fails on GBM
    * [PUB-35] - gbm: prediction broken with superset factors
    * [PUB-123] - DRF2: fails on dataset with constant columns
    * [PUB-127] - Running GLM model on .hex data set and then .hex.autoframe in GLM2 appears to produce a model, but not display it 
    * [PUB-138] - runit_demo_exec2.R is failing
    * [PUB-141] - GBM: cannot generate 30k trees on cars 
    * [PUB-159] - CLONE - GLM2: constant column causes an exception if GLM2 is executed from R
    * [PUB-164] - CLONE - Exported GBM model prediction differs from in-h2o prediction
    * [PUB-188] - After creating a model, when delete the dataset and then click on the built model link, get NPE
    * [PUB-201] - h2o.ls gives incorrect results when drive h2o from two R terminals
    * [PUB-218] - exec to turn col 379 into binomial on nflx manyfiles  causes stack trace
    * [PUB-229] - comparing GLM1 vs GLM2 coefficients (binomial). slightly different
    * [PUB-230] - I don't see 'progress' in the GLM2 polling response (other algos give 'progress' in their response)
    * [PUB-231] - KMeans2: needs to handle cols with all NA better..causes NaNs apparently in cluster variances, centers, probably error
    * [PUB-232] - GLM2: browser, not showing per-class error rate, also issues about what threshold is used
    * [PUB-234] - GLM2 + Predict + ConfusionMatrix not compatible with use of case_mode/case_val params in GLM2
    * [PUB-237] - exec2/summary2: should we declare standard dev = NaN  always wrong for numeric cols? (summary2). example I see
    * [PUB-241] - Big exponents seem to make DRF2 fail (probably GBM too, since shared code here?)
    * [PUB-245] - win8.1, jacobi install, h2o launched from R 3.0.2 for windows. can't read the h2o_vbox_started_from_r.err/r.out files due to permission problems (h2o stdout/err logs are fine though?). Can read them when I exit R.
    * [PUB-248] - can't tell when I'm supposed to h2o.init() or not, after restarting R
    * [PUB-249] - win8.1, used R h2o.init() to start h2o java process, killed R with Task Manager. child process (h2o) still running with no visual indication for user
    * [PUB-250] - win 8.1, R 3.0.2, multiple h2o.init() doesn't complain. Unclear if I'm allowed to do that, (should give feedback to user, since h2o might not be the h2o you want)
    * [PUB-252] - win8.1, r 3.0.2, if I start two R guis, and h2o.init(), nothing complains, yet there is only one java process
    * [PUB-276] - glm2: 'cms'  json: '_predErr' is NaN and '_classErr' is wrong
    * [PUB-277] - '_cms' is an array of cm in glm2 json result. for varying threshold. Have to have 'threshold' key in there, so can tell what is what
    * [PUB-283] - Missing chunk 0 after exec expression; inspect shows NaN for min/max/mean for that col. h2o stdout has missing chunk message
    * [PUB-285] - adding known working single execs, to a longer multi-expression str=.  NPE in Env.allAlive (Env. remove_and_unlock)
    * [PUB-287] - exec AAIOBE at at water.exec.Env.push_slot(Env.java:98)
    * [PUB-288] - Exec, assertion error at at water.exec.Env.poppush(Env.java:201)
    * [PUB-290] - exec assertion error at at water.exec.Env.popDbl(Env.java:183)
    * [PUB-310] - GLM2 exception
    * [PUB-314] - GLM2: covtype class=4 binomial, inverted. Bad results? Maybe the auto-threshold detection is bad 
    * [PUB-315] - Predict2: GLM2 model NA predictions
    * [PUB-317] - GLM2 on Airlines data fails with NPE
    * [PUB-320] - Predict on glm2 produces predictions in different formats for different data sets
    * [PUB-321] -  KMeans2 AAIOBE (KMeans2Model.score0)
    * [PUB-322] - GLM2: can't seem to get correct Predict output for a covtype binomial case
    * [PUB-325] - Kmeans2: get error- Cannot ask for roll-up stats while the vector is being actively written, when running from inside R
    * [PUB-326] - GLM2:java.lang.NegativeArraySizeException on Airlines data on two or more nodes
    * [PUB-328] - Model specification needs to be reported on model results page GLM
    * [PUB-333] - GLM2: Even if the model finishes successfully, the same key name gets assigned to the next GLM2 job
    * [PUB-335] - GLM2 on 4-nodes on RC dataset throws NPE
    * [PUB-339] - Exec2: scalar expressions don't seem to create keys so you can reference h2o scalar results in subsequent expressions
    * [PUB-341] - Model specification needs to be reported on model results page  GBM
    * [PUB-344] - Model specification needs to be reported on model results page PCA
    * [PUB-347] - GLM2 exception.  java.lang.ArrayIndexOutOfBoundsException
    * [PUB-349] - Confusing confusion matrix for mixed inputs
    * [PUB-356] - GLM 2 with regularization inconsistent with R
    * [PUB-360] - GLM2: JSON threshold not best
    * [PUB-363] - GLM2 java.lang.ArrayIndexOutOfBoundsException: 733
    * [PUB-365] - GLM2 not actually producing probabilities in binomial predict
    * [PUB-378] - leaving lambda in default results in no model 
    * [PUB-421] - [[ ]] not wired up in R
    * [PUB-423] - cbind missing
    * [PUB-446] - Errors in K means grid, results from models not updated to results page. Also: F1 missing 
    * [PUB-470] - Rollup stats deadlock if dataset is used by an eq2 slice before doing an inspect
    * [PUB-509] - R startup info is wrong for a just-started jvm
    * [PUB-530] - Figure out DRF2 default settings
    * [PUB-541] - GBM with VI on 3 nodes gives : java.lang.ArrayIndexOutOfBoundsException: -1
    * [PUB-551] - hadoop distcp left logs in hdfs. h2o imports, and apparently doesn't like to RemoveKey them
    * [PUB-557] - exec assertion error...maybe fair to say the expression is excessive, but I believe legal. it parsed in R..assertion error in H2O
    * [PUB-561] - fvec import/parse of 50 manyfiles gz fails on 2 machines. works on one machine
    * [PUB-563] - glm2 grid search from R throws Internal server error
    * [PUB-566] - ddply with cbind  on more than 1 columns in R returns only 1 column in the output
    * [PUB-580] - confusing case with import folder, gz files, 2 jvms, exec (iterate)
    * [PUB-581] - R install fixes for 3.1
    * [PUB-586] - When run RF with variable importance from R on a regression dataset  get :java.lang.AssertionError: Cannot get vector domain!
    * [PUB-588] - I believe all exec functions have to be on their own exec str= currently (can't combine with any other expressions)
    * [PUB-590] - Inspect2 not Redirecting to Model view page
    * [PUB-592] - DeepLearning checkpoint restart of non-enum responses for classifiers fails on multiple nodes
    * [PUB-597] - can't have type change after variable assigned inside a function (assign it as dbl, can't assign it as ary later in the function)
    * [PUB-598] - I think this shows a problem with lhs assigns in the clauses of a ternary op
    * [PUB-602] - Move all tutorials in the product to FVec (they are still on VA)
    * [PUB-611] - benign on GLM2 - both AUC & xval results
    * [PUB-612] - lambda search... not searching?
    * [PUB-617] - Import URL fails to parse

Improvement
-----------
    * [PUB-120] - Kmeans 1 output in gui
    * [PUB-293] - maybe glm2 could more directly report the user error if I say ignored_cols has the output response
    * [PUB-498] - Sparse data handling for DeepLearning
    * [PUB-537] - Add DeepLearning options to R wrapper
    * [PUB-574] - A bad h2o R package install into an alternate R library, might require walking the .libPaths() vector to make sure h2o package is removed. Maybe recommend h2o package not be installed as root.
    * [PUB-577] - should mean() and sd() be fixed to work in ddply like min/max  and the other built-in column functions?

New Feature
-----------
    * [PUB-512] - Add a tutorial for Deep Learning
    * [PUB-523] - Naive Bayes java algorithm implementation
    * [PUB-524] - R connector for Naive Bayes
    * [PUB-593] - Implement Predict() for DeepLearning in R

Task
----
    * [PUB-86] - Java POJO predict model testplan
    * [PUB-91] - Menu cleanup



Release 2.4.0.4 (Kahan build 4)
================================

* [Download this release](http://s3.amazonaws.com/h2o-release/h2o/rel-kahan/4/index.html)
* [Query JIRAs for this release](https://0xdata.atlassian.net/issues/?jql=fixVersion%20%3D%20kahan%20and%20resolution%20is%20not%20empty)

Bug
---
    * [PUB-6] - columns with no data reporting standard error
    * [PUB-30] - bin names for summary when data is reals, (seems like round to int, then str)..leads to same bin names
    * [PUB-40] - Summary results incorrect  percentiles incorrect 
    * [PUB-48] - Different lists of masked functions between install and library
    * [PUB-105] - import folder (va) parse (va) ...then exec causing va to fvec ..caused looping across all in store view
    * [PUB-116] - R imports shouldn't name h2o keys beginning with a number
    * [PUB-118] - kmeans1 (from R)
    * [PUB-121] - summary2 broken (or massive memory leak)
    * [PUB-122] - GLM Binomial produces questionable coeffs 
    * [PUB-124] - R tests do not correctly terminate H2O JVM if target/R does not exist
    * [PUB-137] - runit_NN_multiclass.R is failing
    * [PUB-146] - summary histogram wrong
    * [PUB-148] - R: head broken with NAs
    * [PUB-149] - GBM: MSE is different for runs with/without validation dataset which is the same as train dataset
    * [PUB-161] - table in R still broken
    * [PUB-162] - For h2o.glm, there is no way to get the info about the parameters from the model object
    * [PUB-169] - apply in R not working for columns 
    * [PUB-170] - R: can't assign to columns that don't exist yet
    * [PUB-176] - R/h2o can't do nested ifelse
    * [PUB-178] - R: apply(X,1,sum) wrong
    * [PUB-179] - quantiles and summary give inconsistent information 
    * [PUB-180] - R: need ddply
    * [PUB-190] - h2o.uploadFile throwing internal server error on git hash: 9f70e2080eec3884af7268da79bec095f4e93c5f
    * [PUB-191] - make for h2o fails for 32 bit linux machine
    * [PUB-194] - h2o.gbm grid throws Internal Server Error 
    * [PUB-195] - R: can filter w/ columns that don't exist
    * [PUB-198] - Change windows tmp directory to someplace that's not c:\
    * [PUB-199] - Why do i get the following Warning msg when run h2o.gbm on a large categorical response variable
    * [PUB-200] - Scala REPL produces IFCE during parsing/showing tsv file.
    * [PUB-204] - GLM grid: When click on models built on allyr airlines with default params, get NPE
    * [PUB-210] - Creating a new column results in a synthesized colname Cx that is 0-based not 1-based
    * [PUB-211] - R: can't install local build of R package
    * [PUB-214] - R: upgrading h2oRClient no longer works correctly
    * [PUB-215] - exec dies on files with - in the name
    * [PUB-216] - Wrong Confusion Matrix (and wrong classification error) if the first element of the prediction domain is not predicted
    * [PUB-225] - Can't Inspect (fvec) a created KMeans2 Model. assume we want to be able to
    * [PUB-228] - Exec2: apply causes json response with  d != java.lang.Double (d is not part of the expression)
    * [PUB-233] - glm1 tweedie: java.lang.AssertionError: invalid weight NaN while mapping key $00000000000000000000$covtype.20k.hex
    * [PUB-238] - AIOOB Error in Parse Dataset
    * [PUB-239] - Every-time you refresh the Prediction page a new random Destination key name should be generated
    * [PUB-246] - win 8.1, R 3.0.2, h2o launched from R per jacobi. Old log file (127.0.0.1) isn't removed?
    * [PUB-251] - K-Means result summary of clusters should be categorical, not numerical
    * [PUB-253] - win8.1, r3.0.2 h2o.init(). h2o seems to start with 54321 port open to outside network
    * [PUB-254] - browser: admin/get script should be removed (404 error)
    * [PUB-257] - KMeans FV End to End Issues
    * [PUB-258] - clustering vector in K means returns nonsense information 
    * [PUB-259] - With an h2o data frame I should be able to run at least one of the K means algos
    * [PUB-261] - ROCPlot html stack trace in h2o. just doing prostate GLM1
    * [PUB-262] - When Validate a GLM model on a dataset, get an ROC curve with only single observation point
    * [PUB-263] - K means initialization in both K means 1 and 2 
    * [PUB-264] - PCA error not very informative
    * [PUB-267] - runif producing constant column
    * [PUB-270] - NPE during multi-machine/multifile gz fvec parse
    * [PUB-275] - Kmeans 1 returns error running on enums, Kmeans 2 does not
    * [PUB-278] - as.h2o not behaving as expected
    * [PUB-301] - intermittent NPE on neural net 	at hex.Trainer$MapReduce.done(Trainer.java:320)
    * [PUB-303] - On Neural Net Score page, response and ignore colums fileds have no effect on scoring  
    * [PUB-304] - neural nets need user sampling control for validation set size
    * [PUB-305] - as.data.frame is broken
    * [PUB-306] - R needs colnames implementation
    * [PUB-308] - DRF2 on customer churn data with 3000 trees, default depth,  gives java.lang.IllegalArgumentException:at java.util.Arrays.copyOfRange
    * [PUB-311] - Minor R-package bugs
    * [PUB-316] - Model specification needs to be reported on model results page Kmeans
    * [PUB-318] - neural net samples/s counter wrong
    * [PUB-319] - fvec upload/parse. source/dest overwrite. NPE around frame delete and lock?
    * [PUB-324] - Standard deviation should not be reported for enums
    * [PUB-329] - ImportFolder2 broken from R
    * [PUB-330] - GBM: gives MSE of an extra tree in the output 
    * [PUB-331] - Neural Net:While scoring on full dataset, On single/multi Nodes, get ArrayIndexOutOfBoundsException: 53 
    * [PUB-332] - Neural Net Regression
    * [PUB-337] - GLM2:java.lang.ArrayIndexOutOfBoundsException on Prostate
    * [PUB-340] - nn npe on c9 data
    * [PUB-343] - DRF Variable importance : variable importance from H2O and R on prostate dataset do not match
    * [PUB-348] - drf2 illegal argument exception mid run
    * [PUB-350] - Model specification needs to be reported on model results page - NN
    * [PUB-351] - Exec slice issues (2): first: java.lang.RuntimeException: java.lang.NegativeArraySizeException: null while mapping key $00000000400100000000$4c637938-7084-4aec-ad32-198c4b0595d8
    * [PUB-352] - Support ignored columns in Neural Nets
    * [PUB-353] - h2o parse assertion fail (VA). The assertion is either true and lies, or is false and tells the truth
    * [PUB-354] - parse should reject if column names are not unique
    * [PUB-355] - GLM2: Key is not a frame
    * [PUB-357] - exec2 AIOOBE. 8. Happens in a number of exec2 tests (started last Fri?)
    * [PUB-358] - NPE: fvec multi-file import/parse on 4 jvm cloud (164) (50 nflx files
    * [PUB-359] - NN Returns Finished Before Task Completed
    * [PUB-361] - exec2. adding two single value keys gets AIOBE (sometimes?)
    * [PUB-362] - Neural Net Score Key DNE
    * [PUB-364] - Assertion during parse leaves key locked
    * [PUB-374] - Make ROC curve output for GBM
    * [PUB-375] - windows c:\tmp issues and R
    * [PUB-377] - Kmeans in R rejects H2O parsed dataframes 
    * [PUB-380] - ddply: can't return > 1 value
    * [PUB-381] - ddply: results lose their enum-ness
    * [PUB-383] - R/Exec need unique
    * [PUB-390] - runit_gbm_1_golden.R failure
    * [PUB-392] - Oracle javac - 1.6.0_25 - build.sh is failing to compile source code
    * [PUB-394] - multi-jvm, autoframe triggered by exec, exec doesn't see the resulting key 
    * [PUB-395] - R hang: bad relative pathname on h2o.importFile() seems to cause R hang?
    * [PUB-396] - Predict page shows dates instead of real-number 
    * [PUB-399] - h2o R package errors on startup and beginning of workflow
    * [PUB-403] - Incorrect ordering  of classes in CM
    * [PUB-408] - Import/Parse File Must Auto-Generate Legal Keynames
    * [PUB-410] - R breaks with more than 1000 columns
    * [PUB-411] - wrong year extracted from date
    * [PUB-413] - Summary2: only 10 bins in the histogram (all negative numbers). you can see it affects quantile accuracy (don't we need at least 100 bins always for quantile to get 1% accuracy at the edges)
    * [PUB-414] - summary2: if we keep current threshold=bin edge, there's an end condition for the 99% threshold where value might need to be estimated? (because 1% bin is smaller than bin size?)
    * [PUB-416] - Summary2: smalldata/runifA.csv. only creates 2 and 4 bins on the col histograms
    * [PUB-420] - R: need to be able to create complex functions
    * [PUB-425] - Quantiles with NAs in col
    * [PUB-427] - R: ddply / exec2 functions need named column referencing
    * [PUB-429] - NN on Covtype with  -1 to 6 labels has an extra NA level in the confusion matrix
    * [PUB-431] - h2o.uploadFile breaks when path contains spaces in Windows
    * [PUB-433] - R: installing h2o package sometimes doesn't auto install dependencies
    * [PUB-437] - ddply wrong answers
    * [PUB-445] - NPE on GLM2 with regularization through R
    * [PUB-447] - please report deviance explained in R GLM results 
    * [PUB-448] - Sort variable importance values 
    * [PUB-458] - click thru link on 2/Inspect.hml is wrong. it does 2/SummaryPage.html. should be 2/SummaryPage2.html?
    * [PUB-462] - AUC page throws assertion error
    * [PUB-468] - stop capitalizing column names
    * [PUB-469] - as.factor buttons slowly fill with a darker color (but not all the way)
    * [PUB-472] - cant cbind expressions
    * [PUB-473] - The display table for parameters on GLM2 model page looks weird
    * [PUB-477] - R needs matrix mult / %*%
    * [PUB-494] - Mismatch between H2O and R quantiles
    * [PUB-501] - The R-h2o Confusion matrix function should throw appropriate error msg, if user gives an different dataset than the one used for prediction
    * [PUB-504] - h2o.confustionMatrix printing transpose of the correct CM
    * [PUB-510] - DRF2 only uses cores from one of 4 hosts on big airlines
    * [PUB-515] - h2o.ls() throws Warning message: In matrix(unlist(myList), nrow = res$num_keys, ncol = 2, byrow = TRUE) :
    * [PUB-517] - h2o.glm.FV  - has a nan in CM printing 
    * [PUB-519] - NPE during Parse2 on 64 nodes
    * [PUB-520] - h2o.confusionMatrix throws error when test set has extra level

Improvement
-----------
    * [PUB-298] - How to debug R problems at the customer
    * [PUB-309] - Plot function in R is broken
    * [PUB-369] - NN should randomize rows for every epoch
    * [PUB-370] - FV: no-headers column name defaults should be C1, C2, C3, ...
    * [PUB-407] - NN tuning to handle imbalanced datasets and adding F1score/AUC 
    * [PUB-449] - Grid Search should Return Exception if Model Error
    * [PUB-465] - Feature importance for Deep Learning
    * [PUB-471] - Quantile should take multiple cutoffs
    * [PUB-493] - cd5 with h2o installed as service. Don't see a way to configure h2o java -jar params, notably heap size
    * [PUB-514] - can get odd errors with h2o on hadoop driver, if local node doesn't have the right yarn config (i.e. local runner)
    * [PUB-538] - Make forward and back prop faster
    * [PUB-544] - Add scala interface for quantile

New Feature
-----------
    * [PUB-209] - R: add clusterInfo
    * [PUB-236] - can we please add key assignment option to as.h2o
    * [PUB-269] - Add Support Button / Tab on our Product
    * [PUB-295] - Make ROC curve output for RandomForest
    * [PUB-366] - report on error movement by epoch
    * [PUB-367] - calculate auc
    * [PUB-368] - make sure validation set sample includes enough minority class members
    * [PUB-412] - Report Prediction Error in JSON
    * [PUB-480] - Naive matrix transpose and multiply

Task
----
    * [PUB-372] - Document h2oRClient Methods
    * [PUB-373] - Document h2oRClient S4 Objects



Release 2.2.0.3 (Jacobi build 3)
================================

* [Download this release](http://s3.amazonaws.com/h2o-release/h2o/rel-jacobi/3/index.html)
* [Query JIRAs for this release](https://0xdata.atlassian.net/issues/?jql=fixVersion%20%3D%20jacobi%20and%20resolution%20is%20not%20empty)

Bug
---
    * [PUB-5] - summary page percentiles need fixing when small # of discrete values
    * [PUB-7] - CM may not recalculate correctly if same model_key is used for sequential RF's
    * [PUB-8] - upload benign.csv; parse, click on rf link to get to rf query. NPE at at water.api.RF$RFColumnSelect.query
    * [PUB-10] - parse bug / idx oob exception airline dataset
    * [PUB-11] - Unbalanced quoted strings interact poorly with line separators
    * [PUB-13] - RF predict not actually predicting
    * [PUB-14] - GLM score/predict stack traces
    * [PUB-15] - GLM predict "download as CSV" option generating poorly formatted output
    * [PUB-16] - GLM predict will not predict when column names are assigned by H2O
    * [PUB-17] - non-existing s3n uri (thru import hdfs) gets npe
    * [PUB-18] - When returning to GLM page through compute new model, an error message is automatically displayed
    * [PUB-19] - GBM: Creates new category
    * [PUB-20] - Tweedie Normalized Coeffiicients aren't sorted.
    * [PUB-21] - Once used parsed data cannot be reused
    * [PUB-22] - Admin:StackDump should log stack traces to log file
    * [PUB-23] - GBM:On browser, Vresponse does not let you select from the drop down choices 
    * [PUB-24] - download downloads garbage
    * [PUB-25] - -0 for standard deviation in inspect page
    * [PUB-26] - GBM Fails on Multiple JVMs with Ecology Dataset
    * [PUB-28] - Hit an assertion while autoframing
    * [PUB-29] - R Wrapper / Client installation on RConsole / R default GUI failed on Windows 8
    * [PUB-31] - GBM: Even if the model finishes successfully, the same Destination key name gets assigned to the next GBM job
    * [PUB-32] - Error while viewing Store View: error:HTTP500:java.lang.ArrayIndexOutOfBoundsException
    * [PUB-33] - UI autocomplete click bug
    * [PUB-34] - PCA: ArrayIndexOOBException on 1000x100 size dataset
    * [PUB-36] - Summary: java.lang.AssertionError: _start is NaN!
    * [PUB-37] - GBM:Wrong summary on the prediction page for muticlass classification.
    * [PUB-38] - Confusion Matrix: While creating a CM for GLM1 prediction on airlines data, gives java.lang.AssertionError  
    * [PUB-39] - GBM: gives MSE of an extra tree in the output
    * [PUB-41] - parse sinqle_quotes=1 doesn't work if first char in token is ' (cols end up getting NAs)
    * [PUB-42] - tree building: AIOOBE
    * [PUB-44] - GLM reporting NaN on Null Deviance
    * [PUB-45] - GLM allows users to model data that have more predictors than observation
    * [PUB-46] - GLM1: generated confusion matrix is wrong
    * [PUB-47] - R example in h2o/R/h2oRClient-package/man/h2o.importFile.Rd broken 
    * [PUB-49] - PRC in R returning error from documented example
    * [PUB-53] - link in GLM validation models "not found" 
    * [PUB-54] - glm from R throws "Error in round(model$null.deviance, 1)"
    * [PUB-55] - AUC incorrect
    * [PUB-56] - Model specification is producing two autofill drop down menus
    * [PUB-57] - Confusion Matrix Vector chunks differ
    * [PUB-58] - While running multiple jobs at same time got non SPD error in GLM for data set that is totally fine - output of same requested model in R was fine
    * [PUB-59] - GBM models don't seem to echo parameters (except N, number of trees). Can't see what created best result?
    * [PUB-60] - GBM doesn't take from:to:step for gridding param
    * [PUB-61] - Clicking RandomForest on Inspect page throws Invalid Hex file message
    * [PUB-62] - AutoBuffer$TCPIsUnreliableException while running NNet on covtype test train with response = V5
    * [PUB-64] - GBM returns n+1 trees when n are requested 
    * [PUB-65] - gbm scoring is incorrect
    * [PUB-66] - Exported GBM model prediction differs from in-h2o prediction
    * [PUB-67] - Minimum Returns Wrong Answer
    * [PUB-68] - Slicing returns wrong result
    * [PUB-69] - I think autoframe using Exec doesn't seem to work (single jvm)
    * [PUB-70] - Exec2: intermittent AIOOBE with year2013.csv (airlines)..fvec chunk related?
    * [PUB-71] - Reliable recipe for creating water.exec.Env.pop assertion
    * [PUB-72] - Parse fails with NPE on non-ascii characters
    * [PUB-73] - h2o.R fails if attempting to import multiple files with the same key name
    * [PUB-74] - h2o.R table broken in R
    * [PUB-76] - GBM regression results incorrect in R
    * [PUB-77] - R Docs missing 
    * [PUB-78] - The example on  "H2o in R" doc page should be copy-paste runable.
    * [PUB-79] - Need FVec to VA converter
    * [PUB-80] - GBM Grid :java.lang.ArrayIndexOutOfBoundsException: -1 on SF crime dataset
    * [PUB-81] - R: R CMD INSTALL package should warn curl-config
    * [PUB-82] - glm (on VA): if matrix is non spd R must report
    * [PUB-83] - DRF: test error lower than train error on UCI churn data
    * [PUB-84] - GBM assertion in DRealHistogram
    * [PUB-85] - gbm shouldn't crash with a single NA in dependent var
    * [PUB-97] - GBM:java.lang.AssertionError DRealHistogram.scoreMSE(DRealHistogram.java:84) on cov type test-train
    * [PUB-98] - GBMGrid: while the model is in progress, the prediction error column in the browser shows NAN
    * [PUB-99] - mean and variance are not recomputed for a col when you do "a = Players.hex[0]
    * [PUB-101] - glm1 doesn't correctly handle spd + no penalty
    * [PUB-102] - Summary2 Quantiles AIOOB Exception
    * [PUB-103] - in r: Client package - man/h2o/h2o.prcomp.Rd example broken
    * [PUB-104] - assertion error on fvec to va
    * [PUB-107] - GLM1 golden test NOPASS glm1_2 and glm1_3
    * [PUB-108] - table in R? 
    * [PUB-109] - importFile and importFileVA are not working 
    * [PUB-110] - S3N parse got exception from S3
    * [PUB-112] - glm1 from R has incorrect number of null degrees of freedom (+1)
    * [PUB-113] - VA import eats columns with duplicate names
    * [PUB-114] - VA importFile followed by VA kmeans results in an unexpected autoframe
    * [PUB-115] - Import Folder R example
    * [PUB-117] - Error in GBM Java model generation: "cannot find symbol Infinityf"
    * [PUB-129] - ValueArray.frameAsVA(k) call causing AIOOBE
    * [PUB-130] - Remove weight from GLM interface and Web UI
    * [PUB-132] - Windows 8 R library(h2o) install is broke
    * [PUB-139] - runit_binop2_divCol.R failed
    * [PUB-140] - runit_tail_numeric.R failed
    * [PUB-144] - Wrong rescaling of probabilities produced by GBM
    * [PUB-147] - Confusion matrix is wrong for GBM results on titanic dataset
    * [PUB-154] - GLM: constant column causes an exception if GLM is executed from R
    * [PUB-156] - Frame to VA convertor should not be called here
    * [PUB-157] - GLM1: AUC on all_airlines deteriorates on present master as compared to Godel
    * [PUB-165] - All RUnit tests are failing with Unknown var
    * [PUB-182] - testdir_multi_jvm/test_basics.py fails...doing drf1 on covtype. Unlock.atomic..during RandomForest build
    * [PUB-183] - VA Summary changed? getting empty min[]/max[] and bad 'N'  (could be a parse issue?)
    * [PUB-185] - odd new thing. 4B rows in dataset. I parse (va) and use inspect to get num_rows. (historically good). Here it says num_rows was only 503,348 after the parse (rather than 4B). Normal csv (not gz)
    * [PUB-186] - (NA related?) drf1 rfview on airlines on 172-180, use_non_local_data = 0. AIOOBE (Data.filterVal3 in Data.java)
    * [PUB-196] - CM matrix cannot handle two vectors with different domains

Improvement
-----------
    * [PUB-4] - Running GLM model on another dataset: issues when expanded categoricals are in play
    * [PUB-95] - Parse resource management check
    * [PUB-96] - Don't allow the user to overwrite an existing key
    * [PUB-131] - Add threshold option to R interface for GLM
    * [PUB-142] - GBM logging not providing frequent enough updates
    * [PUB-143] - Generated model forest size causes compilation error

New Feature
-----------
    * [PUB-106] - Jobs Page should Return Error Messages

Task
----
    * [PUB-88] - Disallow RF1 to give an answer if not copying all data everywhere
    * [PUB-89] - Converter seamlessness with Store View.
    * [PUB-90] - Plug in FVec to VA converter to algos
    * [PUB-93] - H2O should start when given -ip localhost even if there is no network or no multicast
    * [PUB-94] - R installation of H2O package -- ease of use
    * [PUB-125] - Windows 8 installation for R
    * [PUB-126] - Windows 8.1 install for R
    * [PUB-128] - Submit h2o R package to CRAN
    * [PUB-136] - Submit new package to CRAN
