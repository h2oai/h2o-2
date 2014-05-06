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
