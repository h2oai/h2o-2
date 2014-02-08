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
