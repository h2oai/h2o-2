# H2O Ensemble (beta)

The `h2oEnsemble` R package provides functionality to create ensembles from the base learning algorithms that are accessible via the `h2o` R package.  This type of ensemble learning is called "super learning", and historically has been referred to as "stacked regression" or "stacking."  The super learner algorithm learns the optimal combination of the base learner fits. In a 2007 article titled, "[Super Learner](http://dx.doi.org/10.2202/1544-6115.1309)," it was shown that the super learner ensemble represents an asymptotically optimal system for learning.

## Install
- To install the `h2oEnsemble` R package, clone the main h2o repository, cd to the `ensemble` directory and install the package:
```
git clone https://github.com/0xdata/h2o.git
cd h2o/R/ensemble
R CMD INSTALL h2oEnsemble-package
```

## Create Ensembles
- An example of how to train and test an ensemble is in the `h2o.ensemble` function documentation in the `h2oEnsemble` package.
- Currently, there are some issues when using h2o-based wrapper functions as the metalearner.  This is why there is an example using the `SuperLearner::SL.glm` and `SL.nnls` function as a metalearner instead.  This will be addressed in a future release.
- The class of the ensemble (list) object is "h2o.ensemble".

## Wrapper Functions
- The ensemble works by using wrapper functions (located in the `wrappers.R` file in the package).  These wrapper functions are used to specify the base learner and metalearner algorithms for the ensemble.
- This methodology of using wrapper functions is modeled after the [SuperLearner](http://cran.r-project.org/web/packages/SuperLearner/index.html) and [subsemble](http://cran.r-project.org/web/packages/subsemble/index.html) ensemble packages.  The use of wrapper functions makes the ensemble code cleaner by providing a unified interface.
- Often it is a good idea to include variants of one algorithm/function by specifying different tuning parameters for different base learners.  There is an examples of how to create new variants of the wrapper functions in the `create_h2o_wrappers.R` script.
- The wrapper functions must have unique names.

## Metalearning
- Historically, techniques like [non-negative least squares (NNLS)](https://en.wikipedia.org/wiki/Non-negative_least_squares) have been used to find the optimal weighted combination of the base learners.
- We allow the user to specify any learner wrapper to define a metalearner, and we can use the `SL.nnls` function (in the `SuperLearner_wrappers.R` script) and the `SL.glm` (included in the [SuperLearner](http://cran.r-project.org/web/packages/SuperLearner/index.html) R package).
- We should consider widening the availability of metalearning functions to improve performance.  The ensembles that use an h2o-based metalearner have suboptimal performance, which will be addressed in the future.  


## Known Bugs
- This package is incompatible with R 3.0.0-3.1.0 due to a [parser bug](https://bugs.r-project.org/bugzilla3/show_bug.cgi?id=15753) in R.  Upgrade to R 3.1.1 or greater to resolve the issue.  It may work on earlier versions of R but has not been tested.
- Sometimes while executing `h2o.ensemble`, the code hangs due to a communication issue with H2O.  You may see something like this.  To fix, restart R.
```
GET /Cloud.json HTTP/1.1
Host: localhost:54321
Accept: */*
```
- See the "Create Ensembles" section above for information relating to issues with using h2o-derived functions as the metalearner.  
- When using a `h2o.deeplearning` model as a base learner, it is not possible to reproduce ensemble model results exactly even when using the `seed` argument of `h2o.ensemble` is set.  This is due to the fact that `h2o.deeplearning` results are not reproducible when used on a machine with multiple cores.
- The multicore and snow cluster functionality is not working (see the `parallel` option of the `h2o.ensemble` function).  There appears to be a conflict with using the R parallel functionality in conjunction with the h2o parallel functionality.  By default, the h2o package will use all cores available, so even when the `h2o.ensemble` function is executed with the default `parallel = "seq"` option, you will be training in parallel.

