rel-lagrange-10. 10.16.2014

Deep Learning changes / updates:
===================================

Added support for ADADELTA adaptive learning rate that introduces an adaptive, time-dependent per-neuron learning rate.  Replaces 7 difficult to control parameters with 2 simple parameters that are usually not very influential in model accuracy.  Leads to fast convergence out of the box without requiring any user input.

Added support for load-balancing of datasets across all compute nodes for maximal training speed.  Useful for small datasets.

Added computation of variable importances.

Added computation of Top-K hit ratios for classifiers.

Added support for improved handling of imbalanced classes based on under/over sampling and probability corrections.

Added support for single-node training even when running in multi-node operation. Can be useful when the model is large and final convergence (e.g., after checkpoint restart) would otherwise be limited by network communication overhead.

Added support for replication of training data onto every compute node. Useful when data fits into memory of each node, as training speed can be increased and sampling from the same distribution on every iteration improves accuracy similar to bagging concepts for tree-based algorithms.

Added support to allow arbitrary number of training rows per MapReduce iteration for optimal control of network overhead vs compute.

Added support for checkpointing during model building. Especially useful for continuation of grid search models or interactive exploratory model building.

Improved training speed.  Obtain C++ performance for mat-vec operations.  Added support for sparse data shortcuts.  Improved performance of adaptive learning rate.

Added support for sparse weight matrices (experimental).

Added support for N-fold cross-validation of the model on training data.

Added deep auto-encoder with arbitrary (non-symmetrical) hidden layer network topology for unsupervised training. Very similar to regular supervised models, except that the response column is ignored and the output layer has the same dimension as the input layer (#features).  The model learns to reconstruct the original features, and can act as a denoising auto-encoder if combined with (input) dropout or other regularization measures.

Added sparsity constraints to auto-encoder, with user-specifiable average activation value for hidden neurons (experimental). This can be useful if the number of hidden neurons is larger than the input features, but only a small intrinsic dimensionality of the learned features is required.

Added anomaly detection app based on deep auto-encoder: Requires an auto-encoder model and a matching dataset as input, returns the reconstruction error per row, which can be thresholded to find outliers.

Added POJO model export for standalone scoring.

Added binary model save/load to file, requires compatible version of H2O.
	
Added option to maintain all factor levels for categoricals for improved interpretation of feature importances. Note that N-1 factor levels are logically sufficient, but don’t result in features importances for all factor levels.

Added preservation of the model with the lowest training/validation error during model training, and optionally return it as the final model.  Useful for finding local minima in the optimization process near final convergence.
			
Improved handling of missing values: Rows with missing values are no longer skipped, but missing values are imputed with the mean value (per column) instead of being skipped.

Added R Vignette documentation with detailed explanation of the features, parameters and example R scripts for MNIST, including parameters for world-class model.  All features are available from R.



GLM
==============================================
+ Added strong rules option to lambda_search:
  Strong rules provide a rule to filter out predictors which are most likely to be 0 for given lambda. 
  Strong rule result in major speed up of lambda -search (allow to compute more fine grained regularization path) and they allow us to handle datasets with too many predictors provided the result has limited number of nonzero coefficients. GLM with strong rules is limited by number of non-zero coefficients in the final model rather than number of potential predictors in the dataset.
  For more detail about strong rules: http://statweb.stanford.edu/~tibs/ftp/strong.pdf

+ Updated cross-validation for lambda-search to compute cross-validation for each lambda:
   Cross validation for lambda search now has separate model with full regularization path for each n-fold model.
   Computation of the models is distributed among the nodes in the cloud (i.e. 10 nodes can get upt to 10x speedup for 10-fold cross-validation) 

+ Improved accuracy:
   - line-search covers more cases when line-search is needed (in particular for linearly separable datasets 
   - now line-search is triggered if the final solution reached Infs in predictions and/or gradient)
   - improved accuracy of inner solver 
      		increased max # iterations for small number of predictors (max_iter now function of # predictors)
       		improved early bail out conditions to not too bail out too early in certain cases
 
+ Improved accuracy of AUC (and cross-validated AUC) 
   - use actual prediction points (sampled for each class seprately) instead of static grid of points  
