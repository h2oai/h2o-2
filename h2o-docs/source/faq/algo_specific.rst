.. _Algo_Specific:


Algorithm Issues
================

**Random Forest : What is the difference between the Distributed Random Forest algorithm and SpeedDRF?**

SpeeDRF is H\ :sub:`2`\ O's "fast" implementation of Random Forest and is the default setting when running random forest in R. It will require all the data to sit on each node in the cluster.

**Random Forest : How is the validation set used for tree models?**

The training set is used to build the requested number of trees after which the validation set is, if specified, used to generate the confusion matrix and vector of MSE by tree. When no validation set is specified in the build process of the model, the training set is used for validation.

**Random Forest : How many trees are used in the prediction step?**

In the current implementation, all the trees in the model are used in the prediction step.

**Random Forest : When the model is running it prints the MSE.  Is this the MSE of the validation set?**

The MSE being given is on the validation set if one was provided, and on the test set otherwise.

**Random Forest : What exactly does the “Max Depth” parameter control?  What is the relationship between Max Depth and the number of interior nodes?**

In general, the max depth is log\ :sub:`2`\ of the number of interior nodes.  Under some circumstances a tree can be unbalanced, changing this ratio.

