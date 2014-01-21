
Model
=====


For detailed information on the specifications of each model, please
see Data Science. Tutorials for each algorithm are available in Tutorial
Documentation. 

In general, a model of the user's choosing can be specified either by
finding the list of algorithms at the top of the **Inspect** page when
data are parsed or by selecting the appropriate model from the drop
down menu Model. 

Each model requires that the user provide a .hex key associated with a
data set. Users can often begin typing the name of the original data
source, and select the appropriate .hex key from the auto fill menu
that appears. Users can also find .hex keys for data sets by selecting
View All from the Data drop down menu, or for all H\ :sub:`2`\ O actions by
selecting Jobs from the Admin drop down menu. 

If a large data set is used in the training and testing of a model,
H\ :sub:`2`\ O's capabilities can be bounded by the amount of memory available on
the machine. To utilize H\ :sub:`2`\ O's full capability, the amount of memory
available should be about 4 times the file size of the data set, but
not more than the machine's total available memory. For instructions
on how to change the amount of memory allocated to H\ :sub:`2`\ O see the Quick
Start Documentation. Advanced users should run H\ :sub:`2`\ O on a cloud
computing resource or server. 

Grid Search Models
-------------------

GLM, K-Means, and GBM all offer Grid Search Models. Each Grid Search
modeling option allows users to generate multiple models
simultaneously, and compare model criteria directly, rather than
separately building models one at a time. Users can specify multiple
model configurations by entering different values of tuning parameters
separated by coma (for example: 2, 4, 8). For example, to specify three
different values of K in a K-means grid search in the K field enter
5,7,9 which will produce models for K=5, K=7, and K=9
respectively. When multiple levels are specified for many tuning
parameters grid search returns one model for each unique
combination. For example, in K-Means grid search specifying 3 values
of K, and 4 values of Max Iterations, a total of 12 models will be
returned one for each combination of K and Max Iterations.

Grid search results return a table showing the combination of tuning
parameters used for each model and basic model evaluation information,
as well as a link to each model. Users can access the details of each
model by clicking on the links in the table.


