
How R Scripts Call H\ :sub:`2`\ O GLM
=====================================

The following diagram shows the different software layers involved
when a user runs an R program that starts a GLM on H2O.

The left side shows the steps that run the the R process and the
right side shows the steps that run in the H2O cloud.  The top layer
is the TCP/IP network code that enables the two processes to
communicate with each other.

The solid line shows an R->H2O request and the dashed line shows
the response for that request.

In the R program, the different components are: 

	* the R script itself
	* the H2O R package
	* dependent packages (RCurl, rjson)
	* the R core runtime

.. image:: PngGen/pictures/start_glm_from_r.png
   :width: 90 %

|
|

The following diagram shows the R program retrieving the resulting GLM
model.  (Not shown: the GLM model executing subtasks within
H2O and depositing the result into the K/V store or R
polling the /Jobs.json URL for the GLM model to complete.)

.. image:: PngGen/pictures/retrieve_glm_result_from_r.png
   :width: 90 %

|
|

An end-to-end sequence diagram of the same transaction is below (click
on the diagram to zoom).  This gives a different perspective of the R
and H2O interactions for the same GLM request and the resulting model.

.. image:: PngGen/uml/run_glm_from_r.png
   :width: 90 %

|
|

""""

How R Expressions are Sent to H\ :sub:`2`\ O for Evaluation
===========================================================

An H2O data frame is represented in R by an S4 object of class
H2OParsedData.  The S4 object has a @key slot which is a reference to
the big data object inside H2O.

The H2O R package overloads generic operations like 'summary' and '+'
with this new H2OParsedData class.  The R core parser makes callbacks
into the H2O R package, and these operations get sent to the H2O cloud over an
HTTP connection.

The H2O cloud performs the big data operation (for example, '+' on two columns
of a dataset imported into H2O) and returns a reference to the result.
This reference is stored in a new H2OParsedData S4 object inside R.
