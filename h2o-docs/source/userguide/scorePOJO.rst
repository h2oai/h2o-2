
Score: POJO
===========

H2O has the ability to code in any front-end API and export the model as
a POJO (Plain Old Java Object). This provide the flexibility to use the model outside of H2O, either to run as a standalone or by integrating the Java Object into a platform like Hadoop's Storm. The following walkthrough describes
the steps required to export a model object via the command line and to score it using a sample class object.

The working example or unit test for scoring using the Java code is available on `github <https://github.com/h2oai/h2o/blob/1516535e6c9358667369074a17a4f25821b281e2/R/tests/Utils/shared_javapredict_GBM.R>`_. 

Walk-through
""""""""""""

**Step 1**

To export an H2O instance sitting on localhost:54321 by default and a GBM model with 50 trees, run the following commands to grab the h2o-model jar file and the Java code for the example model GBM_a2647515ded07d5b710c82015a6842a9.
We recommend creating a new directory for each model.

::

  $ mkdir GBM_a2647515ded07d5b710c82015a6842a9
  $ cd GBM_a2647515ded07d5b710c82015a6842a9
  $ curl http://localhost:54321/h2o-model.jar > h2o-model.jar
  $ curl http://localhost:54321/2/GBMModelView.java?_modelKey=GBM_a2647515ded07d5b710c82015a6842a9 >
  GBM_a2647515ded07d5b710c82015a6842a9.java


**Step 2**

Download from git the `PredictCSV <https://github.com/h2oai/h2o/blob/master/R/tests/testdir_javapredict/PredictCSV.java>`_  class object that
will be used to compile the model object. You can write your own script; the one available in git is a working example that is tested on all the builds at h2o.ai. It uses four arguments:

    *- -header*
        |   specify if the input data set has headers

    *- -model*
        |   model key name used to score on the input file

    *- -input*
        |   the input data that will be scored

    *- -output*
        |   the resulting output csv file with all the s scores for each entry of the input data

**Note**: Make sure that both the PredictCSV.java object and the original dataset are located in the directory you created in step 1. 

**Step 3**

Next, set up a Java instance to compile the model object using PredictCSV.java, which should generate over 50 tree class objects.

    ::

      $ javac -cp h2o-model.jar -J-Xmx2g -J-XX:MaxPermSize=256m PredictCSV.java
      GBM_a2647515ded07d5b710c82015a6842a9.java


**Step 4**

Finally, submit the testing data for scoring by running the following command:

    ::

      $ java -ea -cp .:./h2o-model.jar -Xmx4g -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=256m
      PredictCSV --header --model GBM_a2647515ded07d5b710c82015a6842a9 --input iris_test.csv
      --output out_pojo.csv

 Generic command example:

    ::

      $ java -ea -cp .:./h2o-model.jar -Xmx4g -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=256m
      PredictCSV --header --model <model key> --input <path to input data>
      --output <path to output csv>
