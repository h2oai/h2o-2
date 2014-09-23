
Score: POJO
===========

One of value add of using H\ :sub:`2`\ O is the ability to code in any front end API but have the model exportable as
a POJO (Plain Old Java Object). This allows the user the flexibility to take the model outside of H\ :sub:`2`\ O either
to run standalone or integrating the Java Object into a platform like Hadoop's Storm. The walkthrough below will detail
the steps required via the command line to export a model object and use it to score using a sample class object.

The working example or unit test for scoring using the Java code is available on `github <https://github.com/0xdata/h2o/blob/1516535e6c9358667369074a17a4f25821b281e2/R/tests/Utils/shared_javapredict_GBM.R>`_.

Walk-through
""""""""""""

**Step 1**

For a H\ :sub:`2`\ O instance sitting on localhost:54321 by default and GBM model with 50 trees that you want to
export, run the command to grab the h2o-model jar file as well as the Java code for the example model GBM_a2647515ded07d5b710c82015a6842a9.
It is recommended to create a new directory for each model.

::

  $ mkdir GBM_a2647515ded07d5b710c82015a6842a9
  $ cd GBM_a2647515ded07d5b710c82015a6842a9
  $ curl http://localhost:54321/h2o-model.jar > h2o-model.jar
  $ curl http://localhost:54321/2/GBMModelView.java?_modelKey=GBM_a2647515ded07d5b710c82015a6842a9 >
  GBM_a2647515ded07d5b710c82015a6842a9.java


**Step 2**

Download from git the `PredictCSV <https://github.com/0xdata/h2o/blob/master/R/tests/testdir_javapredict/PredictCSV.java>`_  class object that
will be used to compile the model object. The user can certainly write their own script; the one available in git is working example that is
tested on all the builds at 0xdata. It will take four arguments:

    *- -header*
        |   specify argument if the input data set has headers

    *- -model*
        |   model key name used to score on the input file

    *- -input*
        |   the input data that will be scored

    *- -output*
        |   the resulting output csv file with all the s scores for each entry of the input data


**Step 3**

Next set up a java instance to compile model object with PredictCSV.java which should generate in this case over 50 trees class objects.

    ::

      $ javac -cp h2o-model.jar -J-Xmx2g -J-XX:MaxPermSize=256m PredictCSV.java
      GBM_a2647515ded07d5b710c82015a6842a9.java


**Step 4**

Finally feed in testing data to be scored by running the following command:

    ::

      $ java -ea -cp h2o-model.jar -Xmx4g -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=256m
      PredictCSV.java --header --model GBM_a2647515ded07d5b710c82015a6842a9 --input iris_test.csv
      --output out_pojo.csv

More generic sample command:

    ::

      $ java -ea -cp h2o-model.jar -Xmx4g -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=256m
      PredictCSV.java --header --model <model key> --input <path to input data>
      --output <path to output csv>
