.. _Hadoop_Tutorial:

Running H\ :sub:`2`\ O on Hadoop
================================

The following tutorial will walk the user through the download or build of H\ :sub:`2`\ O and the parameters involved in launching H\ :sub:`2`\ O from the command line.


1. Download the latest H\ :sub:`2`\ O release:

::

  $ wget http://h2o-release.s3.amazonaws.com/h2o/SUBST_RELEASE_NAME/SUBST_BUILD_NUMBER/h2o-SUBST_PROJECT_VERSION.zip


2. Prepare the job input on the Hadoop Node by unzipping the build file and changing to the directory with the Hadoop and H\ :sub:`2`\ O's driver jar files.

::

  $ unzip h2o-SUBST_PROJECT_VERSION.zip
  $ cd h2o-SUBST_PROJECT_VERSION/hadoop



3. To launch H\ :sub:`2`\ O nodes and form a cluster on the Hadoop cluster, run:

::

  $ hadoop jar h2odriver_hdp2.1.jar water.hadoop.h2odriver -libjars ../h2o.jar -mapperXmx 1g -nodes 1 -output hdfsOutputDirName

- For each major release of each distribution of hadoop, there is a driver jar file that the user will need to launch H2O with. Currently available driver jar files in each build of H2O includes `h2odriver_cdh5.jar`, `h2odriver_hdp2.1.jar`, and `mapr2.1.3.jar`.

- The above command launches exactly one 1g node of H2O, it is recommended you launch the cluster with 4 times the memory of your data file.

- *mapperXmx* is the mapper size or the amount of memory allocated to each node.

- *nodes* is the number of nodes requested to form the cluster.

- *output* is the name of the directory created each time a H\ :sub:'2'\O cloud is created so it is necessary for the name to be unique each time it is launched.

4. To monitor your job, direct your web browser to your standard job tracker Web UI.
To access H\ :sub:`2`\ O's Web UI, direct your web browser to one of the launched instances. If you are unsure where your JVM is launched,
review the output from your command after the nodes has clouded up and formed a cluster. Any of the nodes' IP addresses will work as there is no master node.

.. image:: hadoop_cmd_output.png
    :width: 100 %
