
RUNNING H2O NODES IN HADOOP
===========================

Note: You may want to do all of this from the machine where you plan
to launch the Hadoop jar job from.  Otherwise you will end up having
to copy files around.

(If you grabbed a prebuilt h2o-*.zip file, copy it to a Hadoop machine
and skip to the PREPARE section below.)


GET H2O TREE FROM GIT
---------------------

$ git clone https://github.com/0xdata/h2o.git
$ cd h2o


BUILD CODE
----------

$ make


COPY BUILD OUTPUT TO HADOOP NODE
--------------------------------

Copy target/h2o-*.zip <to place where you intend to run hadoop command>


PREPARE JOB INPUT ON HADOOP NODE
--------------------------------

$ unzip h2o-*.zip
$ cd h2o-*
$ cd hadoop



RUN JOB
-------

$ hadoop jar h2odriver_hdp2.1.jar water.hadoop.h2odriver -libjars ../h2o.jar -mapperXmx 1g -nodes 1 -output hdfsOutputDirName

(Note: -nodes refers to H2O nodes.  This may be less than or equal to
       the number of hadoop machines running TaskTrackers where hadoop 
       mapreduce Tasks may land.)

(Note: Make sure to use the h2odriver flavor for the correct version
       of hadoop!  We recommend running the hadoop command from a
       machine in the hadoop cluster.)

(Note: Port 8021 is the default jobtracker port for Cloudera.
       Port 9001 is the default jobtracker port for MapR.)

(Note: H2O requires that ports are opened between Hadoop nodes. To
       ensure correct behavior it’s recommended that the range of 
       ports 54321-54421 be open for TCP and UDP communication. 


MONITOR JOB
-----------

Use standard job tracker web UI.  (http://<jobtrackerip>:50030)
Different distros sometimes have different job tracker Web UI ports.
The cloudera default is 50030.


SHUT DOWN THE CLUSTER
---------------------

Bring up H2O web UI:  http://<h2onode>:54321
Choose Admin->Shutdown

(Note: Alternately use the "hadoop job -kill" command.)


FOR MORE INFORMATION
--------------------

$ hadoop jar hadoop/h2odriver_cdh4.jar water.hadoop.h2odriver -help

