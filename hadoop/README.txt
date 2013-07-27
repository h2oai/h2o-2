
RUNNING H2O NODES IN HADOOP
===========================

Note: You may want to do all of this from the machine where you plan
to launch the hadoop jar job from.  Otherwise you will end up having
to copy files around.

(If you grabbed a prebuilt h2o_hadoop.zip file, copy it to a hadoop 
machine and skip to the PREPARE section below.)


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

Create flatfile.txt.

(Note: The flat file must contain the list of possible IP addresses an
       H2O node (i.e. mapper) may be scheduled on.  One IP address
       per line.)

Here is an example flatfile.txt:
$ cat flatfile.txt 
192.168.1.150
192.168.1.151
192.168.1.152
192.168.1.153
192.168.1.154
192.168.1.155


For your convenience, we have included a tool to help you genearate
a flatfile.  This is only meant to assist you, and may encounter
Java exceptions if DNS and DHCP are not fully configured.
This generator tool is still experimental, please double check the 
output yourself before relying on it.

$ hadoop jar hadoop/h2odriver_cdh4.jar water.hadoop.gen_flatfile -jt <jobtracker:port> > flatfile.txt

(Note: Make sure to use the h2odriver flavor for the correct version
       of hadoop!  We recommend running the hadoop command from a
       machine in the hadoop cluster.)

(Note: Port 8021 is the default jobtracker port for Cloudera.
       Port 9001 is the default jobtracker port for MapR.)


RUN JOB
-------

$ hadoop jar hadoop/h2odriver_cdh4.jar water.hadoop.h2odriver [-jt <jobtracker:port>] -files flatfile.txt -libjars h2o.jar -mapperXmx 1g -nodes 1 -output hdfsOutputDirName

(Note: -nodes refers to H2O nodes.  This may be less than or equal to
       the number of hadoop machines running TaskTrackers where hadoop 
       mapreduce Tasks may land.)

(Note: Make sure to use the h2odriver flavor for the correct version
       of hadoop!  We recommend running the hadoop command from a
       machine in the hadoop cluster.)

(Note: Port 8021 is the default jobtracker port for Cloudera.
       Port 9001 is the default jobtracker port for MapR.)


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

