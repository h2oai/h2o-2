RUNNING H2O NODES IN HADOOP
===========================

Note: You may want to do all of this from the machine where you plan
to launch the hadoop jar job from.  Otherwise you will end up having
to copy files around.


GET H2O TREE FROM GIT
---------------------

git clone https://github.com/0xdata/h2o.git
git checkout --track origin/tomk-hadoop		<<<<< TEMPORARY until code merges back into master branch (post-full-regression-test)


BUILD CODE
----------

cd h2o
make
cd hadoop
make
cd ..


COPY BUILD OUTPUT TO HADOOP NODE
--------------------------------

Copy h2o/hadoop/target/h2o_hadoop.zip <to place where you intend to run hadoop command>


PREPARE JOB INPUT ON HADOOP NODE
--------------------------------

unzip h2o_hadoop.zip
cd h2o_hadoop

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


RUN JOB
-------

hadoop jar h2odriver_cdh4.jar water.hadoop.h2odriver [-jt <jobtracker:port>] -files flatfile.txt -libjars h2o.jar -mapperXmx 1g -nodes 1 -output hdfsOutputDirName

(Note: Make sure to use the h2odriver flavor for the correct version
       of Hadoop!  We recommend running the hadoop command from a
       machine in the hadoop cluster.)


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
