.. _MacHadoop:


Setting up an H\ :sub:`2`\ O Hadoop cluster on a Mac
=====================================================


.. note::

	The following instructions should work on any reasonably modern version of OS X (10.6 and up), but were only tested on OS X 10.9.

Installing H\ :sub:`2`\ O on a Mac
------------------------------------

Prerequisites
--------------

1. Install Xcode from the `Apple store <https://itunes.apple.com/us/app/xcode/id497799835>`_

2. Download and install Java 1.7 from `Oracle's website
   <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_
   **NOTE:** DO NOT USE JAVA 1.8. 

3. Download and install R from `CRAN <http://cran.r-project.org/bin/macosx/>`_

.. warning::

		Homebrew might cause conflicts if you already have MacPorts enabled.
		Proceed with the next step at your own risk or use MacPorts instead of Homebrew.

4. Install the Homebrew package manager by issuing the following commands in a terminal

 ::
		
		$ ruby -e "$(curl -fsSL https://raw.github.com/mxcl/homebrew/go/install)"
		$ brew doctor
		$ brew update

""""""""""""""""""

Optional Dependencies
----------------------

1. Optional: Install sphinx (to build the documentation)

  ::

		$ sudo easy_install sphinx
		$ sudo easy_install sphinxcontrib-fulltoc

2. Optional: Download and install `LaTex for Mac <http://www.tug.org/mactex/index.html>`_

3. Optional: Install PDFUnite (to build some PDFs)

 ::
	
		$ brew install poppler
		
		
""""""""""""""""

Building H\ :sub:`2`\ O From Github
-------------------------------------------

1. Get H2O from `Github <https://github.com/h2oai/h2o>`_.
2. Follow the instructions in :ref:`QuickstartGit`.


""""""""""""

Installing Hadoop on a Mac
----------------------------

1. Install Hadoop via Homebrew:

 ::
	
		$ brew install hadoop

2. Optional - Give yourself permission to write to /usr/local/{include,lib,etc} (or use sudo to launch Hadoop):

 ::

    $ sudo chmod -R a+w /usr/local/{include,lib,etc}

3. Configure Hadoop (modify the file paths or version number if applicable): 

 Note:
 In Hadoop 1.x these files are found in, e.g., ``/usr/local/Cellar/hadoop/1.2.1/libexec/conf/``.
 In Hadoop 2.x these files are found in, e.g., ``/usr/local/Cellar/hadoop/2.2.0/libexec/etc/hadoop/``.

 Modify ``core-site.xml`` to contain the following:

 ::

		<configuration>
			<property>
				<name>fs.default.name</name>
				<value>hdfs://localhost:8020</value>
			</property>
		</configuration>

 Modify ``mapred-site.xml`` to contain the following (NOTE: you may need to create the file from mapred-site.xml.template):

 ::

	<configuration>
		<property>
			<name>mapred.job.tracker</name>
			<value>localhost:9001</value>
		</property>
		<property>
			<name>mapred.tasktracker.map.tasks.maximum</name>
			<value>5</value>
		</property>
	</configuration>
	
 Modify ``hdfs-site.xml`` to contain the following:

 ::

	<configuration>
		<property>
			<name>dfs.replication</name>
			<value>1</value>
		</property>
	</configuration>

4. Optional: Enable password-less SSH from localhost to localhost for convenience.  

 First, enable remote login in the system sharing control panel, and then enter:

 ::

		$ brew install ssh-copy-id
		$ ssh-keygen
		$ ssh-copy-id -i ~/.ssh/id_rsa.pub localhost

5. Start Hadoop MapReduce services, e.g.:

 ::

		$ /usr/local/Cellar/hadoop/1.2.1/bin/start-all.sh

or

 ::

		$ /usr/local/Cellar/hadoop/2.2.0/sbin/start-dfs.sh
		$ /usr/local/Cellar/hadoop/2.2.0/sbin/start-yarn.sh

6. Verify that Hadoop is running by checking the output of ``jps`` (look for NameNode, DataNode, JobTracker, TaskTracker)

 ::

    $ jps
		81829 JobTracker
		81556 NameNode
		81756 SecondaryNameNode
		9382 Jps
		81655 DataNode
		81928 TaskTracker

7. Format HDFS and exit safe mode.
	
 ::
		
	$ hadoop namenode -format
	$ hadoop dfsadmin -safemode leave

""""""""""""""

Launching H\ :sub:`2`\ O on Hadoop
------------------------------------

1. Launch a five-node H2O Hadoop cluster from the h2o directory (assuming you have enough free memory [>5GB])

 ::
		
		$ hadoop jar target/hadoop/h2odriver_cdh4.jar water.hadoop.h2odriver \
						 -libjars target/h2o.jar -mapperXmx 1g -nodes 5 -output out

2. To run H2O, point your web browser to the `HTTP URL http://localhost:54321 <http://localhost:54321>`_.  

3. Optional: Delete the output file after shutting down H2O.

 ::
		
		$ hadoop fs -rmr out
		
		
""""""""""""""		
