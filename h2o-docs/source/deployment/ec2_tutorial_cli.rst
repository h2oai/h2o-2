.. _EC2_Tutorial_CLI:

Launch H\ :sub:`2`\ O from Command Line
=======================================
Launch scripts are available on our Github repository :ref:`QuickstartGit`

**Note**: This procedure applies to H2O only and is not applicable to H2O-Dev. 

**Step 1**

Prerequisite: Install boto python library. In order to run the scripts, you must have the boto library installed. References are available on boto and amazon's website:

   - `Python Boto Documentation <http://boto.readthedocs.org/en/latest/>`_
   - `Amazon AWS Text <http://www.amazon.com/Python-and-AWS-Cookbook-ebook/dp/B005ZTO0UW/ref=sr_1_1?ie=UTF8&qid=1379879111&sr=8-1&keywords=python+aws>`_

**Step 2**

 Edit the `h2o-cluster-launch-instances.py` launch script for parameter changes; refer to :ref:`ec2_glossary` for help.

::

  # Environment variables you MUST set (either here or by passing them in).
  # -----------------------------------------------------------------------
  #
  os.environ['AWS_ACCESS_KEY_ID'] = '...'
  os.environ['AWS_SECRET_ACCESS_KEY'] = '...'
  os.environ['AWS_SSH_PRIVATE_KEY_FILE'] = '/path/to/private_key.pem'

  # Launch EC2 instances with an IAM role
  # --------------------------------------
  # Change either but not both the IAM Profile Name.
  iam_profile_resource_name = None
  iam_profile_name = 'testing_role'
  ...
  # SSH key pair name.
  keyName = 'testing_key'
  securityGroupName = 'SecurityDisabled'
  ...
  numInstancesToLaunch = 2
  instanceType = 't1.micro'
  instanceNameRoot = 'amy_is_testing'
  ...
  regionName = 'us-east-1'
  amiId = 'ami-ed550784'

**Step 3**

Launch the EC2 instances using the H2O AMI by running `h2o-cluster-launch-instances.py`.

::

  $ python h2o-cluster-launch-instances.py
  Using boto version 2.27.0
  Launching 2 instances.
  Waiting for instance 1 of 2 ...
    .
    .
    instance 1 of 2 is up.
  Waiting for instance 2 of 2 ...
    instance 2 of 2 is up.

  Creating output files:  nodes-public nodes-private

  Instance 1 of 2
    Name:    amy_is_testing0
    PUBLIC:  ec2-54-164-161-125.compute-1.amazonaws.com
    PRIVATE: 172.31.21.154

  Instance 2 of 2
    Name:    amy_is_testing1
    PUBLIC:  ec2-54-164-161-149.compute-1.amazonaws.com
    PRIVATE: 172.31.21.155

  Sleeping for 60 seconds for ssh to be available...
  Testing ssh access ...

  Distributing flatfile...

**Step 4**

Download the latest build of H2O onto each instance using:
 - `./h2o-cluster-distribute-h2o.sh` 
  --OR--  
 - `./h2o-cluster-download-h2o.sh`. 
 
 Download is typically faster than distribute since the file is downloaded from S3.

::

  $ ./h2o-cluster-download-h2o.sh
  Fetching latest build number for branch master...
  Fetching full version number for build 1480...
  Downloading H2O version 2.7.0.1480 to cluster...
  Downloading h2o.jar to node 1: ec2-54-164-161-125.compute-1.amazonaws.com
  Downloading h2o.jar to node 2: ec2-54-164-161-149.compute-1.amazonaws.com
  Warning: Permanently added 'ec2-54-164-161-125.compute-1.amazonaws.com,54.164.161.125'
  (RSA) to the list of known hosts.
  Warning: Permanently added 'ec2-54-164-161-149.compute-1.amazonaws.com,54.164.161.149'
  (RSA) to the list of known hosts.
  Unzipping h2o.jar within node 1: ec2-54-164-161-125.compute-1.amazonaws.com
  Unzipping h2o.jar within node 2: ec2-54-164-161-149.compute-1.amazonaws.com
  Copying h2o.jar within node 1: ec2-54-164-161-125.compute-1.amazonaws.com
  Copying h2o.jar within node 2: ec2-54-164-161-149.compute-1.amazonaws.com
  Success.

**Step 5**

Distribute a flatfile.txt with all the private node IP addresses.

::

  $ ./h2o-cluster-distribute-flatfile.sh
  Copying flatfile to node 1: ec2-54-164-161-125.compute-1.amazonaws.com
  flatfile.txt                             100%   40     0.0KB/s   00:00
  Copying flatfile to node 2: ec2-54-164-161-149.compute-1.amazonaws.com
  flatfile.txt                             100%   40     0.0KB/s   00:00
  Success.

**Step 6**

(Optional) To import data from a private S3 bucket, give permission to each launched node. If the cluster was launched without an IAM profile and policy, then AWS credentials have to be distributed to each node as an aws_credentials.properties file using `./h2o-cluster-distribute-aws-credentials.sh`. If the cluster was launched with IAM profile, H2O detects the temporary credentials on the cluster.

::

  $ ./h2o-cluster-distribute-aws-credentials.sh
  Copying aws credential files to node 1: ec2-54-164-161-125.compute-1.amazonaws.com
  core-site.xml                              100%  500     0.5KB/s   00:00
  aws_credentials.properties                 100%   82     0.1KB/s   00:00
  Copying aws credential files to node 2: ec2-54-164-161-149.compute-1.amazonaws.com
  core-site.xml                              100%  500     0.0KB/s   00:17
  aws_credentials.properties                 100%   82     0.1KB/s   00:00
  Success.

**Step 7**

**Note**: Before launching H2O on an EC2 cluster, verify that ports `54321` and `54322` are both accessible by TCP and UDP. 

Start H2O by executing `./h2o-cluster-start-h2o.sh`.

::

  $ h2o-cluster-start-h2o.sh
  Starting on node 1: ec2-54-164-161-125.compute-1.amazonaws.com...
  JAVA_HOME is ./jdk1.7.0_40
  java version "1.7.0_40"
  Java(TM) SE Runtime Environment (build 1.7.0_40-b43)
  Java HotSpot(TM) 64-Bit Server VM (build 24.0-b56, mixed mode)
  01:55:18.438 main      INFO WATER: ----- H2O started -----
  01:55:18.632 main      INFO WATER: Build git branch: master
  01:55:18.633 main      INFO WATER: Build git hash: 1fbeb98671c73d4e2a61fc3defecb6bd1646c4d5
  01:55:18.633 main      INFO WATER: Build git describe: nn-2-9356-g1fbeb98
  01:55:18.634 main      INFO WATER: Build project version: 2.7.0.1480
  01:55:18.634 main      INFO WATER: Built by: 'jenkins'
  01:55:18.635 main      INFO WATER: Built on: 'Thu Aug 21 23:51:30 PDT 2014'
  01:55:18.635 main      INFO WATER: Java availableProcessors: 1
  01:55:18.649 main      INFO WATER: Java heap totalMemory: 0.01 gb
  01:55:18.649 main      INFO WATER: Java heap maxMemory: 0.14 gb
  01:55:18.650 main      INFO WATER: Java version: Java 1.7.0_40 (from Oracle Corporation)
  01:55:18.651 main      INFO WATER: OS   version: Linux 2.6.32-358.14.1.el6.x86_64 (amd64)
  01:55:18.959 main      INFO WATER: Machine physical memory: 0.58 gb
  Starting on node 2: ec2-54-164-161-149.compute-1.amazonaws.com...
  JAVA_HOME is ./jdk1.7.0_40
  java version "1.7.0_40"
  Java(TM) SE Runtime Environment (build 1.7.0_40-b43)
  Java HotSpot(TM) 64-Bit Server VM (build 24.0-b56, mixed mode)
  01:55:21.983 main      INFO WATER: ----- H2O started -----
  01:55:22.067 main      INFO WATER: Build git branch: master
  01:55:22.068 main      INFO WATER: Build git hash: 1fbeb98671c73d4e2a61fc3defecb6bd1646c4d5
  01:55:22.068 main      INFO WATER: Build git describe: nn-2-9356-g1fbeb98
  01:55:22.069 main      INFO WATER: Build project version: 2.7.0.1480
  01:55:22.069 main      INFO WATER: Built by: 'jenkins'
  01:55:22.069 main      INFO WATER: Built on: 'Thu Aug 21 23:51:30 PDT 2014'
  01:55:22.070 main      INFO WATER: Java availableProcessors: 1
  01:55:22.082 main      INFO WATER: Java heap totalMemory: 0.01 gb
  01:55:22.082 main      INFO WATER: Java heap maxMemory: 0.14 gb
  01:55:22.083 main      INFO WATER: Java version: Java 1.7.0_40 (from Oracle Corporation)
  01:55:22.084 main      INFO WATER: OS   version: Linux 2.6.32-358.14.1.el6.x86_64 (amd64)
  01:55:22.695 main      INFO WATER: Machine physical memory: 0.58 gb
  Success.
  
""""  