
This directory contains scripts to help launch an H2O cluster in EC2.
You must install the boto python library.

http://boto.readthedocs.org/en/latest/
http://www.amazon.com/Python-and-AWS-Cookbook-ebook/dp/B005ZTO0UW/ref=sr_1_1?ie=UTF8&qid=1379879111&sr=8-1&keywords=python+aws


[ STEP 0:  Install python and boto, if necessary. ]


STEP 1:  Build a cluster
------------------------

Note:  Run this from a host that can access the nodes via public DNS name.

Edit h2o-cluster-launch-instances.py to suit your specific environment.
At a minimum, you need to specify an ssh key name and a security group name.

% h2o-cluster-launch-instances.py
% h2o-cluster-distribute-flatfile.sh
% h2o-cluster-distribute-h2o.sh


STEP 2:  Start H2O
------------------

% h2o-cluster-start-h2o.sh
(wait 60 seconds)


STEP 3:  Point your browser to H2O
----------------------------------

Point your web browser to 
    http://<any one of the public DNS node addresses>:54321


Stopping and restarting H2O
---------------------------
% h2o-cluster-stop-h2o.sh
% h2o-cluster-start-h2o.sh


Control files (generated when starting the cluster and/or H2O)
--------------------------------------------------------------

    nodes-public
        A list of H2O nodes by public DNS name.

    nodes-private
        A list of H2O nodes by private AWS IP address.

    flatfile.txt
        A list of H2O nodes by (private) IP address and port.


Stopping/Terminating the cluster
--------------------------------

Go to your Amazon AWS console and do the operation manually.



Work in progress...
