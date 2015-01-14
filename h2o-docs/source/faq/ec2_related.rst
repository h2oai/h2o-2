.. _EC2_Related:


Amazon's EC2 FAQ
================

**How do you import data from a S3 bucket?**

If the S3 bucket is public then the user will be able to import using path s3://<bucket_name>. If the bucket is private, pass the user's AWS credentials to each of the nodes so each node has access to the private bucket.
Access will be granted when launching H\ :sub:`2`\ O using the command:

::

  java -jar h2o.jar --aws_credentials=ec2/AwsCredentials.properties

If the EC2 instances were launch with an IAM role, H\ :sub:`2`\ O will be able to detect the temporary credentials instead of having to pass a file with the user's access keys.


**Why can't I access the Web UI after launching H2O?**

#. Check to make sure that your EC2 Security Group have ports open for TCP / 54321 and TCP & UDP 54322.

#. If public DNS or IP address is available navigate to that address with the default port 54321:

::

  http://ec2-54-215-94-183.us-west-1.compute.amazonaws.com:54321
