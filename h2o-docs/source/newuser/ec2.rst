.. _EC2:


H\ :sub:`2`\ O on EC2
=====================

The H\ :sub:`2`\ O platform is tested nightly in EC2
for both single-node and multi-node configurations.

For users who want to quickly give H\ :sub:`2`\ O a try, we provide
an Amazon Machine Image to serve as an easy starting point.

The README.txt file embedded below describes scripts that can be found
in the EC2 directory of the downloadable zip file
:ref:`GettingStartedFromaZipFile`, or in the EC2 directory of the
Github repository :ref:`QuickstartGit`.

Glossery of Parameters for Launching EC2 Instances
""""""""""""""""""""""""""""""""""""""""""""""""""

**AMI ID/Image ID**
  The ID of the AMI image to launch and run, only change this when switching between preferances between different AWS virtualization (HVM vs Paravirtualization).

**AWS Access Key ID**
  The access key can typically be found in the IAM console under Users and the 'Manage Access Keys' option and is one of the two access keys neccessary to sign programmatic requests made to AWS.

**AWS Secret Access Key**
  The secret access key can only be downloaded when first created and is one of the two access keys necessary to sign programmatic requests made to AWS.

**IAM Profile Name**
  The name of the IAM instance profile to associate to the instance. The user can change the permissions by assigning policies to the roles. Because now the instance is using the temporary rotating access keys granted by the IAM role, there's no need to distribute the main access key and private access key to each of the EC2 instance.
  Example: In order to restrict the user of the EC2 instances to only certain private buckets, add the policy:

::

  { 
   "Version": "2012-10-17",
   "Statement": [
     {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::<bucket name>"]
     }
   ]
  }
  
**IAM Profile Resource Name**
  The resource name of the IAM Instance profile to associate to the instance. This can be used instead of IAM Profile Name.

**Instance Type**
  The type of instances you want to run, refer to Amazon's EC2 Documentation
   for information on memory usage and pricing.

**Key Name**
  The name of the key pair with which to launch instances. Pass in the path to the pem file or change the value in the launch script.

**Security Group Name**
  The names of the EC2 security groups with which to associate the instance.


.. raw:: html

    <div style="margin-top:10px;">
      <iframe width=900 height=900 src="../bits/ec2/README.txt" frameborder="0" allowfullscreen></iframe>
    </div>
