.. _EC2_Build_AMI:

Launch H\ :sub:`2`\ O from AWS Console
======================================

Choose Operating System and Virtualization Type
"""""""""""""""""""""""""""""""""""""""""""""""

Choose the operating system you're comfortable running and the virtualization type of the prebuilt AMI on Amazon.
Keep in mind if your operating system is a Windows you will need to use a Hardware-assisted Virtual Machine (or HVM)
but working on a linux allows you to choose between Para-virtulaization (PV) and HVM. This option will at the very least
change the type of instances you can launch.

.. image:: ec2_system.png
    :width: 100 %

View more information about virtualization types at `Amazon <http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/virtualization_types.html>`_.

""""

Configure the Instance
""""""""""""""""""""""

#. Select the IAM role and policy to use to launch the instance. H2O's Lagrange release is able to detect the temporary access keys associated with the instance, so there's no need to copy your AWS credentials to the instances.

    .. image:: ec2_config.png
        :width: 100 %

#. When launching the instance, choose an accessible key pair.

    .. image:: ec2_key_pair.png
        :width: 50%

""""

[For Windows Users] Tunnel into Instance
""""""""""""""""""""""""""""""""""""""""

For Windows users without the capability to use ssh from their terminal, either download Cygwin or a Git Bash that has
the capability to run:

::

  ssh -i amy_account.pem ec2-user@54.165.25.98


Otherwise, download PuTTY and run through the following steps:

#. Start up PuTTY Key Generator.
#. Load your downloaded AWS pem key file (remember to change the type of file in the browser to all to see the file).
#. Save the private key as .ppk file.

    .. image:: ec2_putty_key.png
        :width: 60%

#. Start up the PuTTY client.
#. Under *Session*, enter the Host Name or IP Address. For Ubuntu users, the default is ubuntu@<ip-address>.  For other linux users, the default host name is ec2-user@<ip-address>.

    .. image:: ec2_putty_connect_1.png
        :width: 50%

#. Under *SSH > Auth*, browse for your private key file for authentication.

    .. image:: ec2_putty_connect_2.png
        :width: 50%

#. Open the new session and agree to cache the server's rsa2 key fingerprint to continue connecting.

    .. image:: ec2_putty_alert.png
        :width: 50%

""""

Download Java and H\ :sub:`2`\ O
""""""""""""""""""""""""""""""""

#. Download Java (jdk1.7+) if it is not already available on the instance.

#. To download H2O, run the wget command with the link to the zip file available on our website by copying the link associated with the download button:

    ::

      wget http://h2o-release.s3.amazonaws.com/h2o/SUBST_RELEASE_NAME/SUBST_BUILD_NUMBER/h2o-SUBST_PROJECT_VERSION.zip
      unzip h2o-SUBST_PROJECT_VERSION.zip
      cd h2o-SUBST_PROJECT_VERSION
      java -Xmx1g -jar h2o.jar


#. From your browser, navigate to <private_ip_address>:54321 or <public DNS>:54321 to use H2O's web interface.

""""