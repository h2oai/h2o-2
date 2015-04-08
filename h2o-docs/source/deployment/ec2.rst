.. _EC2:


H\ :sub:`2`\ O on EC2
=====================

The H\ :sub:`2`\ O platform is tested nightly in EC2
for both single-node and multi-node configurations.

For users who want to quickly give H\ :sub:`2`\ O a try, we provide
an Amazon Machine Image to serve as an easy starting point.

A README.txt file that describes the executable scripts can be found
in the EC2 directory of the downloadable zip file
:ref:`GettingStartedFromaZipFile`, or in the EC2 directory of the
Github repository :ref:`QuickstartGit` along with all the scripts used
to launch H\ :sub:`2`\ O.

**Note**: Before launching H2O on an EC2 cluster, verify that ports `54321` and `54322` are both accessible by TCP and UDP. 

.. toctree::
   :maxdepth: 1

   ec2_glossary
   ec2_tutorial_cli
   ec2_build_ami
