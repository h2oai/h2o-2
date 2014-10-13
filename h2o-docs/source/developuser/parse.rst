Parse Overview
==============

Since HDFS is the most commonly used format, this example illustrates that data
source.  However, H2O does support other sources of data.

The parse process typically moves data twice during
ingestion.

The first movement of the data occurs when the data is read from the disk
(an f-chunk in the diagram below) and copied across the network to the
H2O node requesting the specified data from the
filesystem.

.. image:: PngGen/pictures/DataIngestion.png
   :width: 90 %

|
|

The data is moved a second time from the H2O node, where the raw
data is parsed, to the H2O node, where the compressed data will reside
in a Fluid Vector chunk (a p-chunk in the diagram below).

.. image:: PngGen/pictures/Parse.png
   :width: 90 %

|
|
