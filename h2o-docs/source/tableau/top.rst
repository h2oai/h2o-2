.. _Tableau:


Tableau on H\ :sub:`2`\ O
=========================

Tableau is the frontend visual organizer that utilizes all the available statistic tools from open source R and H2O.
Tableau will connect to R via a socket server using a library package already built for R. The H2O client package
available for installation allows R to connect and communicate to H2O via a REST API. So by connecting Tableau to R,
Tableau essentially can launch or initiate H2O and run any of the features already available for R which is heavily supported.

In H\ :sub:`2`\ O's `github <https://github.com/0xdata/h2o/tree/master/Tableau>`_ repository there is two Tableau demo workbook.
One for Tableau 8.1 and one for the newer version of Tableau 8.2, both of which currently only works on a Windows version of Tableau.
There is currently an issue running calculating fields on a Mac that has been submitted as a `question <http://community.tableausoftware.com/thread/148067>`_
on Tableau's forum. Generally it seems like the issue is how states are saved or not saved between a Windows and Mac running on Rserve.

For a walkthrough on creating a dashboard for the airlines data set, choose one of the two versions of Tableau a tutorial is currently
available for.

.. toctree::
   :maxdepth: 1

   tableau8.1
   tableau8.2