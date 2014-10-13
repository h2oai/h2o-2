Job/MRTask/FJTask Overview
==========================

A GLM job is broken down first into MRTask2 tasks, then into
Fork/Join (FJ) tasks.  FJ tasks are spread across the cluster in a
logarithmic tree fashion, with computation performed at the leaves and
results rolled up to the top.

.. image:: PngGen/pictures/GLMAlgoMem.png
   :width: 90 %

|
|
