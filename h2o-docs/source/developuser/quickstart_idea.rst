.. _Idea:

For IDEA Users (Github)
==========================

1. Create a git clone of the H\ :sub:`2`\ O repository.
   :ref:`QuickstartGit`
 
2. Open IDEA.
3. Click Import Project.

.. image:: idea/02ImportProject.png
   :width: 90 %

4. Choose the H\ :sub:`2`\ O directory and click OK.

.. image:: idea/03ChooseH2ODir.png
   :width: 90 %

5. Choose Import project from external model.  Choose Eclipse.  Click Next.

.. image:: idea/04ChooseEclipse.png
   :width: 90 %

6. ENABLE LINK CREATED INTELLIJ IDEA MODULES TO ECLIPSE PROJECT FILES (this is not selected by default).  Click Next.

.. image:: idea/05ConfigureImport.png
   :width: 90 %

7. H\ :sub:`2`\ O should be selected by default.  Keep it selected.  If the "experiments" module is selected uncheck it.  Click Next.

.. image:: idea/06H2OSelected.png
   :width: 90 %

8. SDK 1.6 or 1.7 should selected by default.  If so click Finish.  If you don't have an SDK on your system you will need to install one first.

.. image:: idea/07SelectJavaSK.png
   :width: 90 %

9. (Import from Eclipse) If prompted for Python configuration stuff just click Cancel.

.. image:: idea/08CancelPython.png
   :width: 90 %

10. If prompted to Add Files to Git just click Cancel.

.. image:: idea/09CancelAddProjectFilesToGit.png
   :width: 90 %

11. In IntelliJ IDEA / Preferences (CMD-,) set the project bytecode version to 1.6:

.. image:: idea/11SetProjectBytecodeVersion.png
   :width: 90 %

12. Select a sample Java Application and right click on it.  Choose Run.

.. image:: idea/12SelectJavaApplicationToRun.png
   :width: 90 %

13. In certain versions of IntelliJ you may need to set the Java heap size and re-run:

.. image:: idea/13SetJavaHeapSize.png
   :width: 90 %

14. See the output of a successful run.

.. image:: idea/14SuccessfulRunOutput.png
   :width: 90 %

15. You may connect to http://127.0.0.1:54321/ to use H\ :sub:`2`\ O interactively.

