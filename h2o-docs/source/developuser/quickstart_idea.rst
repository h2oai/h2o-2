.. _Idea:

For IDEA Users (Github)
==========================

If you don't have a Software Development Kit (SDK) on your system, you will need to install one first. For more information about SDKs, refer the IntelliJ IDEA `website <https://www.jetbrains.com/idea/help/sdk.html>`_.

1. Create a git clone of the H2O repository.
   :ref:`QuickstartGit`
 
2. Open IDEA.
3. Click Import Project.

.. image:: idea/02ImportProject.png
   :width: 90 %

4. Choose the H2O directory and click OK.

.. image:: idea/03ChooseH2ODir.png
   :width: 90 %

5. Select the **Import project from external model** radio button and select Eclipse,  then click Next.

.. image:: idea/04ChooseEclipse.png
   :width: 90 %

6. Check the **Link created Intellij IDEA modules to Eclipse project files** checkbox (which is not selected by default) and click Next.

.. image:: idea/05ConfigureImport.png
   :width: 90 %

7. H2O should be selected by default; if not, select it.  If the "experiments" module is selected, uncheck it, then click Next.

.. image:: idea/06H2OSelected.png
   :width: 90 %

8. SDK 1.6 or 1.7 should selected by default.  If so, click Finish. If not, install an SDK and retry.  

.. image:: idea/07SelectJavaSK.png
   :width: 90 %

9. (Import from Eclipse) If prompted for Python configuration, click Cancel.

.. image:: idea/08CancelPython.png
   :width: 90 %

10. If prompted to Add Files to Git, click Cancel.

.. image:: idea/09CancelAddProjectFilesToGit.png
   :width: 90 %

11. In IntelliJ IDEA / Preferences (CMD-,) select **1.6** from the drop-down project bytecode version menu:

.. image:: idea/11SetProjectBytecodeVersion.png
   :width: 90 %

12. Select a sample Java Application and right click it, then select Run.

.. image:: idea/12SelectJavaApplicationToRun.png
   :width: 90 %

13. In certain versions of IntelliJ, you may need to set the Java heap size and re-run:

.. image:: idea/13SetJavaHeapSize.png
   :width: 90 %

14. See the output of a successful run.

.. image:: idea/14SuccessfulRunOutput.png
   :width: 90 %

15. You may connect to http://127.0.0.1:54321/ to use H2O interactively.

