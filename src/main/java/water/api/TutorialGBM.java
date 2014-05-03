package water.api;

/**
 * Basic page introducing tutorial for GBM on Iris
 */
public class TutorialGBM extends TutorialWorkflow {

  private final transient TutorWorkflow _wf;
  private final static String[][] TUTORIAL_STEPS = new String[][]{
    /*               Title     Short Summary     File containing step description */
    new String[] { "Step 1", "Introduction", "/tutorials/gbm.iris/step1.html" },
    new String[] { "Step 2", "Dataset inhale", "/tutorials/gbm.iris/step2.html" },
    new String[] { "Step 3", "Parsing the dataset", "/tutorials/gbm.iris/step3.html" },
    new String[] { "Step 4", "Inspecting the dataset", "/tutorials/gbm.iris/step4.html" },
    new String[] { "Step 5", "Building the model", "/tutorials/gbm.iris/step5.html" },
    new String[] { "Step 6", "Inspecting the model", "/tutorials/gbm.iris/step6.html" },
    new String[] { "Step 7", "Predict on a test set", "/tutorials/gbm.iris/step7.html" },
    new String[] { "Step 8", "Scoring the prediction", "/tutorials/gbm.iris/step8.html" },
  };

  public TutorialGBM() {
    _wf = new TutorWorkflow("GBM Tutorial");
    int i = 1;
    for (String[] info : TUTORIAL_STEPS) {
      _wf.addStep(i++, new FileTutorStep(info));
    }
  }

  @Override
  protected TutorWorkflow getWorkflow() {
    return _wf;
  }
}
