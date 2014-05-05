package water.api;

import hex.deeplearning.DeepLearning;

/**
 * Tutorial about deep learning.
 * @see DeepLearning
 */
public class TutorialDeepLearning extends TutorialWorkflow {

  private final transient TutorWorkflow _wf;
  private final static String[][] TUTORIAL_STEPS = new String[][]{
         /*               Title     Short Summary         File containing step description */
          new String[] { "Step 1", "Introduction"  ,     "/tutorials/deeplearning/step1.html" },
          new String[] { "Step 2", "Dataset inhale",     "/tutorials/deeplearning/step2.html" },
          new String[] { "Step 3", "Parsing the dataset",    "/tutorials/deeplearning/step3.html" },
          new String[] { "Step 4", "Inspecting the dataset", "/tutorials/deeplearning/step4.html" },
          new String[] { "Step 5", "Building the model",     "/tutorials/deeplearning/step5.html" },
          new String[] { "Step 6", "Inspecting the model",   "/tutorials/deeplearning/step6.html" },
          new String[] { "Step 7", "Predicting on a test set",   "/tutorials/deeplearning/step7.html" },
          new String[] { "Step 8", "Score the prediction",   "/tutorials/deeplearning/step8.html" },
  };

  public TutorialDeepLearning() {
    _wf = new TutorWorkflow("Deep Learning Tutorial");
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
