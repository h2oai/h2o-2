/**
 *
 */
package water.api;


/**
 * Basic page introducing tutorial for GLM on prostate dataset.
 *
 * @author michal
 *
 */
public class TutorialKMeans extends TutorialWorkflow {

  private final transient TutorWorkflow _wf;
  private final static String[][] TUTORIAL_STEPS = new String[][]{
    /*               Title     Short Summary     File containing step description */
    new String[] { "Step 1", "Introduction", "/tutorials/kmeans/step1.html" },
    new String[] { "Step 2", "Dataset inhale & parse", "/tutorials/kmeans/step2.html" },
    new String[] { "Step 3", "Running the algorithm", "/tutorials/kmeans/step3.html" },
  };

  public TutorialKMeans() {
    _wf = new TutorWorkflow("K-means");
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
