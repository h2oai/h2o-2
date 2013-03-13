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

  private final TutorWorkflow _wf;
  private final static String[][] TUTORIAL_STEPS = new String[][]{
    /*               Title     Short Summary     File containing step description */
    new String[] { "Step 1", "Introduction", "/tutorials/kmeans/step1.html" },
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
