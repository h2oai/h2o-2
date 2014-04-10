package water.api;

import hex.FrameSplitter;
import water.Func;
import water.H2O;
import water.fvec.Frame;

/** Small utility function to split frame
 * into two parts based on given ratio.
 *
 * <p>User specifies n-split ratios, which expose parts of resulting
 * datasets and produces (n+1)-datasets based on random selection of rows
 * from original dataset.</p>
 *
 * <p>Keep original chunk distribution.</p>
 */
public class FrameSplitPage extends Func {

  @API(help = "Data frame", required = true, filter = Default.class)
  public Frame source;

  @API(help = "Split ratio - can be an array of split ratios", required = true, filter = Default.class)
  public float[] ratio; // n-values => n+1 output datasets

  // Check parameters
  @Override protected void init() throws IllegalArgumentException {
    super.init();
    /* Check input parameters */
    float sum = 0;
    for (int i=0; i<ratio.length; i++) {
      if (!(ratio[i] > 0 && ratio[i] < 1)) throw new IllegalArgumentException("Split ration has to be in (0,1) interval!");
      sum += ratio[i];
    }
    if (sum>1) throw new IllegalArgumentException("Sum of split ratios has to be less or equal to 1!");
  }

  // Run the function
  @Override protected void execImpl() {
    FrameSplitter fs = new FrameSplitter(source, ratio, 42);
    H2O.submitTask(fs).join();

    Frame[] splits = fs.getResult();
    long sum = 0;
    for(int i=0; i<splits.length; i++) {
      sum += splits[i].numRows();
      System.err.println(i + "-split has " + splits[i].numRows() + "rows ");
    }
    System.err.println("Input has: " + source.numRows());
    System.err.println("Sum of splits has: " + sum);
  }
}
