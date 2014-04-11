package water.api;

import java.util.Arrays;

import hex.FrameSplitter;
import water.*;
import water.fvec.Frame;
import water.util.Utils;

/** Small utility page to split frame
 * into n-parts parts based on given ratios.
 *
 * <p>User specifies n-split ratios, which expose parts of resulting
 * datasets and produces (n+1)-datasets based on random selection of rows
 * from original dataset.</p>
 *
 * <p>Keep original chunk distribution.</p>
 *
 * @see FrameSplitter
 */
public class FrameSplitPage extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Data frame", required = true, filter = Default.class)
  public Frame source;

  @API(help = "Split ratio - can be an array of split ratios", required = true, filter = Default.class)
  public float[] ratios = new float[] {0.75f}; // n-values => n+1 output datasets

  @API(help = "Keys for each split partition.")
  public Key[] split_keys;

  @API(help = "Holds a number of rows per each output partition.")
  public long[] split_rows;

  @API(help = "Holds a number of split ratios per partition.")
  public float[] split_ratios;

  // Check parameters
  @Override protected void init() throws IllegalArgumentException {
    super.init();
    /* Check input parameters */
    float sum = 0;
    for (int i=0; i<ratios.length; i++) {
      if (!(ratios[i] > 0 && ratios[i] < 1)) throw new IllegalArgumentException("Split ration has to be in (0,1) interval!");
      sum += ratios[i];
    }
    if (sum>1) throw new IllegalArgumentException("Sum of split ratios has to be less or equal to 1!");
  }

  // Run the function
  @Override protected void execImpl() {
    FrameSplitter fs = new FrameSplitter(source, ratios, 42);
    H2O.submitTask(fs).join();

    Frame[] splits = fs.getResult();

    split_keys = new Key[splits.length];
    split_rows = new long[splits.length];
    float rsum = Utils.sum(ratios);
    split_ratios = Arrays.copyOf(ratios, splits.length);
    split_ratios[splits.length-1] = 1f-rsum;
    long sum = 0;
    for(int i=0; i<splits.length; i++) {
      sum += splits[i].numRows();
      split_keys[i] = splits[i]._key;
      split_rows[i] = splits[i].numRows();
    }
    assert sum == source.numRows() : "Frame split produced wrong number of rows: nrows(source) != sum(nrows(splits))";
  }

  @Override public boolean toHTML(StringBuilder sb) {
    int nsplits = split_keys.length;
    String [] headers = new String[nsplits+1];
    headers[0] = "";
    for(int i=0; i<nsplits; i++) headers[i+1] = "Split #"+i;
    DocGen.HTML.arrayHead(sb, headers);
    // Key table row
    sb.append("<tr><td>").append(DocGen.HTML.bold("Keys")).append("</td>");
    for (int i=0; i<nsplits; i++) {
      Key k = split_keys[i];
      sb.append("<td>").append(Inspect2.link(k.toString(), k)).append("</td>");
    }
    sb.append("</tr>");
    // Number of rows row
    sb.append("<tr><td>").append(DocGen.HTML.bold("Rows")).append("</td>");
    for (int i=0; i<nsplits; i++) {
      long r = split_rows[i];
      sb.append("<td>").append(String.format("%,d", r)).append("</td>");
    }
    sb.append("</tr>");
    // Split ratios
    sb.append("<tr><td>").append(DocGen.HTML.bold("Ratios")).append("</td>");
    for (int i=0; i<nsplits; i++) {
      float r = 100*split_ratios[i];
      sb.append("<td>").append(String.format("%.2f %%", r)).append("</td>");
    }
    sb.append("</tr>");
    DocGen.HTML.arrayTail(sb);
    return true;
  }

}
