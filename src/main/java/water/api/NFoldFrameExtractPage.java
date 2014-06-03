package water.api;

import hex.NFoldFrameExtractor;
import water.*;
import water.fvec.Frame;
import water.util.Utils;

public class NFoldFrameExtractPage extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Data frame", required = true, filter = Default.class)
  public Frame source;

  @API(help = "N-fold split", required = true, filter = Default.class, lmin=0)
  public int nfolds = 10;

  @API(help = "Split to extract", required = true, filter = Default.class, lmin=0)
  public int afold;

  @API(help = "Keys for each split partition.")
  public Key[] split_keys;

  @API(help = "Holds a number of rows per each output partition.")
  public long[] split_rows;

  @Override protected void init() throws IllegalArgumentException {
    super.init();
    if (nfolds > source.numRows()) throw new IllegalArgumentException("Cannot provide more folds than number of rows in dataset!");
    if (afold >= nfolds) throw new IllegalArgumentException("Request fold ("+afold+") is greater than number of folds ("+nfolds+")!");
  }

  @Override protected void execImpl() {
    NFoldFrameExtractor extractor = new NFoldFrameExtractor(source, nfolds, afold, null, null);
    H2O.submitTask(extractor);

    Frame[] splits = extractor.getResult();
    split_keys = new Key [splits.length];
    split_rows = new long[splits.length];

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
    String [] headers = new String[nsplits+2];
    headers[0] = "";
    for(int i=0; i<nsplits; i++) headers[i+1] = "Split #"+i;
    headers[nsplits+1] = "Total";
    DocGen.HTML.arrayHead(sb, headers);
    // Key table row
    sb.append("<tr><td>").append(DocGen.HTML.bold("Keys")).append("</td>");
    for (int i=0; i<nsplits; i++) {
      Key k = split_keys[i];
      sb.append("<td>").append(Inspect2.link(k)).append("</td>");
    }
    sb.append("<td>").append(Inspect2.link(source._key)).append("</td>");
    sb.append("</tr>");
    // Number of rows row
    sb.append("<tr><td>").append(DocGen.HTML.bold("Rows")).append("</td>");
    for (int i=0; i<nsplits; i++) {
      long r = split_rows[i];
      sb.append("<td>").append(String.format("%,d", r)).append("</td>");
    }
    sb.append("<td>").append(String.format("%,d", Utils.sum(split_rows))).append("</td>");
    sb.append("</tr>");
    DocGen.HTML.arrayTail(sb);
    return true;
  }

}
