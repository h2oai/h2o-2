package water.api;

import hex.NFoldFrameExtractor;
import water.*;
import water.fvec.Frame;

public class NFoldFrameExtractPage extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Data frame", required = true, filter = Default.class)
  public Frame source;

  @API(help = "N-fold split", required = true, filter = Default.class)
  public int nfolds = 10;

  @API(help = "Split to extract", required = true, filter = Default.class)
  public int afold;

  @API(help = "Keys for each split partition.")
  public Key[] split_keys;

  @API(help = "Holds a number of rows per each output partition.")
  public long[] split_rows;

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

}
