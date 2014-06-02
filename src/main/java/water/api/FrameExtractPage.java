package water.api;

import hex.NFoldFrameExtractor;
import water.Func;
import water.H2O;
import water.fvec.Frame;

public class FrameExtractPage extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Data frame", required = true, filter = Default.class)
  public Frame source;

  @API(help = "N-fold split", required = true, filter = Default.class)
  public int nfolds;

  @API(help = "Split to extract", required = true, filter = Default.class)
  public int afold;

  @Override protected void execImpl() {
    NFoldFrameExtractor extractor = new NFoldFrameExtractor(source, nfolds, afold, null, null);
    H2O.submitTask(extractor);

    Frame[] splits = extractor.getResult();
  }

}
