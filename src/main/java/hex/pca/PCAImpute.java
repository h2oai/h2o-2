package hex.pca;

import water.Job.FrameJob;
import water.api.DocGen;
import water.fvec.Frame;

public class PCAImpute extends FrameJob {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "pca_imputation";

  @API(help = "Number of principal components to use", filter = Default.class, lmin = 1, lmax = 5000)
  int num_pc = 1;

  @API(help = "Threshold for convergence", filter = Default.class)
  double threshold = 1e-5;

  @API(help = "Maximum number of iterations", filter = Default.class, lmin = 1, lmax = 1000000)
  int max_iter = 50;

  @API(help = "Scale columns by their standard deviations", filter = Default.class)
  boolean scale = true;

  @Override protected void execImpl() {
    Frame fr = source;
    new Frame(destination_key,fr._names.clone(),fr.vecs().clone()).delete_and_lock(null).unlock(null);
  }

  @Override protected void init() {
    super.init();
    if(source != null && num_pc > source.vecs().length)
      throw new IllegalArgumentException("Argument 'num_pc' must be between 1 and " + source.vecs().length);
  }
}
