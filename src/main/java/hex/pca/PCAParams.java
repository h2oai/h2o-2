package hex.pca;

import water.Iced;
import water.api.DocGen;
import water.api.Request.API;

public class PCAParams extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "feature names")
  String[] names;

  @API(help = "maximum number of principal components")
  public int max_pc;

  @API(help = "tolerance")
  public double tolerance;

  @API(help = "standardize")
  public boolean standardize = true;

  public PCAParams(String[] feat, boolean std) {
    names = feat;
    max_pc = 10000;
    tolerance = 0;
    standardize = std;
  }

  public PCAParams(String[] feat, double tol, boolean std) {
    names = feat;
    max_pc = 10000;
    tolerance = tol;
    standardize = std;
  }

  public PCAParams(String[] feat, int max, double tol, boolean std) {
    names = feat;
    max_pc = max;
    tolerance = tol;
    standardize = std;
  }
}
