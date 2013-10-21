package hex.pca;

import water.Iced;
import water.api.DocGen;
import water.api.Request.API;

public class PCAParams extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "feature names")
  final String[] names;

  @API(help = "maximum number of principal components")
  final int max_pc;

  @API(help = "tolerance")
  final double tolerance;

  @API(help = "standardize")
  // final boolean standardize;
  final int standardize;

  public PCAParams(String[] feat, boolean std) {
    names = feat;
    max_pc = 10000;
    tolerance = 0;
    // standardize = std;
    standardize = std ? 1 : 0;
  }

  public PCAParams(String[] feat, double tol, boolean std) {
    names = feat;
    max_pc = 10000;
    tolerance = tol;
    // standardize = std;
    standardize = std ? 1 : 0;
  }

  public PCAParams(String[] feat, int max, double tol, boolean std) {
    names = feat;
    max_pc = max;
    tolerance = tol;
    // standardize = std;
    standardize = std ? 1 : 0;
  }
}
