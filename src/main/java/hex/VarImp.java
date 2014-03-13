package hex;

import water.Iced;
import water.api.DocGen;
import water.api.Request.API;

public class VarImp extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  @API(help="Variable importance of individual variables.")
  public float[]  varimp;
  @API(help="Names of variables.")
  public String[] variables;

  /** Number of trees participating for producing variable importance measurements */
  //private int ntrees;

  public VarImp(float[] varimp) { this(varimp, null); }
  public VarImp(float[] varimp, String[] variables) {
    this.varimp = varimp;
    this.variables = variables;
    //this.ntrees = ntrees;
  }

  @API(help = "Z-score for individual variables")
  public float[] z_score() {
    float[] zscores = new float[varimp.length];
    /*double rnt = Math.sqrt(ntrees);
    for(int v = 0; v < varimp.length ; v++) {
      //zscores[v] = varimp[v] / (varimpSD[v] / rnt);
    }*/
    return zscores;
  }
}
