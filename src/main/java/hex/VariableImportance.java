package hex;

import water.api.DocGen;
import water.api.Request.API;

public class VariableImportance {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  @API(help="Variable importance of individual variables.")
  public float[]  varimp;
  @API(help="Names of variables.")
  public String[] variables;

  public VariableImportance(float[] varimp, String[] variables) {
    this.varimp = varimp;
    this.variables = variables;
  }
}
