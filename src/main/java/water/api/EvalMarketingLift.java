package water.api;

import hex.EvalModelAttrib;
import hex.glm.GLMModel;
import water.Request2;
import water.fvec.Frame;

/**
 * Created by prateem on 3/9/15.
 */
public class EvalMarketingLift extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Linear Regression between 2 columns";

  @API(
    help = "model",
    required = true,
    filter = Default.class)
  public GLMModel model;

  @API(
    help = "stack",
    required = true,
    filter = Default.class)
  public Frame stack_frame;

  @API(
    help = "base coefficient names",
    required = true,
    gridable = false,
    filter = Default.class)
  public String base_names;

  @API(
    help = "marketing coefficient names",
    required = true,
    gridable = false,
    filter = Default.class)
  public String marketing_names;

  @API(help = "lift of marketing terms")
  public double [] lift;

  @Override
  protected Response serve() {
    try {
      if (model == null) {
        throw new IllegalArgumentException("Model is needed to evaluate lift due to marketing features!");
      }
      if (stack_frame == null) {
        throw new IllegalArgumentException("Stack is needed to evaluate lift due to marketing features!");
      }
      if (base_names == null) {
        base_names = "";
      }
      if (marketing_names == null) {
        marketing_names = "";
      }

      lift =
        new EvalModelAttrib().scoreModelAttrib(
          model,
          stack_frame,
          base_names,
          marketing_names);

      return Response.done(this);
    }
    catch (Throwable t) {
      return Response.error(t);
    }
    finally {}
  }
}
