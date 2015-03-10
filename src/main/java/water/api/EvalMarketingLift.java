package water.api;

import hex.EvalModelAttrib;
import hex.glm.GLMModel;
import water.Request2;
import water.fvec.Frame;

/**
 * Created by prateem on 3/9/15.
 */
public class EvalMarketingLift extends Request2 {
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
