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
  public Frame stackFrame;

  @API(
    help = "base coefficient names",
    required = true,
    filter = Default.class)
  public String baseNames;

  @API(
    help = "marketing coefficient names",
    required = true,
    filter = Default.class)
  public String marketingNames;

  @Override
  protected Response serve() {
    try {
      if (model == null) {
        throw new IllegalArgumentException("Model is needed to evaluate lift due to marketing features!");
      }
      if (stackFrame == null) {
        throw new IllegalArgumentException("Stack is needed to evaluate lift due to marketing features!");
      }
      if (baseNames == null) {
        baseNames = "";
      }
      if (marketingNames == null) {
        marketingNames = "";
      }

      double [] lift =
        new EvalModelAttrib().scoreModelAttrib(
          model,
          stackFrame,
          baseNames,
          marketingNames);
      return Inspect2.redirect(this, lift.toString());
    }
    catch (Throwable t) {
      return Response.error(t);
    }
    finally {}
  }
}
