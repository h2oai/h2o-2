/**
 * Created by prateem on 3/5/15.
 */

package hex;

import hex.glm.GLMModel;
import water.Key;
import water.MRTask2;
import water.api.Request;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EvalModelAttrib {
  @API(
    help = "model",
    required = true,
    filter = Request.Default.class)
  public GLMModel model;

  @API(
    help = "stack",
    required = true,
    filter = Request.Default.class)
  public Frame stackFrame;

  @API(
    help = "base coefficient names",
    required = true,
    filter = Request.Default.class)
  public String baseNames;

  @API(
    help = "marketing coefficient names",
    required = true,
    filter = Request.Default.class)
  public String marketingNames;

  public static class EvalLiftTask extends MRTask2<EvalLiftTask> {
    double _res;
    @Override public void map(Chunk[] c) {
      for(int i = 0; i < c[0]._len; i++) {
        _res += (c[0].at0(i) - c[1].at0(i)) / c[2].at0(i);
      }
    }
    @Override public void reduce(EvalLiftTask mst) {
      _res += mst._res;
    }
  }

  private double[] maskModelCoeffs(
    List<String> baseNames,
    List<String> marketingNames,
    double[] modelCoeffs,
    GLMModel model) {

    /* Allocate and initialize the return array. Default initialization is 0.0 */
    double [] newModelCoeffs = new double[modelCoeffs.length];
    /*
    If model coefficients is a list then I can find out the index for a
    variable and selectively copy the coefficient of that particular variable
    to new coefficients array. Hence converting double[] to List<double>
    */

    List<String> modelCoefficientsNames =
      Arrays.asList(model.coefficients_names);
    /*
    for (String mcn : modelCoefficientsNames) {
      Log.info(mcn + ":" + mcn.length());
    }
    */
    if (baseNames != null) {
      for (String bterm : baseNames) {
        /*
        Log.info("Copying " + bterm + ":" + bterm.length());
        Log.info(" at index " + modelCoefficientsNames.indexOf(bterm));
        Log.info(" with value " + modelCoeffs[modelCoefficientsNames.indexOf(bterm)]);
        Log.info(" to new coefficient array.");
        */
        //Log.info("Copying " + bterm + " at index " + modelCoefficientsNames.indexOf(bterm) + " with value " + modelCoeffs[modelCoefficientsNames.indexOf(bterm)] + " to new coefficient array.");

        int indbterm = modelCoefficientsNames.indexOf(bterm);
        if (indbterm != -1) {
          newModelCoeffs[modelCoefficientsNames.indexOf(bterm)] =
            modelCoeffs[modelCoefficientsNames.indexOf(bterm)];
        }
      }
    }

    if (marketingNames != null) {
      for (String mterm : marketingNames) {
        /*
        Log.info("Copying " + bterm + ":" + bterm.length());
        Log.info(" at index " + modelCoefficientsNames.indexOf(bterm));
        Log.info(" with value " + modelCoeffs[modelCoefficientsNames.indexOf(bterm)]);
        Log.info(" to new coefficient array.");
        */
        //Log.info("Copying " + mterm + " at index " + modelCoefficientsNames.indexOf(mterm) + " with value " + modelCoeffs[modelCoefficientsNames.indexOf(mterm)] + " to new coefficient array.");

        int indbterm = modelCoefficientsNames.indexOf(mterm);
        if (indbterm != -1) {
          newModelCoeffs[modelCoefficientsNames.indexOf(mterm)] =
            modelCoeffs[modelCoefficientsNames.indexOf(mterm)];
        }
      }
    }
    return newModelCoeffs;
  }

  public double [] scoreModelAttrib(
    GLMModel model,
    Frame stackFrame,
    String baseNames,
    String marketingNames) {

    /* Convert the variable names string into list of names. */
    List<String> baseNamesList = Arrays.asList(baseNames.trim().split("\\s*,\\s*"));
    List<String> marketingNamesList =
      Arrays.asList(marketingNames.trim().split("\\s*,\\s*"));

    /* Extract the coefficients of the full model */
    double [] modelCoeffs = model.beta().clone();

    for(double coeff : modelCoeffs) {
      Log.info("coeff: " + coeff);
    }

    /*
    Score the full model to find out the conversion probability. There is
    one output for every row of the stack. An output is a tuple of three
    elements:
    o Boolean whether converted or not
    o Probability of non conversion
    o Probability of conversion
    */
    Frame convProbFull = model.score(stackFrame);

    /*
    Create the base model structure from full model by masking the
    marketing variables.
    Step 1: mask the marketing coefficients from the full model
    coefficients.
    */
    double [] baseModelCoeffs =
      maskModelCoeffs(
        baseNamesList,
        null,
        modelCoeffs,
        model);

    // Diagnostic printing of base coefficients.
    for(double basecoeff : baseModelCoeffs) {
      Log.info("basecoeff: " + basecoeff);
    }

    /*
    Step 2: Create the H2O model structure of the base model from the
    full model.
    */
    Key baseModelKey = Key.make();
    GLMModel baseModel = new GLMModel(
      model.get_params(),
      baseModelKey,
      model._dataKey,
      model.getParams(),
      model.coefficients_names,
      baseModelCoeffs,
      model.dinfo(),
      0.5);
    baseModel.delete_and_lock(null).unlock(null);

    /* Score the base model to find out the base conversion probability. */
    Frame convProbBase = baseModel.score(stackFrame);

    double [] lift = new double[marketingNamesList.size()];

    /*
    For each marketing term, add it individually to the the base model
    and score it to get the conversion probability of that
    {all base terms + single marketing term} model and calculate
    avg(singleprob - baseprob / fullprob)
    */
    for (String marketingName : marketingNamesList) {
      Log.info("Trying out: " + marketingName);
      // Create a list with the single marketing variable to pass it to mask
      // function
      ArrayList<String> marketingList = new ArrayList<String>();
      marketingList.add(marketingName);

      /*
      Keep all the base terms and mask out all the marketing terms except
      for the one marketing term being tackled in this iteration
      */
      double [] singleMarketingModelCoeffs =
        maskModelCoeffs(baseNamesList, marketingList, modelCoeffs, baseModel);

      // Diagnostic printing of {all base + single marketing} model
      // coefficients

      for(double singlecoeff : singleMarketingModelCoeffs) {
        Log.info("singlecoeff: " + singlecoeff);
      }

      /* Make the H2O model structure for {all base + single marketing} model */
      Key singleMarketingModelKey = Key.make();
      GLMModel singleMarketingModel = new GLMModel(
        model.get_params(),
        singleMarketingModelKey,
        model._dataKey,
        model.getParams(),
        model.coefficients_names,
        singleMarketingModelCoeffs,
        model.dinfo(),
        0.5);
      singleMarketingModel.delete_and_lock(null).unlock(null);

      /*
      Score the {all base + single marketing} model and find the
      conversion probability
      */
      Frame convProbMarketing = singleMarketingModel.score(stackFrame);

      /*
      Calculate the "lift" to conversion probability due to this
      individual marketing term.
      */
      Vec[] v = new Vec[3];
      v[0] = convProbMarketing.vec(2);
      v[1] = convProbBase.vec(2);
      v[2] = convProbFull.vec(2);
      lift[marketingNamesList.indexOf(marketingName)] =
        new EvalLiftTask().doAll(v)._res / v[0].length();

      Log.info("Is Float? " + v[0].isFloat() + ", Length: " + v[0].length());
      for (long i = 0; i < v[0].length() && i < 4; i++) {
        Log.info("Marketing: " + v[0].at(i) + ", Base: " + v[1].at(i) + ", Full: " + v[2].at(i));
      }

      /* Delete the model and conversion probability structures to release memory */
      convProbMarketing.delete();
      singleMarketingModel.delete();
    }
    if (convProbFull != null)
      convProbFull.delete();
    if (baseModel != null)
      baseModel.delete();
    if (convProbBase != null)
      convProbBase.delete();
    return lift;
  }

}
