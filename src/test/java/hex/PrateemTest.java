package hex;

import hex.glm.GLM2;
import hex.glm.GLMModel;
import hex.glm.GLMParams;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.api.Request;
import water.fvec.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class PrateemTest extends TestUtil {
  //private String marketingName;
  //private double[] modelCoeffs;

  @BeforeClass public static void stall()
  {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Request.API(
    help = "base coefficient names",
    required = true,
    filter = Request.Default.class)
  public String baseNames;

  @Request.API(
    help = "marketing coefficient names",
    required = true,
    filter = Request.Default.class)
  public String marketingNames;

  public static class ModelEvalTask extends MRTask2<ModelEvalTask> {
    double _res;
    @Override public void map(Chunk[] c) {
      for(int i = 0; i < c[0]._len; i++) {
        _res += (c[0].at0(i) - c[1].at0(i)) / c[2].at0(i);
      }
    }
    @Override public void reduce(ModelEvalTask mst) {
      _res += mst._res;
    }
  }

  private double [] scoreModelAttrib(
    GLMModel model,
    Frame stackFrame,
    List<String> baseNamesList,
    List<String> marketingNamesList) {
    // Extract the coefficients of the full model
    double [] modelCoeffs = model.beta().clone();

    for(double coeff : modelCoeffs) {
      System.out.println("coeff: " + coeff);
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
      System.out.println("basecoeff: " + basecoeff);
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
      System.out.println("Trying out: " + marketingName);
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
        System.out.println("singlecoeff: " + singlecoeff);
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

      // Score the {all base + single marketing} model and find the
      // conversion probability
      Frame convProbMarketing = singleMarketingModel.score(stackFrame);

      // Calculate the "lift" to conversion probability due to this
      // individual marketing term.
      Vec [] v = new Vec[3];
      v[0] = convProbMarketing.vec(2);
      v[1] = convProbBase.vec(2);
      v[2] = convProbFull.vec(2);
      lift[marketingNamesList.indexOf(marketingName)] =
        new ModelEvalTask().doAll(v)._res / v[0].length();

      System.out.println("Is Float? " + v[0].isFloat() + ", Length: " + v[0].length());
      for (long i = 0; i < v[0].length() && i < 4; i++) {
        System.out.println("Marketing: " + v[0].at(i) + ", Base: " + v[1].at(i) + ", Full: " + v[2].at(i));
      }

      // Delete the model and conversion probability structures to release memory
      convProbMarketing.delete();
      singleMarketingModel.delete();
    }
    if (convProbBase != null)
      convProbBase.delete();
    if (convProbFull != null)
      convProbFull.delete();
    return lift;
  }

  @Test public void test1() {
    /* base variable and marketing variable names will come in as input. */
    baseNames = "stackmetric_historical_quantity_conversion_all, transvar_non_mktg_ind, transvar_ind_cnt_webvisits_99999_1";
    marketingNames = "transvar_add_num_affiliate_afcl_null_22_1,transvar_ind_num_crm_crm_null_2_1,stackmetric_num_display_cl_desktop_as20,transvar_add_num_display_cl_mobile_22_1,stackmetric_num_display_cl_null_as60,stackmetric_num_display_cl_other_as60,transvar_add_num_display_cl_video_14_1,transvar_add_num_display_im_desktop_7_1,transvar_add_cr_display_im_mobile_14_1,transvar_add_num_display_im_null_28_1,transvar_log_num_display_im_onlineaudio_22_1,transvar_add_cr_display_im_other_28_1,transvar_add_num_display_im_video_2_1,transvar_ind_num_paidsearch_ps_psbrand_22_1,transvar_add_cr_paidsearch_ps_psnonbrand_2_1,transvar_add_num_paidsocial_socl_null_28_1,transvar_add_num_unpaidsocial_upsocl_null_28_1";

    /* Convert the variable names string into list of names. */
    List<String> baseNamesList = Arrays.asList(baseNames.trim().split("\\s*,\\s*"));
    List<String> marketingNamesList =
      Arrays.asList(marketingNames.trim().split("\\s*,\\s*"));

    // Create the data frame for the stack. This will come in as an input.
    Key stackKey = Key.make("thdTransStack10k");
    Frame stackFrame =
      parseFrame(stackKey, "/home/prateem/thdTransStack10k_cleaned.csv");

    /*
    The following lines of the code is to re-balance the data frame
    stackFrame.delete();
    Key rebalancedStackKey = Key.make("rebalanced_thdTransStack10k");
    H2O.submitTask(new RebalanceDataSet(stackFrame, stackKey, 64)).join();
    stackFrame.delete();
    stackFrame = DKV.get(rebalancedStackKey).get();
    */

    /*
    Create the data frame for the coefficient constraints. This will come
    in as input
    */
    Key betaConsKey = Key.make("beta_constraints");
    Frame betaConstraints =
      parseFrame(betaConsKey, "/home/prateem/constraintSample.csv");

    // To remove independent variables do the below
    //Futures fs = new Futures();
    // The following two lines are to be repeated for every independent
    // variable to be removed
    //stackFrame.remove("<colname>").remove(fs);
    //fs.blockForPending();
    //DKV.put(stackFrame._key,stackFrame);

    GLMModel model = null;
    try {
      /*
      Create the H2O structure for the full model that is including all
      base and marketing variables. This will come in as an input.
      */
      Key modelKey = Key.make("Candidate model key");
      GLM2.Source src =
        new GLM2.Source(
          (Frame)stackFrame.clone(),
          stackFrame.vec("response"),
          false,
          true);
      new GLM2("Candidate model", Key.make(), modelKey, src, GLMParams.Family.binomial)
        .setNonNegative(false)
        .setRegularization(new double[]{0},new double[]{0.000})
        .setBetaConstraints(betaConstraints)
        .setHighAccuracy()
        .doInit().fork().get();
      model = DKV.get(modelKey).get();

      // Extract the coefficients of the full model
      double [] modelCoeffs = model.beta().clone();

      for(double coeff : modelCoeffs) {
        System.out.println("coeff: " + coeff);
      }

      double [] lift =
        scoreModelAttrib(
          model,
          stackFrame,
          baseNamesList,
          marketingNamesList);

      for (String marketingName : marketingNamesList) {
        System.out.println(
          "lift["
            + marketingName
            + "]: "
            + lift[marketingNamesList.indexOf(marketingName)]);
      }

      /*
      for (String coefficientnames : model.coefficients_names) {
        System.out.println(coefficientnames);
      }
      */
    } finally {
      betaConstraints.delete();
      stackFrame.delete();
      if (model != null)
        model.delete();
    }
    System.out.println("Hello Prateem test1");
    Assert.assertEquals(1, 0);
  }

  private double[] maskModelCoeffs(
    List<String> baseNames,
    List<String> marketingNames,
    double[] modelCoeffs,
    GLMModel model) {

    //System.out.println("** BEGIN **");
    /*
    System.out.println("Base size: " + baseNames.size() + ", Marketing size: " + marketingNames.size() + ", Coeff size: " + modelCoeffs.length);
    if (baseNames != null)
    for (String b : baseNames) {
      System.out.println(b);
    }
    if (marketingNames != null)
    for (String m : marketingNames) {
      System.out.println(m);
    }
    for (double c : modelCoeffs) {
      System.out.println(c);
    }
    */

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
      System.out.println(mcn + ":" + mcn.length());
    }
    */
    if (baseNames != null) {
      for (String bterm : baseNames) {
        /*
        System.out.println("Copying " + bterm + ":" + bterm.length());
        System.out.println(" at index " + modelCoefficientsNames.indexOf(bterm));
        System.out.println(" with value " + modelCoeffs[modelCoefficientsNames.indexOf(bterm)]);
        System.out.println(" to new coefficient array.");
        */
        //System.out.println("Copying " + bterm + " at index " + modelCoefficientsNames.indexOf(bterm) + " with value " + modelCoeffs[modelCoefficientsNames.indexOf(bterm)] + " to new coefficient array.");

        int indbterm = modelCoefficientsNames.indexOf(bterm);
          if (indbterm != -1) {
            newModelCoeffs[modelCoefficientsNames.indexOf(bterm)] =
              modelCoeffs[modelCoefficientsNames.indexOf(bterm)];
        }
        //System.out.println("Copying done");
      }
      //System.out.println("Out of it");
    }

    if (marketingNames != null) {
      for (String mterm : marketingNames) {
        /*
        System.out.println("Copying " + bterm + ":" + bterm.length());
        System.out.println(" at index " + modelCoefficientsNames.indexOf(bterm));
        System.out.println(" with value " + modelCoeffs[modelCoefficientsNames.indexOf(bterm)]);
        System.out.println(" to new coefficient array.");
        */
        //System.out.println("Copying " + mterm + " at index " + modelCoefficientsNames.indexOf(mterm) + " with value " + modelCoeffs[modelCoefficientsNames.indexOf(mterm)] + " to new coefficient array.");

        int indbterm = modelCoefficientsNames.indexOf(mterm);
        if (indbterm != -1) {
          newModelCoeffs[modelCoefficientsNames.indexOf(mterm)] =
            modelCoeffs[modelCoefficientsNames.indexOf(mterm)];
        }
        //System.out.println("Copying done");
      }
      //System.out.println("Out of it");
    }
    //System.out.println("*** END ***");
    return newModelCoeffs;
  }

}
