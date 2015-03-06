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
import water.util.Log;

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

      for (double coeff : modelCoeffs) {
        Log.info("coeff: " + coeff);
      }

      double [] lift =
        new EvalModelAttrib().scoreModelAttrib(
          model,
          stackFrame,
          baseNames,
          marketingNames);

      for (String marketingName : marketingNamesList) {
        Log.info(
          "lift["
            + marketingName
            + "]: "
            + lift[marketingNamesList.indexOf(marketingName)]);
      }

      /*
      for (String coefficientnames : model.coefficients_names) {
        Log.info();(coefficientnames);
      }
      */
    } finally {
      betaConstraints.delete();
      stackFrame.delete();
      if (model != null)
        model.delete();
    }
    Assert.assertEquals(1, 0);
  }
}
