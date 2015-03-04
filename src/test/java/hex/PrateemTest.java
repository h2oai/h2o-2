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
  private String marketingName;
  private double[] modelCoeffs;

  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Request.API(help = "base coefficient names", required = true, filter = Request.Default.class)
  public String baseNames;

  @Request.API(help = "marketing coefficient names", required = true, filter = Request.Default.class)
  public String marketingNames;

  public static class ModelEvalTask extends MRTask2<ModelEvalTask> {
    double _res;
    @Override public void map(Chunk[] c) {
      for(int i = 0; i < c[0]._len; i++) {
        _res += (c[0].at0(i) - c[1].at0(i))/c[2].at0(i);
      }
    }
    @Override public void reduce(ModelEvalTask mst) {
      _res += mst._res;
    }
  }

  @Test public void test1() {
    baseNames = "stackmetric_historical_quantity_conversion_all, transvar_non_mktg_ind, transvar_ind_cnt_webvisits_99999_1";
    marketingNames = "transvar_add_num_affiliate_afcl_null_22_1,stackmetric_historical_quantity_conversion_all,transvar_ind_num_crm_crm_null_2_1,stackmetric_num_display_cl_desktop_as20,transvar_add_num_display_cl_mobile_22_1,stackmetric_num_display_cl_null_as60,stackmetric_num_display_cl_other_as60,transvar_add_num_display_cl_video_14_1,transvar_add_num_display_im_desktop_7_1,transvar_add_cr_display_im_mobile_14_1,transvar_add_num_display_im_null_28_1,transvar_log_num_display_im_onlineaudio_22_1,transvar_add_cr_display_im_other_28_1,transvar_add_num_display_im_video_2_1,transvar_ind_num_paidsearch_ps_psbrand_22_1,transvar_add_cr_paidsearch_ps_psnonbrand_2_1,transvar_add_num_paidsocial_socl_null_28_1,transvar_add_num_unpaidsocial_upsocl_null_28_1";
    List<String> baseNamesList = Arrays.asList(baseNames.split(","));
    List<String> marketingNamesList = Arrays.asList(marketingNames.split(","));

    Key stackKey = Key.make("thdTransStack10k");
    Frame stackFrame = parseFrame(stackKey, "/home/prateem/thdTransStack10k_cleaned.csv");

            // The following lines of the code is to re-balance the data frame
    //stackFrame.delete();
    //Key rebalancedStackKey = Key.make("rebalanced_thdTransStack10k");
    //H2O.submitTask(new RebalanceDataSet(stackFrame, stackKey, 64)).join();
    //stackFrame.delete();
    //stackFrame = DKV.get(rebalancedStackKey).get();

    Key betaConsKey = Key.make("beta_constraints");
    Frame betaConstraints = parseFrame(betaConsKey, "/home/prateem/constraintSample.csv");

    // To remove independent variables do the below
    //Futures fs = new Futures();
    // The following two lines are to be repeated for every independent variable to be removed
    //stackFrame.remove("<colname>").remove(fs);
    //fs.blockForPending();
    //DKV.put(stackFrame._key,stackFrame);

    GLMModel model = null;
    try {
      Key modelKey = Key.make("candidate model");
      GLM2.Source src = new GLM2.Source((Frame)stackFrame.clone(), stackFrame.vec("response"), false, true);
      new GLM2("Candidate model", Key.make(), modelKey, src, GLMParams.Family.binomial)
              .setNonNegative(false)
              .setRegularization(new double[]{0},new double[]{0.000})
              .setBetaConstraints(betaConstraints)
              .setHighAccuracy()
              .doInit().fork().get();
      model = DKV.get(modelKey).get();
      double [] modelCoeffs = model.beta().clone();
      Frame convProbFull = model.score(stackFrame);

      for (String coefficientnames : model.coefficients_names) {
        System.out.println(coefficientnames);
      }

      double [] baseModelCoeffs = maskModelCoeffs(baseNamesList, marketingNamesList, modelCoeffs, model);
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
      Frame convProbBase = baseModel.score(stackFrame);

      double [] mean = new double[marketingNamesList.size()];

      for (String marketingName : marketingNamesList) {
        ArrayList<String> marketingList = new ArrayList<String>();
        marketingList.add(marketingName);
        double [] singleMarketingModelCoeffs = maskModelCoeffs(null, marketingList, modelCoeffs, baseModel);
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
        Frame convProbMarketing = singleMarketingModel.score(stackFrame);
        Vec [] v = new Vec[3];
        v[0] = convProbMarketing.vec(2);
        v[1] = convProbBase.vec(2);
        v[2] = convProbFull.vec(2);
        mean[marketingNamesList.indexOf(marketingName)] = new ModelEvalTask().doAll(v)._res / v[0].length();
      }

      for (int i = 0; i < mean.length; i++) {
        System.out.println("mean[" + i + "]: " + mean[i]);
      }
      //Key newmodelkey = Key.make(); //Key.make((byte) 1, /*Key.HIDDEN_USER_KEY*/Key.USER_KEY, H2O.SELF);
      //GLMModel newmodel = new GLMModel(model.get_params(), newmodelkey, model._dataKey, model.getParams(), model.coefficients_names, modelCoeffs, model.dinfo(), 0.5);
      //newmodel.delete_and_lock(null).unlock(null);

      // for each independent variables that are marketing except intercept {
      //      make a model involving the all base variables and the marketing variable
      //      find the conversion probability for every row in stack using the model
      //Frame newPred = model.score(stackFrame);

      //stackFrame.add("response", stackFrame.remove("respnse"));

    } finally {
      stackFrame.delete();
      betaConstraints.delete();
      if (model != null)
        model.delete();
    }


    System.out.println("Hello Prateem test1");
    Assert.assertEquals(1, 0);
  }

  private double[] maskModelCoeffs(List<String> baseName, List<String> marketingName, double[] modelCoeffs, GLMModel model) {

    return modelCoeffs;
  }

  private static Frame getFrameForFile(Key outputKey, String path,String [] ignores, String response){
    File f = TestUtil.find_test_file(path);
    Key k = NFSFileVec.make(f);
    Frame fr = ParseDataset2.parse(outputKey, new Key[]{k});
    if(ignores != null)
      for(String s:ignores) UKV.remove(fr.remove(s)._key);
    // put the response to the end
    fr.add(response, fr.remove(response));
    return fr;
  }
}
