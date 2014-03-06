package hex.glm;

import hex.ConfusionMatrix;
import hex.glm.GLMParams.Family;

import java.text.DecimalFormat;
import java.util.Arrays;

import water.*;
import water.api.AUC;
import water.api.DocGen;
import water.api.Request.API;
import water.api.RequestBuilders;

/**
 * Class for GLMValidation.
 *
 * @author tomasnykodym
 *
 */
public class GLMValidation extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="")
  final double _ymu;
  @API(help="")
  double residual_deviance;
  @API(help="")
  double null_deviance;
  @API(help="")
  double avg_err;
  @API(help="")
  long nobs;

  @API(help="best decision threshold")
  float best_threshold;

  @API(help="")
  double auc = Double.NaN;

  @API(help="AIC")
  double aic;// internal aic used only for poisson family!
  @API(help="internal aic used only for poisson family!")
  private double _aic2;// internal aic used only for poisson family!
  @API(help="")
  final Key dataKey;

  public static float [] default_thresholds = new float [] {  0.00f,
      0.01f, 0.02f, 0.03f, 0.04f, 0.05f, 0.06f, 0.07f, 0.08f, 0.09f, 0.10f,
      0.11f, 0.12f, 0.13f, 0.14f, 0.15f, 0.16f, 0.17f, 0.18f, 0.19f, 0.20f,
      0.21f, 0.22f, 0.23f, 0.24f, 0.25f, 0.26f, 0.27f, 0.28f, 0.29f, 0.30f,
      0.31f, 0.32f, 0.33f, 0.34f, 0.35f, 0.36f, 0.37f, 0.38f, 0.39f, 0.40f,
      0.41f, 0.42f, 0.43f, 0.44f, 0.45f, 0.46f, 0.47f, 0.48f, 0.49f, 0.50f,
      0.51f, 0.52f, 0.53f, 0.54f, 0.55f, 0.56f, 0.57f, 0.58f, 0.59f, 0.60f,
      0.61f, 0.62f, 0.63f, 0.64f, 0.65f, 0.66f, 0.67f, 0.68f, 0.69f, 0.70f,
      0.71f, 0.72f, 0.73f, 0.74f, 0.75f, 0.76f, 0.77f, 0.78f, 0.79f, 0.80f,
      0.81f, 0.82f, 0.83f, 0.84f, 0.85f, 0.86f, 0.87f, 0.88f, 0.89f, 0.90f,
      0.91f, 0.92f, 0.93f, 0.94f, 0.95f, 0.96f, 0.97f, 0.98f, 0.99f, 1.00f
  };

  @API(help="Decision thresholds used to generare confuion matrices, AUC and to find the best thresholds based on user criteria")
  public final float [] thresholds;
  @API(help="")
  ConfusionMatrix [] _cms;
  @API(help="")
  final GLMParams _glm;
  @API(help="")
  final private int _rank;

  public static class GLMXValidation extends GLMValidation {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help="n-fold models built for cross-validation")
    Key [] xval_models;
    public GLMXValidation(GLMModel mainModel, GLMModel [] xvalModels, int lambdaIdx, long nobs) {
      super(mainModel._dataKey, mainModel.ymu, mainModel.glm, mainModel.rank(lambdaIdx));
      xval_models = new Key[xvalModels.length];
      for(int i = 0; i < xvalModels.length; ++i){
        add(xvalModels[i].validation());
        xval_models[i] = xvalModels[i]._key;
      }
      this.nobs = nobs;
      finalize_AIC_AUC();
    }
  }
  public GLMValidation(Key dataKey, double ymu, GLMParams glm, int rank){
    this(dataKey, ymu, glm, rank,glm.family == Family.binomial?default_thresholds:null);
  }
  public GLMValidation(Key dataKey, double ymu, GLMParams glm, int rank, float [] thresholds){
    _rank = rank;
    _ymu = ymu;
    _glm = glm;
    if(_glm.family == Family.binomial){
      _cms = new ConfusionMatrix[thresholds.length];
      for(int i = 0; i < _cms.length; ++i)
        _cms[i] = new ConfusionMatrix(2);
    }
    this.dataKey = dataKey;
    this.thresholds = thresholds;
  }
  protected void regularize(double reg){avg_err = avg_err*reg;}
  public static Key makeKey(){return Key.make("__GLMValidation_" + Key.make());}
  public void add(double yreal, double ymodel){
    null_deviance += _glm.deviance(yreal, _ymu);
    if(_glm.family == Family.binomial) // classification -> update confusion matrix too
      for(int i = 0; i < thresholds.length; ++i)
        _cms[i].add((int)yreal, (ymodel >= thresholds[i])?1:0);
    residual_deviance  += _glm.deviance(yreal, ymodel);
    ++nobs;
    avg_err += (ymodel - yreal) * (ymodel - yreal);
    if( _glm.family == Family.poisson ) { // aic for poisson
      long y = Math.round(yreal);
      double logfactorial = 0;
      for( long i = 2; i <= y; ++i )
        logfactorial += Math.log(i);
      _aic2 += (yreal * Math.log(ymodel) - logfactorial - ymodel);
    }
  }
  public void add(GLMValidation v){
    residual_deviance  += v.residual_deviance;
    null_deviance += v.null_deviance;
    avg_err = (double)nobs/(nobs+v.nobs)*avg_err +  (double)v.nobs/(nobs+v.nobs)*v.avg_err;
    nobs += v.nobs;
    _aic2 += v._aic2;
    if(_cms == null)_cms = v._cms;
    else for(int i = 0; i < _cms.length; ++i)_cms[i].add(v._cms[i]);
  }
  public final double nullDeviance(){return null_deviance;}
  public final double residualDeviance(){return residual_deviance;}
  public final long nullDOF(){return nobs-1;}
  public final long resDOF(){return nobs - _rank -1;}
  public double auc(){return auc;}
  public double aic(){return aic;}
  protected void computeAIC(){
    aic = 0;
    switch( _glm.family ) {
      case gaussian:
        aic =  nobs * (Math.log(residual_deviance / nobs * 2 * Math.PI) + 1) + 2;
        break;
      case binomial:
        aic = residual_deviance;
        break;
      case poisson:
        aic = -2*_aic2;
        break; // aic is set during the validation task
      case gamma:
      case tweedie:
        aic = Double.NaN;
        break;
      default:
        assert false : "missing implementation for family " + _glm.family;
    }
    aic += 2*_rank;
  }
  @Override
  public String toString(){
    return "null_dev = " + null_deviance + ", res_dev = " + residual_deviance + ", auc = " + auc();
  }

  protected void finalize_AIC_AUC(){
    if(_glm.family == Family.binomial){
      AUC auc = new AUC(_cms,thresholds);
      this.auc = auc.AUC();
      best_threshold = auc.threshold();
      computeAIC();
    }
  }
  /**
   * Computes area under the ROC curve. The ROC curve is computed from the confusion matrices
   * (there is one for each computed threshold). Area under this curve is then computed as a sum
   * of areas of trapezoids formed by each neighboring points.
   *
   * @return estimate of the area under ROC curve of this classifier.
   */
  double[] tprs;
  double[] fprs;

  private double trapeziod_area(double x1, double x2, double y1, double y2) {
    double base = Math.abs(x1 - x2);
    double havg = 0.5 * (y1 + y2);
    return base * havg;
  }

}
