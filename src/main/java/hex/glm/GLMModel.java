package hex.glm;

import hex.ConfusionMatrix;
import hex.FrameTask.DataInfo;
import hex.glm.GLMParams.Family;
import hex.glm.GLMValidation.GLMXValidation;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.util.Utils;

import java.util.HashMap;

public class GLMModel extends Model implements Comparable<GLMModel> {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="lambda max, smallest lambda which drives all coefficients to zero")
  final double  lambda_max;
  @API(help="mean of response in the training dataset")
  final double     ymu;

  @API(help="job key assigned to the job building this model")
  final Key job_key;

  @API(help = "Model parameters", json = true)
  final private GLM2 parameters;
  @Override public final GLM2 get_params() { return parameters; }
  @Override public final Request2 job() { return get_params(); }

  @API(help="Input data info")
  DataInfo data_info;

  @API(help="warnings")
  String []  warnings;
  @API(help="Decision threshold.")
  double     threshold;
  @API(help="glm params")
  final GLMParams  glm;
  @API(help="beta epsilon - stop iterating when beta diff is below this threshold.")
  final double     beta_eps;
  @API(help="regularization parameter driving proportion of L1/L2 penalty.")
  final double     alpha;

  @API(help="column names including expanded categorical values")
  public String [] coefficients_names;

  @API(help="index of lambda giving best results")
  int best_lambda_idx;

  public double auc(){
    if(glm.family == Family.binomial && submodels != null && submodels[best_lambda_idx].validation != null)
      return submodels[best_lambda_idx].validation.auc;
    return -1;
  }
  public double aic(){
    if(submodels != null && submodels[best_lambda_idx].validation != null)
      return submodels[best_lambda_idx].validation.aic;
    return Double.MAX_VALUE;
  }
  public double devExplained(){
    if(submodels == null || submodels[best_lambda_idx].validation == null)
      return 0;
    GLMValidation val = submodels[best_lambda_idx].validation;
    return 1.0 - val.residual_deviance/val.null_deviance;
  }

  @Override public GLMModel clone(){
    GLMModel res = (GLMModel)super.clone();
    res.submodels = submodels.clone();
    if(warnings != null)res.warnings = warnings.clone();
    return res;
  }

  @Override
  public int compareTo(GLMModel m){
//    assert m._dataKey.equals(_dataKey);
    assert m.glm.family == glm.family;
    assert m.glm.link == glm.link;
    switch(glm.family){
      case binomial: // compare by AUC, higher is better
        return (int)(1e6*(m.auc()-auc()));
      case gamma: // compare by percentage of explained deviance, higher is better
        return (int)(100*(m.devExplained()-devExplained()));
      default: // compare by AICs by default, lower is better
        return (int)(100*(aic()- m.aic()));
    }
  }
  @API(help="Overall run time")
  long run_time;
  @API(help="computation started at")
  long start_time;

  static class Submodel extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    @API(help="number of iterations computed.")
    final int        iteration;
    @API(help="running time of the algo in ms.")
    final long       run_time;
    @API(help="Validation")
    GLMValidation validation;
    @API(help="Beta vector containing model coefficients.") double []  beta;
    @API(help="Beta vector containing normalized coefficients (coefficients obtained on normalized data).") double []  norm_beta;

    final int rank;

    @API(help="Indexes to the coefficient_names array containing names (and order) of the non-zero coefficients in this model.")
    final int [] idxs;

    public Submodel(double [] beta, double [] norm_beta, long run_time, int iteration){
      this.beta = beta;
      this.norm_beta = norm_beta;
      this.run_time = run_time;
      this.iteration = iteration;
      int r = 0;
      if(beta != null){
        final double [] b = norm_beta != null?norm_beta:beta;
        // grab the indeces of non-zero coefficients
        for(double d:beta)if(d != 0)++r;
        idxs = new int[r];
        int ii = 0;
        for(int i = 0; i < b.length; ++i)if(b[i] != 0)idxs[ii++] = i;
        // now sort them
        for(int i = 1; i < r; ++i){
          for(int j = 1; j < r-i;++j){
            if(Math.abs(b[idxs[j-1]]) < Math.abs(b[idxs[j]])){
              int jj = idxs[j];
              idxs[j] = idxs[j-1];
              idxs[j-1] = jj;
            }
          }
        }
      } else idxs = null;
      rank = r;
    }
    @Override
    public Submodel clone(){return new Submodel(beta == null?null:beta.clone(),norm_beta == null?null:norm_beta.clone(),run_time,iteration);}
  }

  @API(help = "models computed for particular lambda values")
  Submodel [] submodels;

  @API(help = "lambda sequence")
  final double [] lambdas;

  public GLMModel(GLM2 job, Key selfKey, DataInfo dinfo, GLMParams glm, double beta_eps, double alpha, double lambda_max, double [] lambda, double ymu) {
    super(selfKey,null,dinfo._adaptedFrame);
    parameters = job;
    job_key = job.self();
    this.ymu = ymu;
    this.glm = glm;
    threshold = 0.5;
    this.data_info = dinfo;
    this.warnings = null;
    this.alpha = alpha;
    this.lambda_max = lambda_max;
    this.lambdas = lambda;
    this.beta_eps = beta_eps;
    submodels = new Submodel[lambda.length];
    for(int i = 0; i < submodels.length; ++i)
      submodels[i] = new Submodel(null, null, 0, 0);
    run_time = 0;
    start_time = System.currentTimeMillis();
    coefficients_names = coefNames();
  }
  public void setLambdaSubmodel(int lambdaIdx, double [] beta, double [] norm_beta, int iteration){
    run_time = (System.currentTimeMillis()-start_time);
    submodels[lambdaIdx] = new Submodel(beta, norm_beta, run_time, iteration);
  }

  public double lambda(){
    if(submodels == null)return Double.NaN;
    return lambdas[best_lambda_idx];
  }
  public double lambdaMax(){
    return lambdas[0];
  }
  public double lambdaMin(){
    return lambdas[lambdas.length-1];
  }
  public GLMValidation validation(){
    return submodels[best_lambda_idx].validation;
  }
  public int iteration(){
    int res = submodels[0].iteration;
    for(int i = 1; i < submodels.length && submodels[i] != null && submodels[i].iteration != 0; ++i)
      res = submodels[i].iteration;
    return res;
  }
  public double [] beta(){return submodels[best_lambda_idx].beta;}
  public double [] beta(int i){return submodels[i].beta;}
  public double [] norm_beta(){return submodels[best_lambda_idx].norm_beta;}
  public double [] norm_beta(int i){return submodels[i].norm_beta;}
  @Override protected float[] score0(double[] data, float[] preds) {
    return score0(data,preds,best_lambda_idx);
  }
  protected float[] score0(double[] data, float[] preds, int lambdaIdx) {
    double eta = 0.0;
    final double [] b = beta(lambdaIdx);
    for(int i = 0; i < data_info._catOffsets.length-1; ++i) if(data[i] != 0)
      eta += b[data_info._catOffsets[i] + (int)(data[i]-1)];
    final int noff = data_info.numStart() - data_info._cats;
    for(int i = data_info._cats; i < data.length; ++i)
      eta += b[noff+i]*data[i];
    eta += b[b.length-1]; // add intercept
    double mu = glm.linkInv(eta);
    preds[0] = (float)mu;
    if( glm.family == Family.binomial ) { // threshold for prediction
      if(Double.isNaN(mu)){
        preds[0] = Float.NaN;
        preds[1] = Float.NaN;
        preds[2] = Float.NaN;
      } else {
        preds[0] = (mu >= threshold ? 1 : 0);
        preds[1] = 1.0f - (float)mu; // class 0
        preds[2] =        (float)mu; // class 1
      }
    }
    return preds;
  }
  public final int ncoefs() {return beta().length;}

  public static class GLMValidationTask<T extends GLMValidationTask<T>> extends MRTask2<T> {
    protected final GLMModel _model;
    protected GLMValidation _res;
    public int _lambdaIdx;
    public boolean _improved;
    public static Key makeKey(){return Key.make("__GLMValidation_" + Key.make().toString());}
    public GLMValidationTask(GLMModel model, int lambdaIdx){this(model,lambdaIdx,null);}
    public GLMValidationTask(GLMModel model, int lambdaIdx, H2OCountedCompleter completer){super(completer); _lambdaIdx = lambdaIdx; _model = model;}
    @Override public void map(Chunk [] chunks){
      _res = new GLMValidation(null,_model.ymu,_model.glm,_model.rank(_lambdaIdx));
      final int nrows = chunks[0]._len;
      double [] row   = MemoryManager.malloc8d(_model._names.length);
      float  [] preds = MemoryManager.malloc4f(_model.glm.family == Family.binomial?3:1);
      OUTER:
      for(int i = 0; i < nrows; ++i){
        if(chunks[chunks.length-1].isNA0(i))continue;
        for(int j = 0; j < chunks.length-1; ++j){
          if(chunks[j].isNA0(i))continue OUTER;
          row[j] = chunks[j].at0(i);
        }
        _model.score0(row, preds,_lambdaIdx);
        double response = chunks[chunks.length-1].at0(i);
        _res.add(response, _model.glm.family == Family.binomial?preds[2]:preds[0]);
      }
    }
    @Override public void reduce(GLMValidationTask gval){_res.add(gval._res);}
    @Override public void postGlobal(){
      _res.finalize_AIC_AUC();
    }
  }
  // use general score to reduce number of possible different code paths
  public static class GLMXValidationTask extends GLMValidationTask<GLMXValidationTask>{
    protected final GLMModel [] _xmodels;
    protected GLMValidation [] _xvals;
    long _nobs;
    public static Key makeKey(){return Key.make("__GLMValidation_" + Key.make().toString());}
    public GLMXValidationTask(GLMModel mainModel,int lambdaIdx, GLMModel [] xmodels){this(mainModel,lambdaIdx,xmodels,null);}
    public GLMXValidationTask(GLMModel mainModel,int lambdaIdx, GLMModel [] xmodels, H2OCountedCompleter completer){super(mainModel, lambdaIdx, completer); _xmodels = xmodels;}
    @Override public void map(Chunk [] chunks){
      _xvals = new GLMValidation[_xmodels.length];
      for(int i = 0; i < _xmodels.length; ++i)
        _xvals[i] = new GLMValidation(null,_xmodels[i].ymu,_xmodels[i].glm,_xmodels[i].rank());
      final int nrows = chunks[0]._len;
      double [] row   = MemoryManager.malloc8d(_model._names.length);
      float  [] preds = MemoryManager.malloc4f(_model.glm.family == Family.binomial?3:1);
      OUTER:
      for(int i = 0; i < nrows; ++i){
        if(chunks[chunks.length-1].isNA0(i))continue;
        for(int j = 0; j < chunks.length-1; ++j){
          if(chunks[j].isNA0(i))continue OUTER;
          row[j] = chunks[j].at0(i);
        }
        ++_nobs;
        final int mid = i % _xmodels.length;
        final GLMModel model = _xmodels[mid];
        final GLMValidation val = _xvals[mid];
        model.score0(row, preds);
        double response = chunks[chunks.length-1].at80(i);
        val.add(response, model.glm.family == Family.binomial?preds[1]:preds[0]);
      }
    }
    @Override public void reduce(GLMXValidationTask gval){
      _nobs += gval._nobs;
      for(int i = 0; i < _xvals.length; ++i)
        _xvals[i].add(gval._xvals[i]);}

    @Override public void postGlobal(){
      Futures fs = new Futures();
      for(int i = 0; i < _xmodels.length; ++i){
        _xvals[i].finalize_AIC_AUC();
        _xvals[i].nobs = _nobs-_xvals[i].nobs;
        _xmodels[i].setAndTestValidation(0, _xvals[i]);
        DKV.put(_xmodels[i]._key, _xmodels[i],fs);
      }
      _res = new GLMXValidation(_model, _xmodels,_lambdaIdx,_nobs);
      fs.blockForPending();
    }
  }

  public GLMParams getParams() {
      return glm;
  }

  @Override
  public String toString(){
    final double [] beta = beta(), norm_beta = norm_beta();
    StringBuilder sb = new StringBuilder("GLM Model (key=" + _key + " , trained on " + _dataKey + ", family = " + glm.family + ", link = " + glm.link + ", #iterations = " + iteration() + "):\n");
    final int cats = data_info._cats;
    int k = 0;
    for(int i = 0; i < cats; ++i)
      for(int j = 1; j < _domains[i].length; ++j)
        sb.append(_names[i] + "." + _domains[i][j] + ": " + beta[k++] + "\n");
    final int nums = beta.length-k-1;
    for(int i = 0; i < nums; ++i)
      sb.append(_names[cats+i] + ": " + beta[k+i] + "\n");
    sb.append("Intercept: " + beta[beta.length-1] + "\n");
    return sb.toString();
  }
  public int rank() {return rank(best_lambda_idx);}
  public int rank(int lambdaIdx) {return submodels[lambdaIdx].rank;}

  public boolean setAndTestValidation(int lambdaIdx,GLMValidation val ){
    submodels[lambdaIdx].validation = val;
    if(lambdaIdx == 0 || rank(lambdaIdx) == 1){
      threshold = val.best_threshold;
      return true;
    }
    double diff = (submodels[lambdaIdx-1].validation.residual_deviance - val.residual_deviance)/val.null_deviance;
    if(diff >= 0.01) {
      best_lambda_idx = lambdaIdx;
      threshold = val.best_threshold;
      System.out.println("setting threshold to " + threshold);
    }
    return  true;
  }

  /**
   * get beta coefficients in a map indexed by name
   * @return
   */
  public HashMap<String,Double> coefficients(){
    HashMap<String, Double> res = new HashMap<String, Double>();
    final double [] b = beta();
    if(b != null) for(int i = 0; i < b.length; ++i)res.put(coefficients_names[i],b[i]);
    return res;
  }
  private String [] coefNames(){
    return Utils.append(data_info.coefNames(),new String[]{"Intercept"});
  }
}
