package hex.glm;

import hex.FrameTask.DataInfo;
import hex.VarImp;
import hex.glm.GLMParams.Family;
import hex.glm.GLMValidation.GLMXValidation;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.util.Utils;

import java.util.Arrays;
import java.util.HashMap;

public class GLMModel extends Model implements Comparable<GLMModel> {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="lambda_value max, smallest lambda_value which drives all coefficients to zero")
  final double  lambda_max;
  @API(help="mean of response in the training dataset")
  public final double     ymu;

  @API(help="actual expected mean of the response (given by the user before running the model or ymu)")
  final double prior;

  @API(help="job key assigned to the job building this model")
  final Key job_key;

  @API(help = "Model parameters", json = true)
  final private GLM2 parameters;
  @Override public final GLM2 get_params() { return parameters; }
  @Override public final Request2 job() { return get_params(); }

  @API(help="Input data info")
  DataInfo data_info;

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

  @API(help="index of lambda_value giving best results")
  int best_lambda_idx;

  public DataInfo dinfo(){return data_info;}
  public Key [] xvalModels() {
    if(submodels == null)return null;
    for(Submodel sm:submodels)
      if(sm.xvalidation instanceof GLMXValidation){
        return ((GLMXValidation)sm.xvalidation).xval_models;
      }
    return null;
  }
  public double auc(){
    if(glm.family == Family.binomial && submodels != null && submodels[best_lambda_idx].validation != null) {
      Submodel sm = submodels[best_lambda_idx];
      return sm.xvalidation != null?sm.xvalidation.auc:sm.validation.auc;
    }
    return -1;
  }
  public double aic(){
    if(submodels != null && submodels[best_lambda_idx].validation != null){
      Submodel sm = submodels[best_lambda_idx];
      return sm.xvalidation != null?sm.xvalidation.aic:sm.validation.aic;
    }
    return Double.MAX_VALUE;
  }
  public double devExplained(){
    if(submodels == null || submodels[best_lambda_idx].validation == null)
      return 0;
    Submodel sm = submodels[best_lambda_idx];
    GLMValidation val = sm.xvalidation == null?sm.validation:sm.xvalidation;
    return 1.0 - val.residual_deviance/null_validation.residual_deviance;
  }

  public static class UnlockModelTask extends DTask.DKeyTask<UnlockModelTask,GLMModel>{
    final Key _jobKey;
    public UnlockModelTask(H2OCountedCompleter cmp, Key modelKey, Key jobKey){
      super(cmp,modelKey);
      _jobKey = jobKey;
    }
    @Override
    public void map(GLMModel m) {
      Key [] xvals = m.xvalModels();
      if(xvals != null){
        addToPendingCount(xvals.length);
        for(int i = 0; i < xvals.length; ++i)
          new UnlockModelTask(this,xvals[i],_jobKey).forkTask();
      }
      m.unlock(_jobKey);
    }
  }

  public static class DeleteModelTask extends DTask.DKeyTask<DeleteModelTask,GLMModel>{
    final Key _modelKey;

    public DeleteModelTask(H2OCountedCompleter cmp, Key modelKey){
      super(cmp,modelKey);
      _modelKey = modelKey;
    }
    @Override
    public void map(GLMModel m) {
      Key[] xvals = m.xvalModels();
      if (xvals != null) {
        addToPendingCount(xvals.length);
        for (int i = 0; i < xvals.length; ++i)
          new DeleteModelTask(this, xvals[i]).forkTask();
      }
      m.delete();
    }
  }

  @Override public GLMModel clone(){
    GLMModel res = (GLMModel)super.clone();
    res.submodels = submodels.clone();
    if(warnings != null)
      res.warnings = warnings.clone();
    else
      res.warnings = new String[0];
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

  // fully expanded beta used for scoring
  private double [] global_beta;



  @API(help="Validation of the null model")
  public GLMValidation null_validation;

  static class Submodel extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help="lambda_value value used for computation of this submodel")
    final double lambda_value;
    @API(help="number of iterations computed.")
    final int        iteration;
    @API(help="running time of the algo in ms.")
    final long       run_time;
    @API(help="Validation")
    GLMValidation validation;
    @API(help="X-Validation")
    GLMValidation xvalidation;
    @API(help="Beta vector containing model coefficients.") double []  beta;
    @API(help="Beta vector containing normalized coefficients (coefficients obtained on normalized data).") double []  norm_beta;

    final int rank;

    @API(help="Indexes to the coefficient_names array containing names (and order) of the non-zero coefficients in this model.")
    final int [] idxs;

    @API(help="sparseCoefFlag")
    final boolean sparseCoef;

    public Submodel(double lambda , double [] beta, double [] norm_beta, long run_time, int iteration, boolean sparseCoef){
      this.lambda_value = lambda;
      this.run_time = run_time;
      this.iteration = iteration;
      int r = 0;
      if(beta != null){
        final double [] b = norm_beta != null?norm_beta:beta;
        // grab the indeces of non-zero coefficients
        for(double d:beta)if(d != 0)++r;
        idxs = MemoryManager.malloc4(sparseCoef?r:beta.length);
        int j = 0;
        for(int i = 0; i < beta.length; ++i)
          if(!sparseCoef || beta[i] != 0)idxs[j++] = i;
        j = 0;
        this.beta = MemoryManager.malloc8d(idxs.length);
        for(int i:idxs)
          this.beta[j++] = beta[i];
        if(norm_beta != null){
          j = 0;
          this.norm_beta = MemoryManager.malloc8d(idxs.length);
          for(int i:idxs) this.norm_beta[j++] = norm_beta[i];
        }
      } else idxs = null;
      rank = r;
      this.sparseCoef = sparseCoef;
    }
  }

  @API(help = "models computed for particular lambda_value values")
  Submodel [] submodels;

  final boolean useAllFactorLevels;


  @API(help = "Variable importances", json=true)
  VarImp variable_importances;

  public GLMModel(GLM2 parameters, Key selfKey, Key dataKey, GLMParams glmp, String [] coefficients_names, double [] beta, DataInfo dinfo, double threshold) {
    super(selfKey,dataKey,dinfo._adaptedFrame,null);
    this.parameters = (GLM2)parameters.clone();
    submodels = new Submodel[]{new Submodel(0,beta,null,-1,-1,false)};
    this.coefficients_names = coefficients_names;
    alpha = 0;
    lambda_max = Double.NaN;
    this.threshold = threshold;
    useAllFactorLevels = dinfo._useAllFactorLevels;
    global_beta = submodels[0].beta.clone();
    this.glm = glmp;
    this.ymu = Double.NaN;
    this.prior = Double.NaN;
    this.warnings = new String[]{"Hand made model."};
    this.data_info = dinfo;
    this.beta_eps = Double.NaN;
    this.job_key = null;
    best_lambda_idx = 0;
  }
  public GLMModel(GLM2 job, Key selfKey, DataInfo dinfo, GLMParams glm, GLMValidation nullVal, double beta_eps, double alpha, double lambda_max, double ymu, double prior) {
    super(selfKey,job.source._key == null ? dinfo._frameKey : job.source._key,dinfo._adaptedFrame, /* priorClassDistribution */ null);
    parameters = Job.hygiene((GLM2) job.clone());
    job_key = job.self();
    this.ymu = ymu;
    this.prior = prior;
    this.glm = glm;
    threshold = 0.5;
    this.data_info = dinfo;
    this.warnings = new String[0];
    this.alpha = alpha;
    this.lambda_max = lambda_max;
    this.beta_eps = beta_eps;
    submodels = new Submodel[0];
    run_time = 0;
    start_time = System.currentTimeMillis();
    coefficients_names = coefNames();
    useAllFactorLevels = dinfo._useAllFactorLevels;
    null_validation = nullVal;
    null_validation.null_deviance = null_validation.residual_deviance;
  }

  public void pickBestModel(boolean useAuc){
    int bestId = submodels.length-1;
    if(submodels.length > 2) {
      boolean xval = false;
      GLMValidation bestVal = null;
      for(Submodel sm:submodels) {
        if(sm.xvalidation != null) {
          xval = true;
          bestVal = sm.xvalidation;
        }
      }
      if(!xval)
        bestVal = submodels[0].validation;
      for (int i = 1; i < submodels.length; ++i) {
        GLMValidation val = xval ? submodels[i].xvalidation : submodels[i].validation;
        if (val == null || val == bestVal) continue;
        if ((useAuc && val.auc > bestVal.auc)
                || (xval && val.residual_deviance < bestVal.residual_deviance)
                || (((bestVal.residual_deviance - val.residual_deviance) / null_validation.residual_deviance) >= 0.01)) {
          bestVal = val;
          bestId = i;
        }
      }
    }
    best_lambda_idx = bestId;
    setSubmodelIdx(bestId);
  }


  //  public static void setSubmodel(H2OCountedCompleter cmp, Key modelKey, final double lambda, double[] beta, double[] norm_beta, int iteration, long runtime, boolean sparseCoef){
  public static void setSubmodel(H2OCountedCompleter cmp, Key modelKey, final double lambda, double[] beta, double[] norm_beta, int iteration, long runtime, boolean sparseCoef){
    setSubmodel(cmp,modelKey,lambda,beta,norm_beta,iteration,runtime,sparseCoef,null);
  }

  public static class GetScoringModelTask extends DTask.DKeyTask<GetScoringModelTask,GLMModel> {
    final double _lambda;
    public GLMModel _res;
    public GetScoringModelTask(H2OCountedCompleter cmp, Key modelKey, double lambda){
      super(cmp,modelKey);
      _lambda = lambda;
    }
    @Override
    public void map(GLMModel m) {
      _res = m.clone();
      Submodel sm = Double.isNaN(_lambda)?_res.submodels[_res.best_lambda_idx]:_res.submodelForLambda(_lambda);
      assert sm != null : "GLM[" + m._key + "]: missing submodel for lambda " + _lambda;
      sm = (Submodel) sm.clone();
      _res.submodels = new Submodel[]{sm};
      _res.setSubmodelIdx(0);
    }
  }

  public static void setXvalidation(H2OCountedCompleter cmp, Key modelKey, final double lambda, final GLMValidation val){
    // expected cmp has already set correct pending count
    new TAtomic<GLMModel>(cmp){
      @Override
      public GLMModel atomic(GLMModel old) {
        if(old == null)return old; // job could've been cancelled
        old.submodels = old.submodels.clone();
        int id = old.submodelIdForLambda(lambda);
        old.submodels[id] = (Submodel)old.submodels[id].clone();
        old.submodels[id].xvalidation = val;
        old.pickBestModel(false);
        return old;
      }
    }.fork(modelKey);
  }
  public static void setSubmodel(H2OCountedCompleter cmp, Key modelKey, final double lambda, double[] beta, double[] norm_beta, final int iteration, long runtime, boolean sparseCoef, final GLMValidation val){
    final Submodel sm = new Submodel(lambda,beta, norm_beta, runtime, iteration,sparseCoef);
    sm.validation = val;
    cmp.addToPendingCount(1);
    new TAtomic<GLMModel>(cmp){
      @Override
      public GLMModel atomic(GLMModel old) {
        if(old == null)return old; // job could've been cancelled!
        if(old.submodels == null){
          old.submodels = new Submodel[]{sm};
        } else {
          int id = old.submodelIdForLambda(lambda);
          if (id < 0) {
            id = -id - 1;
            old.submodels = Arrays.copyOf(old.submodels, old.submodels.length + 1);
            for (int i = old.submodels.length - 1; i > id; --i)
              old.submodels[i] = old.submodels[i - 1];
          } else if (old.submodels[id].iteration > sm.iteration)
            return old;
          else
            old.submodels = old.submodels.clone();
          old.submodels[id] = sm;
          old.run_time = Math.max(old.run_time,sm.run_time);
        }
        old.pickBestModel(false);
        return old;
      }
    }.fork(modelKey);
  }

  public void addSubmodel(double lambda){
    submodels = Arrays.copyOf(submodels,submodels.length+1);
    run_time = (System.currentTimeMillis()-start_time);
    submodels[submodels.length-1] = new Submodel(lambda,null, null, 0, 0,true);
  }
  public void dropSubmodel() {
    submodels = Arrays.copyOf(submodels,submodels.length-1);
  }
  public double lambda(){
    if(submodels == null)return Double.NaN;
    return submodels[best_lambda_idx].lambda_value;
  }

  public GLMValidation validation(){
    return submodels[best_lambda_idx].validation;
  }
  public int iteration(){
    Submodel [] sm = submodels;
    for(int i = sm.length-1; i >= 0; --i)
      if(sm[i] != null && sm[i].iteration != 0)
        return sm[i].iteration;
    return 0;
  }
  public double [] beta(){return global_beta;}

  public double [] norm_beta(double lambda){
    int i = submodels.length-1;
    for(;i>=0;--i)
      if(submodels[i].lambda_value == lambda) {
        if(submodels[i].norm_beta == null)
          return beta(); // not normalized
        double [] res = MemoryManager.malloc8d(beta().length);
        int k = 0;
        for(int j:submodels[i].idxs)
          res[j] = submodels[i].norm_beta[k++];
        return res;
      }
    throw new RuntimeException("No submodel for lambda_value = " + lambda);
  }

  public void addWarning(String w){
    final int n = warnings.length;
    warnings = Arrays.copyOf(warnings,warnings.length+1);
    warnings[n] = w;
  }
  @Override protected float[] score0(double[] data, float[] preds) {
    double eta = 0.0;
    final double [] b = beta();
    if(!useAllFactorLevels){ // skip level 0 of all factors
      for(int i = 0; i < data_info._catOffsets.length-1; ++i) if(data[i] != 0)
        eta += b[data_info._catOffsets[i] + (int)(data[i]-1)];
    } else { // do not skip any levels!
      for(int i = 0; i < data_info._catOffsets.length-1; ++i)
        eta += b[data_info._catOffsets[i] + (int)data[i]];
    }
    final int noff = data_info.numStart() - data_info._cats;
    for(int i = data_info._cats; i < data.length; ++i)
      eta += b[noff+i]*data[i];
    if(data_info._hasIntercept)
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

  @Override public int nclasses(){ return glm.family == Family.binomial?2:1;}

  @Override
  public String [] classNames(){
    String [] res = super.classNames();
    if(glm.getFamily() == Family.binomial && res == null)
      res = new String[]{"0","1"};
    return res;
  }

  //  public static void setAndTestValidation(final H2OCountedCompleter cmp,final Key modelKey, final double lambda, final GLMValidation val){
//    if(cmp != null)cmp.addToPendingCount(1);
//    new TAtomic<GLMModel>(cmp){
//      @Override
//      public GLMModel atomic(GLMModel old) {
//        if(old == null)return old;
//        old.submodels = old.submodels.clone();
//        Submodel sm = old.submodelForLambda(lambda);
//        if(sm == null)return old;
//        if(val instanceof GLMXValidation)
//          sm.xvalidation = (GLMXValidation)val;
//        else
//          sm.validation = val;
//        old.pickBestModel(false);
//        return old;
//      }
//    }.fork(modelKey);
//  }
  public static class GLMValidationTask<T extends GLMValidationTask<T>> extends MRTask2<T> {
    protected final GLMModel _model;
    protected GLMValidation _res;
    public final double _lambda;
    public boolean _improved;
    Key _jobKey;
    public static Key makeKey(){return Key.make("__GLMValidation_" + Key.make().toString());}
    public GLMValidationTask(GLMModel model, double lambda){this(model,lambda,null);}
    public GLMValidationTask(GLMModel model, double lambda, H2OCountedCompleter completer){super(completer); _lambda = lambda; _model = model;}
    @Override public void map(Chunk [] chunks){
      _res = new GLMValidation(null,_model.glm,_model.rank(_lambda));
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
        _model.score0(row, preds);
        double response = chunks[chunks.length-1].at0(i);
        _res.add(response, _model.glm.family == Family.binomial?preds[2]:preds[0]);
      }
    }
    @Override public void reduce(GLMValidationTask gval){_res.add(gval._res);}
    @Override public void postGlobal(){
      _res.computeAIC();
      _res.computeAUC();
    }
  }
  // use general score to reduce number of possible different code paths
  public static class GLMXValidationTask extends GLMValidationTask<GLMXValidationTask>{
    protected final GLMModel [] _xmodels;
    protected GLMValidation [] _xvals;
    long _nobs;
    final float [] _thresholds;
    public static Key makeKey(){return Key.make("__GLMValidation_" + Key.make().toString());}

    public GLMXValidationTask(GLMModel mainModel,double lambda, GLMModel [] xmodels, float [] thresholds){this(mainModel,lambda,xmodels,thresholds,null);}
    public GLMXValidationTask(GLMModel mainModel,double lambda, GLMModel [] xmodels, float [] thresholds, final H2OCountedCompleter completer){
      super(mainModel, lambda,completer);
      _xmodels = xmodels;
      _thresholds = thresholds;
    }
    @Override public void map(Chunk [] chunks){
      _xvals = new GLMValidation[_xmodels.length];
      for(int i = 0; i < _xmodels.length; ++i)
        _xvals[i] = new GLMValidation(null,_xmodels[i].glm,_xmodels[i].rank(),_thresholds);
      final int nrows = chunks[0]._len;
      long start = chunks[0]._start;
      double [] row   = MemoryManager.malloc8d(_xmodels[0]._names.length);
      float  [] preds = MemoryManager.malloc4f(_xmodels[0].glm.family == Family.binomial?3:1);
      OUTER:
      for(int i = 0; i < nrows; ++i){
        if(chunks[chunks.length-1].isNA0(i))continue;
        for(int j = 0; j < chunks.length-1; ++j){
          if(chunks[j].isNA0(i))continue OUTER;
          row[j] = chunks[j].at0(i);
        }
        ++_nobs;
        final int mid = (int)((i + start) % _xmodels.length);
        final GLMModel model = _xmodels[mid];
        final GLMValidation val = _xvals[mid];
        model.score0(row, preds);
        double response = chunks[chunks.length-1].at80(i);
        val.add(response, model.glm.family == Family.binomial?preds[2]:preds[0]);
      }
    }
    @Override public void reduce(GLMXValidationTask gval){
      _nobs += gval._nobs;
      for(int i = 0; i < _xvals.length; ++i)
        _xvals[i].add(gval._xvals[i]);}

    @Override public void postGlobal() {
      H2OCountedCompleter cmp = (H2OCountedCompleter)getCompleter();
      if(cmp != null)cmp.addToPendingCount(_xvals.length + 1);
      for (int i = 0; i < _xvals.length; ++i) {
        _xvals[i].computeAIC();
        _xvals[i].computeAUC();
        _xvals[i].nobs = _nobs - _xvals[i].nobs;
        _xvals[i].null_deviance = _xmodels[i].null_validation.residual_deviance;
        GLMModel.setXvalidation(cmp, _xmodels[i]._key, _lambda, _xvals[i]);
      }
      GLMXValidation xval = new GLMXValidation(_model, _xmodels, _xvals, _lambda, _nobs,_thresholds);
      xval.null_deviance = _model.null_validation.residual_deviance;
      GLMModel.setXvalidation(cmp, _model._key, _lambda, xval);
    }
  }


  public GLMParams getParams() {
    return glm;
  }

  @Override
  public String toString(){
    return ("GLM Model (key=" + _key + " , trained on " + _dataKey + ", family = " + glm.family + ", link = " + glm.link + ", #iterations = " + iteration() + ")");
  }
  public int rank() {return rank(submodels[best_lambda_idx].lambda_value);}

  public int  submodelIdForLambda(double lambda){
    if(lambda > lambda_max)lambda = lambda_max;
    int i = submodels.length-1;
    for(;i >=0; --i)
      // first condition to cover lambda == 0 case (0/0 is Inf in java!)
      if(lambda == submodels[i].lambda_value || Math.abs(submodels[i].lambda_value - lambda)/lambda < 1e-5)
        return i;
      else if(submodels[i].lambda_value > lambda)
        return -i-2;
    return -1;
  }
  public Submodel  submodelForLambda(double lambda){
    if(lambda > lambda_max)
      return submodels[0];
    int i = submodelIdForLambda(lambda);
    return i < 0?null:submodels[i];
  }
  public int rank(double lambda) {
    Submodel sm = submodelForLambda(lambda);
    if(sm == null)return 0;
    return submodelForLambda(lambda).rank;
  }

  public void setValidation(GLMValidation val ){
    submodels[submodels.length-1].validation = val;
  }

  public void setSubmodelIdx(int l){
    best_lambda_idx = l;
    threshold = submodels[l].validation == null?0.5:submodels[l].validation.best_threshold;
    if(global_beta == null) global_beta = MemoryManager.malloc8d(this.coefficients_names.length);
    else Arrays.fill(global_beta,0);
    int j = 0;
    for(int i:submodels[l].idxs)
      global_beta[i] = submodels[l].beta[j++];
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

  public VarImp varimp() {
    return this.variable_importances;
  }

  protected void maybeComputeVariableImportances() {
    GLM2 params = get_params();
    this.variable_importances = null;

    final double[] b = beta();
    if (params.variable_importances && null != b) {

      // Warn if we may be returning results that might not include an important (base) level. . .
      if (! params.use_all_factor_levels)
        this.addWarning("Variable Importance may be missing important variables: because use_all_factor_levels is off the importance of base categorical levels will NOT be included.");

      float[] coefs_abs_value = new float[b.length - 1]; // Don't include the Intercept
      String[] names = new String[b.length - 1];
      for (int i = 0; i < b.length - 1; ++i) {
        coefs_abs_value[i] = (float)Math.abs(b[i]);
        names[i] = coefficients_names[i];
      }
      this.variable_importances = new VarImp(coefs_abs_value, names);
    }
  }
}
