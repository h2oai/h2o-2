package hex.glm;

import hex.glm.GLMParams.Family;

import java.text.DecimalFormat;
import java.util.HashMap;

import water.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.*;
import water.util.RString;

public class GLMModel extends Model {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="mean of response in the training dataset")
  final double     ymu;

  @API(help="Beta vector containing model coefficients.")
  final double []  beta;

  @API(help="Beta vector containing normalized coefficients (coefficients obtained on normalized data).")
  final double []  norm_beta;
  @API(help="offsets of categorical columns into the beta vector. The last value is the offset of the first numerical column.")
  final int    []  catOffsets;
  @API(help="warnings")
  final String []  warnings;
  @API(help="Decision threshold.")
  final double     threshold;
  @API(help="glm params")
  final GLMParams  glm;
  @API(help="beta epsilon - stop iterating when beta diff is below this threshold.")
  final double     beta_eps;
  @API(help="regularization parameter driving proportion of L1/L2 penalty.")
  final double     alpha;
  @API(help="regularization param giving the strength of the applied regularization. high values drive coeffficients to zero.")
  final double     lambda;
  @API(help="number of iterations computed.")
  final int        iteration;
  @API(help="running time of the algo in ms.")
  final long       run_time;
  @API(help="Keys containing computed validations of this model.")
  Key []           validations;

  private static final DecimalFormat DFORMAT = new DecimalFormat("###.####");

  public GLMModel(Key selfKey, Frame fr, GLMParams glm, double beta_eps, double alpha, double lambda,long run_time) {
    super(selfKey,null,fr);
    ymu = 0;
    beta = null;
    norm_beta = null;
    this.glm = glm;
    threshold = 0.5;
    iteration = 0;
    this.catOffsets = null;
    this.warnings = null;
    this.alpha = alpha;
    this.lambda = lambda;
    this.beta_eps = beta_eps;
    this.run_time = run_time;
  }

  public GLMModel(Key selfKey, Key dataKey, int iteration, Frame fr, GLMTask glmt, double beta_eps, double alpha, double lambda, double [] beta, double threshold, String [] warnings, long run_time) {
    super(selfKey, dataKey, fr);
    glm = glmt._glm;
    this.threshold = threshold;
    catOffsets = glmt._catOffsets;
    if(glmt._standardize){
      this.norm_beta = beta;
      // denormalize beta
      this.beta = beta.clone();
      double norm = 0.0;        // Reverse any normalization on the intercept
      // denormalize only the number coefs (categoricals are not normalized)
      final int numoff = beta.length - glmt._nums - 1;
      for( int i=numoff; i< this.beta.length-1; i++ ) {
        double b = this.beta[i]*glmt._normMul[i-numoff];
        norm += b*glmt._normSub[i-numoff]; // Also accumulate the intercept adjustment
        this.beta[i] = b;
      }
      this.beta[beta.length-1] -= norm;
    } else {
      this.beta = beta;
      norm_beta = null;
    }
    final Vec [] vecs = fr.vecs();
    ymu = vecs[vecs.length-1].mean();
    this.iteration = iteration;
    this.warnings = warnings;
    this.alpha = alpha;
    this.lambda = lambda;
    this.beta_eps = beta_eps;
    this.run_time = run_time;
  }
  public GLMValidation validation(){
    GLMValidation res = DKV.get(validations[0]).get();
    return res;
  }
  public double [] beta(){return beta;}
  @Override protected float[] score0(double[] data, float[] preds) {
    double eta = 0.0;
    for(int i = 0; i < catOffsets.length-1; ++i) if(data[i] != 0)
      eta += beta[catOffsets[i] + (int)(data[i]-1)];
    final int noff = catOffsets[catOffsets.length-1] - catOffsets.length + 1;
    for(int i = catOffsets.length-1; i < data.length; ++i)
      eta += beta[noff+i]*data[i];
    eta += beta[beta.length-1]; // add intercept
    double mu = glm.linkInv(eta);
    preds[0] = (float)mu;
    if(glm.family == Family.binomial){ // threshold
      if(preds.length > 1)preds[1] = preds[0];
      preds[0] = preds[0] >= threshold?1:0;
    }
    return preds;
  }

  public final int ncoefs() {return beta.length;}

  // use general score to reduce number of possible different code paths
  public static class GLMValidationTask extends MRTask2<GLMValidationTask>{
    final GLMModel _model;
    GLMValidation _res;
    private final int _offset;
    private final int _step;
    private final boolean _complement;

    public static Key makeKey(){return Key.make("__GLMValidation_" + Key.make().toString());}
    public GLMValidationTask(GLMModel m, int step, int offset,boolean complement){_model = m; _step = step; _offset = offset;_complement = complement;}
    @Override public void map(Chunk [] chunks){
      _res = new GLMValidation(null,_model.ymu,_model.glm,_model.rank());
      final int nrows = chunks[0]._len;
      double [] row   = MemoryManager.malloc8d(_model._names.length);
      float  [] preds = MemoryManager.malloc4f(_model.glm.family == Family.binomial?2:1);
      final int step  = _complement?_step:1;
      final int start = _complement?_offset:0;
      OUTER:
      for(int i = start; i < nrows; i += step){
        if(_step > step && (i % _step) == _offset)continue;
        if(chunks[chunks.length-1].isNA0(i))continue;
        for(int j = 0; j < chunks.length-1; ++j){
          if(chunks[j].isNA0(i))continue OUTER;
          row[j] = chunks[j].at0(i);
        }
        _model.score0(row, preds);
        _res.add(chunks[chunks.length-1].at80(i), _model.glm.family == Family.binomial?preds[1]:preds[0]);
      }
      if(_res.nobs > 0)_res.avg_err /= _res.nobs;
    }
    @Override public void reduce(GLMValidationTask gval){_res.add(gval._res);}
  }

  public void generateHTML(String title, StringBuilder sb) {
    if(title != null && !title.isEmpty())DocGen.HTML.title(sb,title);
    DocGen.HTML.paragraph(sb,"Model Key: "+_selfKey);
    if(beta != null)
      DocGen.HTML.paragraph(sb,water.api.Predict.link(_selfKey,"Predict!"));
    String succ = (warnings == null || warnings.length == 0)?"alert-success":"alert-warning";
    sb.append("<div class='alert " + succ + "'>");
    sb.append(iteration + " iterations computed in ");
    pprintTime(sb, run_time);
    if(warnings != null && warnings.length > 0){
      sb.append("<b>Warnings:</b><ul>");
      for(String w:warnings)sb.append("<li>" + w + "</li>");
      sb.append("</ul>");
    }
    sb.append("</div>");
    sb.append("<h4>Parameters</h4>");
    parm(sb,"family",glm.family);
    parm(sb,"link",glm.link);
    parm(sb,"&epsilon;<sub>&beta;</sub>",beta_eps);
    parm(sb,"&alpha;",alpha);
    parm(sb,"&lambda;",lambda);
    if(beta != null)
      coefs2html(sb);
    if(validations != null && validations.length > 0){
      for(Key k:validations){
        GLMValidation v = DKV.get(k).get();
        v.generateHTML("", sb);
      }
    }
  }
  /**
   * get beta coefficients in a map indexed by name
   * @return
   */
  public HashMap<String,Double> coefficients(){
    String [] names = coefNames();
    HashMap<String, Double> res = new HashMap<String, Double>();
    for(int i = 0; i < beta.length; ++i)res.put(names[i],beta[i]);
    return res;
  }
  public String [] coefNames(){
    final int cats = catOffsets.length-1;
    int k = 0;
    String [] res = new String[beta.length];
    for(int i = 0; i < cats; ++i)
      for(int j = 1; j < _domains[i].length; ++j)
        res[k++] = _names[i] + "." + _domains[i][j];
    final int nums = beta.length-k-1;
    for(int i = 0; i < nums; ++i)
      res[k+i] = _names[cats+i];
    assert k + nums == res.length-1;
    res[k+nums] = "Intercept";
    return res;
  }
  private static void parm( StringBuilder sb, String x, Object... y ) {
    sb.append("<span><b>").append(x).append(": </b>").append(y[0]).append("</span> ");
  }
  private void coefs2html(StringBuilder sb){
    StringBuilder names = new StringBuilder();
    StringBuilder equation = new StringBuilder();
    StringBuilder vals = new StringBuilder();
    StringBuilder normVals = norm_beta == null?null:new StringBuilder();
    String [] cNames = coefNames();
    for(int i = 0; i < cNames.length; ++i){
      names.append("<th>" + cNames[i] + "</th>");
      vals.append("<td>" + beta[i] + "</td>");
      if(i != 0)
        equation.append(beta[i] > 0?" + ":" - ");
      equation.append(DFORMAT.format(Math.abs(beta[i])));
      if(i < (cNames.length-1))
         equation.append("*x[" + cNames[i] + "]");
      if(norm_beta != null) normVals.append("<td>" + norm_beta[i] + "</td>");
    }
    sb.append("<h4>Equation</h4>");
    RString eq = null;
    switch( glm.link ) {
    case identity: eq = new RString("y = %equation");   break;
    case logit:    eq = new RString("y = 1/(1 + Math.exp(-(%equation)))");  break;
    case log:      eq = new RString("y = Math.exp((%equation)))");  break;
    case inverse:  eq = new RString("y = 1/(%equation)");  break;
    case tweedie:  eq = new RString("y = (%equation)^(1 -  )"); break;
    default:       eq = new RString("equation display not implemented"); break;
    }
    eq.replace("equation",equation.toString());
    sb.append("<div style='width:100%;overflow:scroll;'>");
    sb.append("<div><code>" + eq + "</code></div>");
    sb.append("<h4>Coefficients</h4><table class='table table-bordered table-condensed'>");
    sb.append("<tr>" + names.toString() + "</tr>");
    sb.append("<tr>" + vals.toString() + "</tr>");
    sb.append("</table>");
    if(norm_beta != null){
      sb.append("<h4>Normalized Coefficients</h4>" +
      		"<table class='table table-bordered table-condensed'>");
      sb.append("<tr>" + names.toString()    + "</tr>");
      sb.append("<tr>" + normVals.toString() + "</tr>");
      sb.append("</table>");
    }
    sb.append("</div>");
  }
  private void pprintTime(StringBuilder sb, long t){
    long hrs = t / (1000*60*60);
    long minutes = (t -= 1000*60*60*hrs)/(1000*60);
    long seconds = (t -= 1000*60*minutes)/1000;
    t -= 1000*seconds;
    if(hrs > 0)sb.append(hrs + "hrs ");
    if(hrs > 0 || minutes > 0)sb.append(minutes + "min ");
    if(hrs > 0 || minutes > 0 | seconds > 0)sb.append(seconds + "sec ");
    sb.append(t + "msec");
  }
  public String toString(){
    StringBuilder sb = new StringBuilder("GLM Model (key=" + _selfKey + " , trained on " + _dataKey + ", family = " + glm.family + ", link = " + glm.link + ", #iterations = " + iteration + "):\n");
    final int cats = catOffsets.length-1;
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
  public int rank() {
    if( beta == null ) return -1;
    int res = 0; //we do not count intercept so we start from -1
    for( double b : beta ) if( b != 0 ) ++res;
    return res;
  }
  @Override public void delete(){
    if(validations != null) for(Key k:validations)
      DKV.remove(k);
    super.delete();
  }
  public void setValidation(Key k){this.validations = new Key[]{k};}
}
