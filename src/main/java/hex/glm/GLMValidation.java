package hex.glm;

import hex.ConfusionMatrix;
import hex.glm.GLMParams.Family;

import java.text.DecimalFormat;

import water.*;
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
  @API(help="")
  ConfusionMatrix [] _cms;
  @API(help="")
  final GLMParams _glm;
  @API(help="")
  final private int _rank;


  private static final DecimalFormat DFORMAT = new DecimalFormat("##.##");

  public static class GLMXValidation extends GLMValidation {
    Key [] _xvalModels;
    public GLMXValidation(GLMModel mainModel, GLMModel [] xvalModels, int lambdaIdx) {
      super(mainModel._dataKey, mainModel.ymu, mainModel.glm, mainModel.rank(lambdaIdx));
      _xvalModels = new Key[xvalModels.length];
      for(int i = 0; i < xvalModels.length; ++i){
        add(xvalModels[i].validation());
        _xvalModels[i] = xvalModels[i]._selfKey;
      }
      finalize_AIC_AUC();
    }
    @Override public void generateHTML(String title, StringBuilder sb) {
      super.generateHTML(_xvalModels.length + "-fold Cross Validation", sb);
      // add links to the xval models
      sb.append("<h4>Cross Validation Models</h4>");
      sb.append("<table class='table table-bordered table-condensed'>");
      int i = 0;
      for(Key k:_xvalModels){
        sb.append("<tr>");
        sb.append("<td>" + GLMModelView.link("Model" + ++i, k) + "</td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
    }
  }
  public GLMValidation(Key dataKey, double ymu, GLMParams glm, int rank){
    _rank = rank;
    _ymu = ymu;
    _glm = glm;
    if(_glm.family == Family.binomial){
      _cms = new ConfusionMatrix[DEFAULT_THRESHOLDS.length];
      for(int i = 0; i < _cms.length; ++i)
        _cms[i] = new ConfusionMatrix(2);
    }
    this.dataKey = dataKey;
  }
  protected void regularize(double reg){
    avg_err = Math.sqrt(avg_err)*reg;
  }

  public static Key makeKey(){return Key.make("__GLMValidation_" + Key.make());}
  public void add(double yreal, double ymodel){
    null_deviance += _glm.deviance(yreal, _ymu);
    if(_glm.family == Family.binomial) // clasification -> update confusion matrix too
      for(int i = 0; i < DEFAULT_THRESHOLDS.length; ++i)
        _cms[i].add((int)yreal, (ymodel >= DEFAULT_THRESHOLDS[i])?1:0);
    if(Double.isNaN(_glm.deviance(yreal, ymodel)))
      System.out.println("NaN from yreal=" + yreal + ", ymodel=" + ymodel);
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
    computeAIC();
    if(_glm.family == Family.binomial)computeAUC();
  }
  /**
   * Computes area under the ROC curve. The ROC curve is computed from the confusion matrices
   * (there is one for each computed threshold). Area under this curve is then computed as a sum
   * of areas of trapezoids formed by each neighboring points.
   *
   * @return estimate of the area under ROC curve of this classifier.
   */
  static double[] tprs;
  static double[] fprs;
  protected void computeAUC() {
    if( _cms == null ) return;
    tprs = new double[_cms.length];
    fprs = new double[_cms.length];
    double auc = 0;           // Area-under-ROC
    double TPR_pre = 1;
    double FPR_pre = 1;
    for( int t = 0; t < _cms.length; ++t ) {
      double TPR = 1 - _cms[t].classErr(1); // =TP/(TP+FN) = true -positive-rate
      double FPR = _cms[t].classErr(0); // =FP/(FP+TN) = false-positive-rate
      auc += trapeziod_area(FPR_pre, FPR, TPR_pre, TPR);
      TPR_pre = TPR;
      FPR_pre = FPR;
      tprs[t] = TPR;
      fprs[t] = FPR;
    }
    auc += trapeziod_area(FPR_pre, 0, TPR_pre, 0);
    this.auc = Math.round(1000*auc)*0.001;
    if(_glm.family == Family.binomial){
      int best = 0;
      for(int i = 1; i < _cms.length; ++i){
        if(Math.max(_cms[i].classErr(0),_cms[i].classErr(1)) < Math.max(_cms[best].classErr(0),_cms[best].classErr(1)))
          best = i;
      }
      best_threshold = best*0.01f;
    }
  }

  private double trapeziod_area(double x1, double x2, double y1, double y2) {
    double base = Math.abs(x1 - x2);
    double havg = 0.5 * (y1 + y2);
    return base * havg;
  }

  public void generateHTML(String title, StringBuilder sb) {
    sb.append("<h4>" + title + "</h4>");
    sb.append("<table class='table table-striped table-bordered table-condensed'>");
    final long null_dof = nobs-1, res_dof = Math.max(0,nobs-_rank-1);
    sb.append("<tr><th>Degrees of freedom:</th><td>" + null_dof + " total (i.e. Null); " + res_dof + " Residual</td></tr>");
    sb.append("<tr><th>Null Deviance</th><td>" + null_deviance + "</td></tr>");
    sb.append("<tr><th>Residual Deviance</th><td>" + residual_deviance + "</td></tr>");
    sb.append("<tr><th>AIC</th><td>" + aic() + "</td></tr>");
    sb.append("<tr><th>Training Error Rate Avg</th><td>" + avg_err + "</td></tr>");
    if(_glm.family == Family.binomial)sb.append("<tr><th>AUC</th><td>" + DFORMAT.format(auc()) + "</td></tr>");
    sb.append("</table>");

    if(_glm.family == Family.binomial){
      sb.append("<span><b>ROC curve</b></span>");
      ROCc(sb);
      int best = (int)(100*best_threshold);
      sb.append("<span><b>Confusion Matrix at decision threshold:</b></span><span>" + DEFAULT_THRESHOLDS[best] + "</span>");
      confusionHTML(_cms[best], sb);
    }
  }

  private static void cmRow( StringBuilder sb, String hd, double c0, double c1, double cerr ) {
    sb.append("<tr><th>").append(hd).append("</th><td>");
    if( !Double.isNaN(c0)) sb.append(DFORMAT.format(c0));
    sb.append("</td><td>");
    if( !Double.isNaN(c1)) sb.append(DFORMAT.format(c1));
    sb.append("</td><td>");
    if( !Double.isNaN(cerr)) sb.append(DFORMAT.format(cerr));
    sb.append("</td></tr>");
  }

  private static void confusionHTML( hex.ConfusionMatrix cm, StringBuilder sb) {
    if( cm == null ) return;
    sb.append("<table class='table table-bordered table-condensed'>");
    sb.append("<tr><th>Actual / Predicted</th><th>false</th><th>true</th><th>Err</th></tr>");
    double err0 = cm._arr[0][1]/(double)(cm._arr[0][0]+cm._arr[0][1]);
    cmRow(sb,"false",cm._arr[0][0],cm._arr[0][1],err0);
    double err1 = cm._arr[1][0]/(double)(cm._arr[1][0]+cm._arr[1][1]);
    cmRow(sb,"true ",cm._arr[1][0],cm._arr[1][1],err1);
    double err2 = cm._arr[1][0]/(double)(cm._arr[0][0]+cm._arr[1][0]);
    double err3 = cm._arr[0][1]/(double)(cm._arr[0][1]+cm._arr[1][1]);
    cmRow(sb,"Err ",err2,err3,cm.err());
    sb.append("</table>");
  }

  static double[] DEFAULT_THRESHOLDS = new double[] { 0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10,
    0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29,
    0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39, 0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48,
    0.49, 0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67,
    0.68, 0.69, 0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86,
    0.87, 0.88, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00 };

  public void ROCc(StringBuilder sb) {
    sb.append("<script type=\"text/javascript\" src='/h2o/js/d3.v3.min.js'></script>");
    sb.append("<div id=\"ROC\">");
    sb.append("<style type=\"text/css\">");
    sb.append(".axis path," +
            ".axis line {\n" +
            "fill: none;\n" +
            "stroke: black;\n" +
            "shape-rendering: crispEdges;\n" +
            "}\n" +

            ".axis text {\n" +
            "font-family: sans-serif;\n" +
            "font-size: 11px;\n" +
            "}\n");

    sb.append("</style>");
    sb.append("<div id=\"rocCurve\" style=\"display:inline;\">");
    sb.append("<script type=\"text/javascript\">");

    sb.append("//Width and height\n");
    sb.append("var w = 500;\n"+
            "var h = 300;\n"+
            "var padding = 40;\n"
    );
    sb.append("var dataset = [");

    for(int c = 0; c < _cms.length; c++) {
      if (c == 0) {
        sb.append("["+String.valueOf(fprs[c])+",").append(String.valueOf(tprs[c])).append("]");
      }
      sb.append(", ["+String.valueOf(fprs[c])+",").append(String.valueOf(tprs[c])).append("]");
    }
    for(int c = 0; c < 2*_cms.length; c++) {
        sb.append(", ["+String.valueOf(c/(2.0*_cms.length))+",").append(String.valueOf(c/(2.0*_cms.length))).append("]");
    }
    sb.append("];\n");

    sb.append(
            "//Create scale functions\n"+  
                    "var xScale = d3.scale.linear()\n"+
                    ".domain([0, d3.max(dataset, function(d) { return d[0]; })])\n"+
                    ".range([padding, w - padding * 2]);\n"+

                    "var yScale = d3.scale.linear()"+
                    ".domain([0, d3.max(dataset, function(d) { return d[1]; })])\n"+
                    ".range([h - padding, padding]);\n"+

                    "var rScale = d3.scale.linear()"+
                    ".domain([0, d3.max(dataset, function(d) { return d[1]; })])\n"+
                    ".range([2, 5]);\n"+

                    "//Define X axis\n"+
                    "var xAxis = d3.svg.axis()\n"+
                    ".scale(xScale)\n"+
                    ".orient(\"bottom\")\n"+
                    ".ticks(5);\n"+

                    "//Define Y axis\n"+
                    "var yAxis = d3.svg.axis()\n"+
                    ".scale(yScale)\n"+
                    ".orient(\"left\")\n"+
                    ".ticks(5);\n"+

                    "//Create SVG element\n"+
                    "var svg = d3.select(\"#rocCurve\")\n"+
                    ".append(\"svg\")\n"+
                    ".attr(\"width\", w)\n"+
                    ".attr(\"height\", h);\n"+

                    "//Create circles\n"+
                    "svg.selectAll(\"circle\")\n"+
                    ".data(dataset)\n"+
                    ".enter()\n"+
                    ".append(\"circle\")\n"+
                    ".attr(\"cx\", function(d) {\n"+
                    "return xScale(d[0]);\n"+
                    "})\n"+
                    ".attr(\"cy\", function(d) {\n"+
                    "return yScale(d[1]);\n"+
                    "})\n"+
                    ".attr(\"fill\", function(d) {\n"+
                    "  if (d[0] == d[1]) {\n"+
                    "    return \"red\"\n"+
                    "  } else {\n"+
                    "  return \"blue\"\n"+
                    "  }\n"+
                    "})\n"+
                    ".attr(\"r\", function(d) {\n"+
                    "  if (d[0] == d[1]) {\n"+
                    "    return 1\n"+
                    "  } else {\n"+
                    "  return 2\n"+
                    "  }\n"+
                    "});\n"+
                    
                    "/*"+
                    "//Create labels\n"+
                    "svg.selectAll(\"text\")"+
                    ".data(dataset)"+
                    ".enter()"+
                    ".append(\"text\")"+
                    ".text(function(d) {"+
                    "return d[0] + \",\" + d[1];"+
                    "})"+
                    ".attr(\"x\", function(d) {"+
                    "return xScale(d[0]);"+
                    "})"+
                    ".attr(\"y\", function(d) {"+
                    "return yScale(d[1]);"+
                    "})"+
                    ".attr(\"font-family\", \"sans-serif\")"+
                    ".attr(\"font-size\", \"11px\")"+
                    ".attr(\"fill\", \"red\");"+
                    "*/\n"+

                    "//Create X axis\n"+
                    "svg.append(\"g\")"+
                    ".attr(\"class\", \"axis\")"+
                    ".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
                    ".call(xAxis);\n"+

                    "//X axis label\n"+
                    "d3.select('#rocCurve svg')"+
                    ".append(\"text\")"+
                    ".attr(\"x\",w/2)"+
                    ".attr(\"y\",h - 5)"+
                    ".attr(\"text-anchor\", \"middle\")"+
                    ".text(\"False Positive Rate\");\n"+

                    "//Create Y axis\n"+
                    "svg.append(\"g\")"+
                    ".attr(\"class\", \"axis\")"+
                    ".attr(\"transform\", \"translate(\" + padding + \",0)\")"+
                    ".call(yAxis);\n"+

                    "//Y axis label\n"+
                    "d3.select('#rocCurve svg')"+
                    ".append(\"text\")"+
                    ".attr(\"x\",150)"+
                    ".attr(\"y\",-5)"+
                    ".attr(\"transform\", \"rotate(90)\")"+
                    //".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
                    ".attr(\"text-anchor\", \"middle\")"+
                    ".text(\"True Positive Rate\");\n"+

                    "//Title\n"+
                    "d3.select('#rocCurve svg')"+
                    ".append(\"text\")"+
                    ".attr(\"x\",w/2)"+
                    ".attr(\"y\",padding - 20)"+
                    ".attr(\"text-anchor\", \"middle\")"+
                    ".text(\"ROC\");\n");

    sb.append("</script>");
    sb.append("</div>");

  }
}
