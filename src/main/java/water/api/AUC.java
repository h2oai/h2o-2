package water.api;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import hex.ConfusionMatrix;
import org.apache.commons.lang.StringEscapeUtils;
import water.MRTask2;
import water.Request2;
import water.UKV;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;

import java.util.HashSet;

import static java.util.Arrays.sort;

public class AUC extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "", required = true, filter = Default.class, json=true)
  public Frame actual;

  @API(help="Column of the actual results (will display vertically)", required=true, filter=actualVecSelect.class, json=true)
  public Vec vactual;
  class actualVecSelect extends VecClassSelect { actualVecSelect() { super("actual"); } }

  @API(help = "", required = true, filter = Default.class, json=true)
  public Frame predict;

  @API(help="Column of the predicted results (will display horizontally)", required=true, filter=predictVecSelect.class, json=true)
  public Vec vpredict;
  class predictVecSelect extends VecClassSelect { predictVecSelect() { super("predict"); } }

  @API(help = "Thresholds (optional, e.g. 0:1:0.01 or 0.0,0.2,0.4,0.6,0.8,1.0).", required = false, filter = Default.class, json = true)
  public float[] thresholds;

  @API(help = "Criterion for choosing optimal threshold", filter = Default.class, json = true)
  public ThresholdCriterion threshold_criterion = ThresholdCriterion.maximum_F1;

  enum ThresholdCriterion {
    maximum_F1,
    maximum_Accuracy,
    maximum_Precision,
    maximum_Recall,
    maximum_Specificity,
    minimize_max_per_class_Error
  }

  @API(help="domain of the actual response")
  private String [] actual_domain;
  @API(help="AUC")
  public double AUC;

  @API(help="F1 values at thresholds")
  public double[] F1;
  @API(help="Classification errors at thresholds")
  public double[] err;
  @API(help="Threshold criteria")
  String[] threshold_criteria;
  @API(help="Optimal thresholds (for different threshold criteria)")
  private float[] optimal_thresholds;
  @API(help="Confusion Matrices (for different threshold criteria")
  private long[][][] optimal_cms;

  /* Independent on thresholds */
  public double AUC() { return AUC; }
  public double Gini() { return 2*AUC-1; }

  /* Return the metrics for given criterion */
  public double F1(ThresholdCriterion criter) { return F1[criter.ordinal()]; }
  public double err(ThresholdCriterion criter) { return _cms[idxCriter[criter.ordinal()]].err(); }
  public float threshold(ThresholdCriterion criter) { return optimal_thresholds[criter.ordinal()]; }
  public long[][] cm(ThresholdCriterion criter) { return optimal_cms[criter.ordinal()]; }

  /* Return the metrics for chosen threshold criterion */
  public double F1() { return F1(threshold_criterion); }
  public double err() { return err(threshold_criterion); }
  public float threshold() { return threshold(threshold_criterion); }
  public long[][] cm() { return cm(threshold_criterion); }
  public ConfusionMatrix CM() { return _cms[idxCriter[threshold_criterion.ordinal()]]; }

  /* Return the best possible metrics */
  public double bestF1() { return F1(ThresholdCriterion.maximum_F1); }
  public double bestErr() { return err(ThresholdCriterion.maximum_Accuracy); }

  /* Helpers */
  private int[] idxCriter;
  private double[] _tprs;
  private double[] _fprs;
  private hex.ConfusionMatrix[] _cms;

  public AUC() {}

  /**
   * Constructor for algos that make their own CMs
   * @param cms ConfusionMatrices
   * @param thresh Thresholds
   */
  public AUC(hex.ConfusionMatrix[] cms, float[] thresh) {
    assert(_cms.length == thresholds.length);
    _cms = cms;
    thresholds = thresh;
  }

  @Override public Response serve() {
    Vec va = null, vp;
    // Input handling
    if( vactual==null || vpredict==null )
      throw new IllegalArgumentException("Missing actual or predict!");
    if (vactual.length() != vpredict.length())
      throw new IllegalArgumentException("Both arguments must have the same length!");
    if (!vactual.isInt())
      throw new IllegalArgumentException("Actual column must be integer class labels!");
    if (vpredict.isInt())
      throw new IllegalArgumentException("Predicted column must be a floating point probability!");

    try {
      va = vactual.toEnum(); // always returns TransfVec
      actual_domain = va._domain;
      vp = vpredict;
      // The vectors are from different groups => align them, but properly delete it after computation
      if (!va.group().equals(vp.group())) {
        vp = va.align(vp);
      }

      // compute thresholds, if not user-given
      if (thresholds == null) {
        HashSet hs = new HashSet();
        final int bins = (int)Math.min(vpredict.length(), 200l);
        final long stride = Math.max(vpredict.length() / bins, 1);
        for( int i=0; i<bins; ++i) hs.add(new Float(vpredict.at(i*stride))); //data-driven thresholds TODO: use percentiles (from Summary2?)
        for (int i=0;i<51;++i) hs.add(new Float(i/50.)); //always add 0.02-spaced thresholds from 0 to 1

        // created sorted vector of unique thresholds
        thresholds = new float[hs.size()];
        int i=0;
        for (Object h : hs) {thresholds[i++] = (Float)h; }
        sort(thresholds);
      }
      // compute CMs
      if (_cms == null) {
        AUCTask at = new AUCTask(thresholds).doAll(va,vp);
        _cms = at.getCMs();
      }
      // compute AUC and best thresholds
      computeAUC();
      findBestThresholds();
      return Response.done(this);
    } catch( Throwable t ) {
      return Response.error(t);
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
    }
  }


  private static double trapezoid_area(double x1, double x2, double y1, double y2) { return Math.abs(x1-x2)*(y1+y2)/2.; }

  private void computeAUC() {
    _tprs = new double[_cms.length];
    _fprs = new double[_cms.length];
    double TPR_pre = 1;
    double FPR_pre = 1;
    AUC = 0;
    for( int t = 0; t < _cms.length; ++t ) {
      double TPR = 1 - _cms[t].classErr(1); // =TP/(TP+FN) = true-positive-rate
      double FPR = _cms[t].classErr(0); // =FP/(FP+TN) = false-positive-rate
      AUC += trapezoid_area(FPR_pre, FPR, TPR_pre, TPR);
      TPR_pre = TPR;
      FPR_pre = FPR;
      _tprs[t] = TPR;
      _fprs[t] = FPR;
    }
    AUC += trapezoid_area(FPR_pre, 0, TPR_pre, 0);
    assert(AUC > -1e-5 && AUC < 1.+1e-5); //check numerical sanity
    AUC = Math.max(0., Math.min(AUC, 1.)); //clamp to 0...1
  }

  /* return true if a is better than b with respect to criterion criter */
  private boolean isBetter(ConfusionMatrix a, ConfusionMatrix b, ThresholdCriterion criter) {
    if (criter == ThresholdCriterion.maximum_F1) {
      return (!Double.isNaN(a.F1()) &&
              (Double.isNaN(b.F1()) || a.F1() > b.F1()));
    } else if (criter == ThresholdCriterion.maximum_Recall) {
      return (!Double.isNaN(a.recall()) &&
              (Double.isNaN(b.recall()) || a.recall() > b.recall()));
    } else if (criter == ThresholdCriterion.maximum_Precision) {
      return (!Double.isNaN(a.precision()) &&
              (Double.isNaN(b.precision()) || a.precision() > b.precision()));
    } else if (criter == ThresholdCriterion.maximum_Accuracy) {
      return a.accuracy() > b.accuracy();
    } else if (criter == ThresholdCriterion.minimize_max_per_class_Error) {
      return (Math.max(a.classErr(0),a.classErr(1)) < Math.max(b.classErr(0), b.classErr(1)));
    } else if (criter == ThresholdCriterion.maximum_Specificity) {
      return (!Double.isNaN(a.specificity()) &&
              (Double.isNaN(b.specificity()) || a.specificity() > b.specificity()));
    }
    else {
      throw new InvalidArgumentException("Unknown threshold criterion.");
    }
  }

  private void findBestThresholds() {
    threshold_criteria = new String[ThresholdCriterion.values().length];
    int i=0;
    HashSet<ThresholdCriterion> hs = new HashSet<ThresholdCriterion>();
    for (ThresholdCriterion criter : ThresholdCriterion.values()) {
      hs.add(criter);
      threshold_criteria[i++] = criter.toString().replace("_", " ");
    }
    optimal_cms = new long[hs.size()][][];
    idxCriter = new int[hs.size()];
    optimal_thresholds = new float[hs.size()];
    F1 = new double[hs.size()];
    err = new double[hs.size()];

    for (ThresholdCriterion criter : hs) {
      final int id = criter.ordinal();
      idxCriter[id] = 0;
      optimal_thresholds[id] = thresholds[0];
      for(i = 1; i < _cms.length; ++i) {
        if (isBetter(_cms[i], _cms[idxCriter[id]], criter)) {
          idxCriter[id] = i;
          optimal_thresholds[id] = thresholds[i];
        }
      }
      // Set members for JSON
      optimal_cms[id] = _cms[idxCriter[id]]._arr;
      F1[id] = _cms[idxCriter[id]].F1();
      err[id] = _cms[idxCriter[id]].err();
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    sb.append("<div>");
    DocGen.HTML.section(sb, "Scoring for Binary Classification");

    // data for JS
    sb.append("\n<script type=\"text/javascript\">");//</script>");
    sb.append("var cms = [\n");
    for(hex.ConfusionMatrix cm:_cms){
      StringBuilder tmp = new StringBuilder();
      cm.toHTML(tmp, actual_domain);
      sb.append("\t'" + StringEscapeUtils.escapeJavaScript(tmp.toString()) + "',\n");
    }
    sb.append("];\n");
    sb.append("var criterion = " + threshold_criterion.ordinal() + ";\n"); //which one
    sb.append("var criteria = ["); for(String c:threshold_criteria) sb.append("\"" + c + "\","); sb.append(" ];\n");
    sb.append("var thresholds = ["); for(double t:optimal_thresholds) sb.append((float)t + ","); sb.append(" ];\n");
    sb.append("var F1 = ["); for(double f:F1) sb.append((float)f + ","); sb.append(" ];\n");
    sb.append("var Err = ["); for(double e:err) sb.append((float)e + ","); sb.append(" ];\n");
    sb.append("var idxCriter = ["); for(int i:idxCriter) sb.append(i + ","); sb.append(" ];\n");
    sb.append("</script>\n");

    // Selection of threshold criterion
    sb.append("\n<div><b>Threshold criterion:</b></div><select id='threshold_select' onchange='set_criterion(this.value, idxCriter[this.value])'>\n");
    for(int i = 0; i < threshold_criteria.length; ++i)
      sb.append("\t<option value='" + i + "'" + (i == threshold_criterion.ordinal()?"selected='selected'":"") +">" + threshold_criteria[i] + "</option>\n");
    sb.append("</select>\n");
    sb.append("</div>");

    DocGen.HTML.arrayHead(sb);
    sb.append("<th>AUC</th><th>Gini</th><th>F1</th><th id='threshold_criterion'>Threshold for " + threshold_criterion.toString().replace("_", " ") + "</th>");
    sb.append("<tr class='warning'>");
    sb.append("<td>" + String.format("%.5f", AUC()) + "</td>"
            + "<td>" + String.format("%.5f", Gini()) + "</td>"
            + "<td id='F1_value'>" + String.format("%.5f", F1()) + "</td>"
            + "<td id='threshold'>" + String.format("%g", threshold()) + "</td>");
    DocGen.HTML.arrayTail(sb);
    sb.append("<div id='BestConfusionMatrix'>");
    CM().toHTML(sb, actual_domain);
    sb.append("</div>");


    sb.append("<table><tr><td>");
    plotROC(sb);
    sb.append("</td><td id='ConfusionMatrix'>");
    CM().toHTML(sb, actual_domain);
    sb.append("</td></tr>");
    sb.append("<tr><td><h5>Threshold:</h5></div><select id=\"select\" onchange='show_cm(this.value)'>\n");
    for(int i = 0; i < _cms.length; ++i)
      sb.append("\t<option value='" + i + "'" + (thresholds[i] == threshold()?"selected='selected'":"") +">" + thresholds[i] + "</option>\n");
    sb.append("</select></td></tr>");
    sb.append("</table>");


    sb.append("\n<script type=\"text/javascript\">");
    sb.append("function show_cm(i){\n");
    sb.append("\t" + "document.getElementById('ConfusionMatrix').innerHTML = cms[i];\n");
    sb.append("\t" + "update(dataset);\n");
    sb.append("}\n");
    sb.append("function set_criterion(i, idx){\n");
    sb.append("\t" + "criterion = i;\n");
    sb.append("\t" + "document.getElementById('BestConfusionMatrix').innerHTML = cms[idx];\n");
    sb.append("\t" + "document.getElementById('threshold_criterion').innerHTML = \" Threshold for \" + criteria[i];\n");
    sb.append("\t" + "document.getElementById('F1_value').innerHTML = F1[i];\n");
    sb.append("\t" + "document.getElementById('threshold').innerHTML = thresholds[i];\n");
    sb.append("\t" + "show_cm(idx);\n");
    sb.append("\t" + "document.getElementById(\"select\").selectedIndex = idx;\n");
    sb.append("\t" + "update(dataset);\n");
    sb.append("}\n");
    sb.append("</script>\n");


    return true;
  }

  public double toASCII( StringBuilder sb ) {
    sb.append(CM().toString());
    sb.append("AUC: " + String.format("%.5f", AUC()));
    sb.append(", Gini: " + String.format("%.5f", Gini()));
    sb.append(", F1: " + String.format("%.5f", F1()));
    sb.append(", Threshold for " + threshold_criterion.toString().replace("_", " ") + ": " + String.format("%g", threshold()));
    sb.append(", Classification Error: " + String.format("%.5f", err()));
    return AUC();
  }

  void plotROC(StringBuilder sb) {
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
    for(int c = 0; c < _fprs.length; c++) {
      assert(_tprs.length == _fprs.length);
      if (c == 0) {
        sb.append("["+String.valueOf(_fprs[c])+",").append(String.valueOf(_tprs[c])).append("]");
      }
      sb.append(", ["+String.valueOf(_fprs[c])+",").append(String.valueOf(_tprs[c])).append("]");
    }
    //diagonal
    for(int c = 0; c < 200; c++) {
      sb.append(", ["+String.valueOf(c/200.)+",").append(String.valueOf(c/200.)).append("]");
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
                    ".text(\"ROC\");\n" +

                    "function update(dataset) {" +
                    "svg.selectAll(\"circle\").remove();" +

                    "//Create circles\n"+
                    "var data = svg.selectAll(\"circle\")"+
                    ".data(dataset);\n"+

                    "var activeIdx = idxCriter[criterion];\n" +

                    "data.enter()\n"+
                    ".append(\"circle\")\n"+
                    ".attr(\"cx\", function(d) {\n"+
                    "return xScale(d[0]);\n"+
                    "})\n"+
                    ".attr(\"cy\", function(d) {\n"+
                    "return yScale(d[1]);\n"+
                    "})\n"+
                    ".attr(\"fill\", function(d,i) {\n"+
                    "  if (document.getElementById(\"select\") != null && i == document.getElementById(\"select\").selectedIndex && i != activeIdx) {\n" +
                    "    return \"blue\"\n" +
                    "  }\n" +
                    "  else if (i == activeIdx) {\n"+
                    "    return \"green\"\n"+
                    "  }\n" +
                    "  else if (d[0] != d[1] && d[0] != 0) {\n"+
                    "    return \"blue\"\n"+
                    "  }\n" +
                    "  else {\n"+
                    "    return \"red\"\n"+
                    "  }\n"+
                    "})\n"+
                    ".attr(\"r\", function(d,i) {\n"+
                    "  if (document.getElementById(\"select\") != null && i == document.getElementById(\"select\").selectedIndex && i != activeIdx) {\n" +
                    " return 5\n" +
                    "  }\n" +
                    "  else if (i == activeIdx) {\n"+
                    "    return 7\n"+
                    "  }\n" +
                    "  else if (d[0] != d[1] && d[0] != 0) {\n"+
                    "    return 2\n"+
                    "  }\n"+
                    "  else {\n"+
                    "    return 1\n"+
                    "  }\n" +
                    "})\n" +
                    ".on(\"mouseover\", function(d,i){\n" +
                    "   if(i <= " + _fprs.length + ") {" +
                    "     document.getElementById(\"select\").selectedIndex = i\n" +
                    "     show_cm(i)\n" +
                    "   }\n" +
                    "});\n"+
                    "data.exit().remove();" +
                    "}\n" +

                    "update(dataset);");

    sb.append("</script>");
    sb.append("</div>");
  }

  // Compute CMs for different thresholds via MRTask2
  private static class AUCTask extends MRTask2<AUCTask> {
    /* @OUT CMs */ public final hex.ConfusionMatrix[] getCMs() { return _cms; }
    private hex.ConfusionMatrix[] _cms;

    /* IN thresholds */ final private float[] _thresh;

    AUCTask(float[] thresh) {
      _thresh = thresh.clone();
    }

    @Override public void map( Chunk ca, Chunk cp ) {
      _cms = new hex.ConfusionMatrix[_thresh.length];
      for (int i=0;i<_cms.length;++i)
        _cms[i] = new hex.ConfusionMatrix(2);
      final int len = Math.min(ca._len, cp._len);
      for( int i=0; i < len; i++ ) {
        assert(!ca.isNA0(i)); //should never have actual NaN probability!
        final int a = (int)ca.at80(i); //would be a 0 if double was NaN
        assert (a == 0 || a == 1) : "Invalid vactual: must be binary (0 or 1).";
        if (cp.isNA0(i)) {
//          Log.warn("Skipping predicted NaN."); //some models predict NaN!
          continue;
        }
        for( int t=0; t < _cms.length; t++ ) {
          final int p = cp.at0(i)>=_thresh[t]?1:0;
          _cms[t].add(a, p);
        }
      }
    }

    @Override public void reduce( AUCTask other ) {
      for( int i=0; i<_cms.length; ++i) {
        _cms[i].add(other._cms[i]);
      }
    }
  }
}
