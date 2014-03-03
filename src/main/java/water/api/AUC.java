package water.api;

import water.MRTask2;
import water.Request2;
import water.UKV;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;

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

  @API(help="domain of the actual response")
  private String [] actual_domain;
  @API(help="AUC")
  public double auc;
  @API(help="F1")
  public double f1;
  @API(help="Threshold for max. F1")
  private float best_thresholdF1;

  //helper
  private int idx_bestF1;

  public double AUC() { return auc; }
  public double err() { return _cms[idx_bestF1].err(); }
  public double F1() { return f1; }
  public double Gini() { return 2*auc-1; }
  public int best_idxF1() { return idx_bestF1; }
  public float best_thresholdF1() { return best_thresholdF1; }

  /* Helper */ private float[] _thresh;
  /* Helper */ private double[] _tprs;
  /* Helper */ private double[] _fprs;
  /* Helper */ private hex.ConfusionMatrix[] _cms;

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

      // make thresholds
      HashSet hs = new HashSet();
      final int bins = (int)Math.min(vpredict.length(), 200l);
      final long stride = Math.max(vpredict.length() / bins, 1);
      for( int i=0; i<bins; ++i) hs.add(new Float(vpredict.at(i*stride))); //data-driven thresholds TODO: use percentiles (from Summary2?)
      for (int i=0;i<51;++i) hs.add(new Float(i/50.)); //always add 0.02-spaced thresholds from 0 to 1

      // created sorted vector of unique thresholds
      _thresh = new float[hs.size()];
      int i=0;
      for (Object h : hs) {_thresh[i++] = (Float)h; }
      sort(_thresh);

      // compute AUC, CMs, and best threshold
      AUCTask at = new AUCTask(_thresh).doAll(va,vp);
      _cms = at.getCMs();
      idx_bestF1 = at.getBestIdxF1();
      _tprs = at.getTPRs();
      _fprs = at.getFPRs();
      auc = at.getAUC();
      f1 = _cms[idx_bestF1].precisionAndRecall();
      best_thresholdF1 = at.getBestThresholdF1();
      return Response.done(this);
    } catch( Throwable t ) {
      return Response.error(t);
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    sb.append("<div>");
    DocGen.HTML.section(sb, "Scoring for Binary Classification");
    DocGen.HTML.arrayHead(sb);
//    DocGen.HTML.section(sb, "Predicting: " + actual.names()[actual.find(vactual)]);
    sb.append("<th>AUC</th><th>Gini</th><th>F1</th><th>Threshold for max. F1</th>");
    sb.append("<tr class='warning'>");
    sb.append("<td>"
            + String.format("%5f", AUC()) + "</td><td>"
            + String.format("%5f", Gini()) + "</td><td>"
            + String.format("%5f", F1()) + "</td><td>"
            + String.format("%g", best_thresholdF1()) + "</td>"
    );
    sb.append("</tr>");
    DocGen.HTML.arrayTail(sb);
    _cms[idx_bestF1].toHTML(sb, actual_domain);
    plotROC(sb);
    _cms[idx_bestF1].toHTMLbasic(sb, actual_domain);
    sb.append("\n<script type=\"text/javascript\">");//</script>");
    sb.append("var cms = [\n");
    for(hex.ConfusionMatrix cm:_cms){
      sb.append("\t[\n");
      for(long [] line:cm._arr) {
        sb.append("\t\t[");
        for(long l:line) sb.append(l + ",");
        sb.append("],\n");
      }
      sb.append("\t],\n");
    }
    sb.append("];\n");
    sb.append("function show_cm(i){\n");
    sb.append("\t" + "document.getElementById('TN').innerHTML = cms[i][0][0];\n");
    sb.append("\t" + "document.getElementById('TP').innerHTML = cms[i][1][1];\n");
    sb.append("\t" + "document.getElementById('FN').innerHTML = cms[i][0][1];\n");
    sb.append("\t" + "document.getElementById('FP').innerHTML = cms[i][1][0];\n");
    sb.append("}\n");
    sb.append("</script>\n");
    sb.append("\n<div><b>Confusion Matrix at decision threshold:</b></div><select id=\"select\" onchange='show_cm(this.value)'>\n");
    for(int i = 0; i < _cms.length; ++i)
      sb.append("\t<option value='" + i + "'" + (_thresh[i] == best_thresholdF1()?"selected='selected'":"") +">" + _thresh[i] + "</option>\n");
    sb.append("</select>\n");
    sb.append("</div>");
    return true;
  }

  public double toASCII( StringBuilder sb ) {
    sb.append("AUC: " + String.format("%5f", AUC()));
    sb.append(", Gini: " + String.format("%5f", Gini()));
    sb.append(", F1: " + String.format("%5f", F1()));
    sb.append(", Best threshold for F1: " + String.format("%g", best_thresholdF1()));
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
    for(int c = 0; c < 2*_fprs.length; c++) {
      sb.append(", ["+String.valueOf(c/(2.0*_fprs.length))+",").append(String.valueOf(c/(2.0*_fprs.length))).append("]");
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
                    "})\n" +
                    ".on(\"mouseover\", function(d,i){\n" +
                    "   if(i <= 100) {" +
                    "     document.getElementById(\"select\").selectedIndex = 100 - i\n" +
                    "     show_cm(i)\n" +
                    "   }\n" +
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

  // Compute the AUC via MRTask2
  private static class AUCTask extends MRTask2<AUCTask> {
    /* @OUT AUC */ public double getAUC() { return _auc; }
    transient private double _auc;
    /* @OUT Best Threshold Idx */ public int getBestIdxF1() { return _best_idxF1; }
    transient private int _best_idxF1;
    /* @OUT Best Threshold */ public float getBestThresholdF1() { return _best_thresholdF1; }
    transient private float _best_thresholdF1;
    /* @OUT TPRs */ public final double[] getTPRs() { return _tprs; }
    transient private double[] _tprs;
    /* @OUT FPRs */ public final double[] getFPRs() { return _fprs; }
    transient private double[] _fprs;
    /* @OUT CMs */ public final hex.ConfusionMatrix[] getCMs() { return _cms; }
    final private hex.ConfusionMatrix[] _cms;


    /* IN thresholds */ final private float[] _thresh;

    AUCTask(float[] thresh) {
      _thresh = thresh.clone();
      _cms = new hex.ConfusionMatrix[_thresh.length];
      for (int i=0;i<_cms.length;++i)
        _cms[i] = new hex.ConfusionMatrix(2);
    }

    @Override public void map( Chunk ca, Chunk cp ) {
      final int len = Math.min(ca._len, cp._len);
      for( int i=0; i < len; i++ ) {
        assert(!ca.isNA0(i)); //should never have actual NaN probability!
        final int a = (int)ca.at80(i); //would be a 0 if double was NaN
        assert (a == 0 || a == 1) : "Invalid vactual: must be binary (0 or 1).";
        if (cp.isNA0(i)) {
          Log.warn("Skipping predicted NaN."); //Fix your score0(): models should never predict NaN!
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
        if (other._cms != _cms) {
          _cms[i].add(other._cms[i]);
        }
      }
    }

    @Override protected void postGlobal() {
      _tprs = new double[_cms.length];
      _fprs = new double[_cms.length];

      double TPR_pre = 1;
      double FPR_pre = 1;
      _auc = 0;
      for( int t = 0; t < _cms.length; ++t ) {
        double TPR = 1 - _cms[t].classErr(1); // =TP/(TP+FN) = true-positive-rate
        double FPR = _cms[t].classErr(0); // =FP/(FP+TN) = false-positive-rate
        _auc += trapezoid_area(FPR_pre, FPR, TPR_pre, TPR);
        TPR_pre = TPR;
        FPR_pre = FPR;
        _tprs[t] = TPR;
        _fprs[t] = FPR;
      }
      _auc += trapezoid_area(FPR_pre, 0, TPR_pre, 0);
      assert(_auc >= 0. && _auc <= 1.0);
      _best_idxF1 = 0;
      _best_thresholdF1 = _thresh[0];
      for(int i = 1; i < _cms.length; ++i) {
        if (_cms[i].precisionAndRecall() > _cms[_best_idxF1].precisionAndRecall()) {
          _best_idxF1 = i;
          _best_thresholdF1 = _thresh[i];
        }
      }
    }

    private static double trapezoid_area(double x1, double x2, double y1, double y2) { return Math.abs(x1-x2)*(y1+y2)/2.; }
  }
}
