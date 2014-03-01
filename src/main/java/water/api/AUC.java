package water.api;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import water.MRTask2;
import water.Request2;
import water.UKV;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;

public class AUC extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "", required = true, filter = Default.class)
  public Frame actual;

  @API(help="Column of the actual results (will display vertically)", required=true, filter=actualVecSelect.class)
  public Vec vactual;
  class actualVecSelect extends VecClassSelect { actualVecSelect() { super("actual"); } }

  @API(help = "", required = true, filter = Default.class)
  public Frame predict;

  @API(help="Column of the predicted results (will display horizontally)", required=true, filter=predictVecSelect.class)
  public Vec vpredict;
  class predictVecSelect extends VecClassSelect { predictVecSelect() { super("predict"); } }

  @API(help="domain of the actual response")
  String [] actual_domain;
  @API(help="AUC")
  public double auc;
  @API(help="Best threshold")
  private double best_threshold;

  /* Helper */ private hex.ConfusionMatrix[] _cms;
  /* Helper */ private double[] _thresh;
  /* Helper */ double[] _tprs;
  /* Helper */ double[] _fprs;

  @Override public Response serve() {
    Vec va = null,vp = null;
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
      va = vactual .toEnum(); // always returns TransfVec
      actual_domain = va._domain;
      vp = vpredict;
      // The vectors are from different groups => align them, but properly delete it after computation
      if (!va.group().equals(vp.group())) {
        vp = va.align(vp);
      }
      final int bins = 100;
      _cms = new hex.ConfusionMatrix[bins];
      _thresh = new double[bins];
      _tprs = new double[bins];
      _fprs = new double[bins];
      for( int i=0; i<bins; ++i) {
        _cms[i] = new hex.ConfusionMatrix(2);
        _thresh[i] = (0.5f+i)/bins; //TODO: accurate percentiles
      }
      AUCTask at = new AUCTask(_cms, _thresh, _tprs, _fprs).doAll(va,vp);
      auc = at.getAUC();
      best_threshold = at.getBestThreshold();
      return Response.done(this);
    } catch( Throwable t ) {
      return Response.error(t);
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
    }
  }

  // Compute the AUC via MRTask2
  private static class AUCTask extends MRTask2<AUCTask> {
    /* @OUT AUC */ public double getAUC() { return _auc; }
    private double _auc;
    /* @OUT Best Threshold */ public double getBestThreshold() { return _best_threshold; }
    private double _best_threshold;
    /* Helper */ private hex.ConfusionMatrix[] _cms;
    /* Helper */ private double[] _thresh;
    /* Helper */ double[] _tprs;
    /* Helper */ double[] _fprs;

    AUCTask(hex.ConfusionMatrix[] cms, double[] thresh, double[] tprs, double[] fprs) {
      _cms = cms;
      _thresh = thresh;
      _tprs = tprs;
      _fprs = fprs;
    }

    @Override public void map( Chunk ca, Chunk cp ) {
      final int len = Math.min(ca._len, cp._len);
      for( int i=0; i < len; i++ ) {
        for( int t=0; t < _cms.length; t++ ) {
          final int a = (int)ca.at80(i);
          if (a != 0 && a != 1) throw new InvalidArgumentException("Invalid vactual: must be binary (0 or 1).");
          final int p = cp.at0(i)>=_thresh[t]?1:0;
          _cms[t].add(a, p);
        }
      }
    }

    @Override public void reduce( AUCTask other ) {
      for( int i=0; i<_cms.length; ++i) _cms[i].add(other._cms[i]);
    }

    @Override protected void postGlobal() {
      super.postGlobal();
      double TPR_pre = 1;
      double FPR_pre = 1;
      _auc = 0;
      for( int t = 0; t < _cms.length; ++t ) {
        double TPR = 1 - _cms[t].classErr(1); // =TP/(TP+FN) = true -positive-rate
        double FPR = _cms[t].classErr(0); // =FP/(FP+TN) = false-positive-rate
        _auc += trapezoid_area(FPR_pre, FPR, TPR_pre, TPR);
        TPR_pre = TPR;
        FPR_pre = FPR;
        _tprs[t] = TPR;
        _fprs[t] = FPR;
      }
      _auc += trapezoid_area(FPR_pre, 0, TPR_pre, 0);
      int best = 0;
      for(int i = 1; i < _cms.length; ++i) {
        if(Math.max(_cms[i].classErr(0),_cms[i].classErr(1)) < Math.max(_cms[best].classErr(0),_cms[best].classErr(1)))
          best = i;
      }
      _best_threshold = _thresh[best];
    }

    private double trapezoid_area(double x1, double x2, double y1, double y2) {
      final double base = Math.abs(x1 - x2);
      final double havg = 0.5 * (y1 + y2);
      return base * havg;
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    DocGen.HTML.arrayHead(sb);
    DocGen.HTML.section(sb, "Predicting class " + actual.names()[actual.find(vactual)] + " from probabilities " + predict.names()[predict.find(vpredict)]);
    sb.append("<th>AUC</th><th>Best threshold</th>");
    sb.append("<tr class='warning'>");
    sb.append("<td>" + String.format("%4f", auc) + "</td><td>" + String.format("%4f", best_threshold) + "</td>");
    sb.append("</tr>");
    DocGen.HTML.arrayTail(sb);
    plotROC(sb);
    return true;
  }

  public double toASCII( StringBuilder sb ) {
    sb.append("AUC: " + String.format("%4f", auc));
    sb.append("Best threshold: " + String.format("%4f", best_threshold));
    return auc;
  }

  public void plotROC(StringBuilder sb) {
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
        sb.append("["+String.valueOf(_fprs[c])+",").append(String.valueOf(_tprs[c])).append("]");
      }
      sb.append(", ["+String.valueOf(_fprs[c])+",").append(String.valueOf(_tprs[c])).append("]");
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
}
