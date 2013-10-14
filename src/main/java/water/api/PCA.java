package water.api;

import java.util.*;

import hex.*;
import hex.DPCA.*;
import hex.pca.*;
import hex.NewRowVecTask.DataFrame;
import water.*;
import water.util.Log;
import water.util.RString;

import com.google.gson.*;

public class PCA extends Request {
  protected final H2OKey _dest = new H2OKey(DEST_KEY, PCAModel.makeKey());
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  // protected final HexColumnSelect _ignore = new HexPCAColumnSelect(IGNORE, _key);
  protected final HexColumnSelect _x = new HexPCAColumnSelect(X, _key);
  // protected final Int _numPC = new Int("num_pc", 10, 1, MAX_COL);
  protected final Int _maxPC = new Int("max_pc", MAX_COL, 1, MAX_COL);
  protected final Real _tol = new Real("tolerance", 0.0, 0, 1, "Omit components with std dev <= tol times std dev of first component");
  protected final Bool _standardize = new Bool("standardize", true, "Set to standardize (0 mean, unit variance) the data before training.");

  public static final int MAX_COL = 10000;   // Maximum number of columns supported on local PCA

  public PCA() {
    _requestHelp = "Compute principal components of a data set.";
    // _ignore._requestHelp = "A list of ignored columns (specified by name or 0-based index).";
    _x._requestHelp = "A list of columns to analyze (specified by name or 0-based index).";
    // _numPC._requestHelp = "Number of principal components to return.";
    _maxPC._requestHelp = "Maximum number of principal components to return.";
    _tol._requestHelp = "Components omitted if their standard deviations are <= tol times standard deviation of first component.";
  }


  PCAParams getPCAParams() {
    // PCAParams res = new PCAParams(_numPC.value());
    // PCAParams res = new PCAParams(_tol.value(), _standardize.value());
    PCAParams res = new PCAParams(_maxPC.value(), _tol.value(), _standardize.value());
    return res;
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='PCA.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public static String link(Key k, double tol, String content) {
    StringBuilder sb = new StringBuilder("<a href='PCA.query?");
    sb.append(KEY + "=" + k.toString());
    sb.append("&tolerance=" + tol);
    sb.append("'>" + content + "</a>");
    return sb.toString();
  }

  private int[] createColumns(ValueArray ary) {
    BitSet bs = new BitSet();
    // bs.set(0, ary._cols.length);
    // for( int i : _ignore.value() ) bs.clear(i);
    for(int i : _x.value()) bs.set(i);
    int cols[] = new int[bs.cardinality()];
    int idx = 0;
    for(int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1))
      cols[idx++] = i;
    assert idx == cols.length;
    return cols;
  }

  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
    /* if(arg == _ignore) {
      int[] ii = _ignore.value();
      if(ii != null && ii.length >= _key.value()._cols.length)
        throw new IllegalArgumentException("Cannot ignore all columns");

      // Degrees of freedom = number of rows - 1
      int numIgnore = ii == null ? 0 : ii.length;
      if(_key.value() != null && _key.value()._cols.length - numIgnore > _key.value()._numrows - 1)
        throw new IllegalArgumentException("Cannot have more columns than degrees of freedom = " + String.valueOf(_key.value()._numrows-1));
    } */
    if(arg == _x) {
      int[] ii = _x.value();
      if(ii == null) throw new IllegalArgumentException("Cannot ignore all columns");

      // Degrees of freedom = number of rows - 1
      int numSelect = ii == null ? 0 : ii.length;
      if(_key.value() != null && numSelect > _key.value()._numrows - 1)
        throw new IllegalArgumentException("Cannot have more columns than degrees of freedom = " + String.valueOf(_key.value()._numrows-1));
    }
  }

  @Override protected Response serve() {
    try {
      JsonObject j = new JsonObject();
      Key dest = _dest.value();
      ValueArray ary = _key.value();

      PCAParams pcaParams = getPCAParams();
      // int[] cols = new int[ary._cols.length];
      // for( int i = 0; i < cols.length; i++ ) cols[i] = i;
      int[] cols = createColumns(ary);
      if(cols.length > MAX_COL)
        throw new RuntimeException("Cannot run PCA on more than " + MAX_COL + " columns");

      DataFrame data = DataFrame.makePCAData(ary, cols, _standardize.value());
      PCAJob job = DPCA.startPCAJob(dest, data, pcaParams);
      j.addProperty(JOB, job.self().toString());
      j.addProperty(DEST_KEY, job.dest().toString());

      Response r = Progress.redirect(j, job.self(), job.dest());
      r.setBuilder(DEST_KEY, new KeyElementBuilder());
      return r;
    } catch(RuntimeException e) {
      Log.err(e);
      return Response.error(e.getMessage());
    } catch (Throwable t) {
      Log.err(t);
      return Response.error(t.getMessage());
    }
  }

  static class Builder extends ObjectBuilder {
    final PCAModel _m;

    Builder(PCAModel m) {
      _m = m;
    }

    public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();
      modelHTML(_m, json, sb);
      return sb.toString();
    }

    private void modelHTML(PCAModel m, JsonObject json, StringBuilder sb) {
      sb.append("<script type=\"text/javascript\" src='h2o/js/d3.v3.js'></script>");
      sb.append("<div class='alert'>Actions: " + PCAScore.link(m._selfKey, "Score on dataset") + ", "
          + PCA.link(m._dataKey, "Compute new model") + "</div>");
      screevarString(m,sb);
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<tr>");
      sb.append("<th>Feature</th>");

      for(int i = 0; i < m._num_pc; i++)
        sb.append("<th>").append("PC" + i).append("</th>");
      sb.append("</tr>");

      // Row of standard deviation values
      sb.append("<tr class='warning'>");
      // sb.append("<td>").append("&sigma;").append("</td>");
      sb.append("<td>").append("Std Dev").append("</td>");
      for(int c = 0; c < m._num_pc; c++)
        sb.append("<td>").append(ElementBuilder.format(m._sdev[c])).append("</td>");
      sb.append("</tr>");

      // Row with proportion of variance
      sb.append("<tr class='warning'>");
      sb.append("<td>").append("Prop Var").append("</td>");
      for(int c = 0; c < m._num_pc; c++)
        sb.append("<td>").append(ElementBuilder.format(m._propVar[c])).append("</td>");
      sb.append("</tr>");

      // Row with cumulative proportion of variance
      sb.append("<tr class='warning'>");
      sb.append("<td>").append("Cum Prop Var").append("</td>");
      for(int c = 0; c < m._num_pc; c++)
        sb.append("<td>").append(ElementBuilder.format(m._cumVar[c])).append("</td>");
      sb.append("</tr>");

      // Each row is component of eigenvector
      for( int r = 0; r < m._va._cols.length; r++ ) {
        sb.append("<tr>");
        sb.append("<th>").append(m._va._cols[r]._name).append("</th>");
        for( int c = 0; c < m._num_pc; c++ ) {
          double e = m._eigVec[r][c];
          sb.append("<td>").append(ElementBuilder.format(e)).append("</td>");
        }
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }

    public void screevarString(PCAModel m, StringBuilder sb) {
      sb.append("<div class=\"pull-left\"><a href=\"#\" onclick=\'$(\"#scree_var\").toggleClass(\"hide\");\' class=\'btn btn-inverse btn-mini\'>Scree & Variance Plots</a></div>");
      sb.append("<div class=\"hide\" id=\"scree_var\">");
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
      sb.append("<div id=\"scree\" style=\"display:inline;\">");
      sb.append("<script type=\"text/javascript\">");

      sb.append("//Width and height\n");
      sb.append("var w = 500;\n"+
      "var h = 300;\n"+
      "var padding = 40;\n"
      );
      sb.append("var dataset = [");

      for(int c = 0; c < m._num_pc; c++) {
        if (c == 0) {
          sb.append("["+String.valueOf(c+1)+",").append(ElementBuilder.format(m._sdev[c]*m._sdev[c])).append("]");
        }
        sb.append(", ["+String.valueOf(c+1)+",").append(ElementBuilder.format(m._sdev[c]*m._sdev[c])).append("]");
      }
      sb.append("];");

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
      "var svg = d3.select(\"#scree\")\n"+
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
         ".attr(\"r\", function(d) {\n"+
            "return 2;\n"+//rScale(d[1]);\n"+
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
      "d3.select('#scree svg')"+
        ".append(\"text\")"+
        ".attr(\"x\",w/2)"+
        ".attr(\"y\",h - 5)"+
        ".attr(\"text-anchor\", \"middle\")"+
        ".text(\"Principal Component\");\n"+

      "//Create Y axis\n"+
      "svg.append(\"g\")"+
        ".attr(\"class\", \"axis\")"+
        ".attr(\"transform\", \"translate(\" + padding + \",0)\")"+
        ".call(yAxis);\n"+

      "//Y axis label\n"+
      "d3.select('#scree svg')"+
        ".append(\"text\")"+
        ".attr(\"x\",150)"+
        ".attr(\"y\",-5)"+
        ".attr(\"transform\", \"rotate(90)\")"+
        //".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
        ".attr(\"text-anchor\", \"middle\")"+
        ".text(\"Eigenvalue\");\n"+

      "//Title\n"+
      "d3.select('#scree svg')"+
        ".append(\"text\")"+
        ".attr(\"x\",w/2)"+
        ".attr(\"y\",padding - 20)"+
        ".attr(\"text-anchor\", \"middle\")"+
        ".text(\"Scree Plot\");\n");

      sb.append("</script>");
      sb.append("</div>");
      ///////////////////////////////////
      sb.append("<div id=\"var\" style=\"display:inline;\">");
      sb.append("<script type=\"text/javascript\">");

      sb.append("//Width and height\n");
      sb.append("var w = 500;\n"+
      "var h = 300;\n"+
      "var padding = 50;\n"
      );
      sb.append("var dataset = [");

      for(int c = 0; c < m._num_pc; c++) {
        if (c == 0) {
          sb.append("["+String.valueOf(c+1)+",").append(ElementBuilder.format(m._cumVar[c])).append("]");
        }
        sb.append(", ["+String.valueOf(c+1)+",").append(ElementBuilder.format(m._cumVar[c])).append("]");
      }
      sb.append("];");

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
      "var svg = d3.select(\"#var\")\n"+
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
         ".attr(\"r\", function(d) {\n"+
            "return 2;\n"+//rScale(d[1]);\n"+
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
      "d3.select('#var svg')"+
        ".append(\"text\")"+
        ".attr(\"x\",w/2)"+
        ".attr(\"y\",h - 5)"+
        ".attr(\"text-anchor\", \"middle\")"+
        ".text(\"Principal Component\");\n"+

      "//Create Y axis\n"+
      "svg.append(\"g\")"+
        ".attr(\"class\", \"axis\")"+
        ".attr(\"transform\", \"translate(\" + padding + \",0)\")"+
        ".call(yAxis);\n"+

      "//Y axis label\n"+
      "d3.select('#var svg')"+
        ".append(\"text\")"+
        ".attr(\"x\",150)"+
        ".attr(\"y\",-5)"+
        ".attr(\"transform\", \"rotate(90)\")"+
        //".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
        ".attr(\"text-anchor\", \"middle\")"+
        ".text(\"Cumulative Proportion of Variance\");\n"+

      "//Title\n"+
      "d3.select('#var svg')"+
        ".append(\"text\")"+
        ".attr(\"x\",w/2)"+
        ".attr(\"y\",padding-20)"+
        ".attr(\"text-anchor\", \"middle\")"+
        ".text(\"Cumulative Variance Plot\");\n");

      sb.append("</script>");
      sb.append("</div>");
      sb.append("</div>");
      sb.append("<br />");
    }
  }
}
