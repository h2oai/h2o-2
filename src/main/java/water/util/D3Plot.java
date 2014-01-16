package water.util;

import water.api.RequestBuilders;

public class D3Plot {
  private String link_title = "show plot";
  private String title = "Missing Title";
  private String xaxislabel = "x axis";
  private String yaxislabel = "y axis";
  private int width = 1200;
  private int height = 400;
  private int padding = 40;
  private int font_size = 11;
  private float[] x;
  private float[] y;

  private D3Plot() {
  }

  public D3Plot(float[] x, float[] y) {
    this.x = x;
    this.y = y;
  }

  public D3Plot(float[] x, float[] y, String link_title, String title, String xaxislabel, String yaxislabel, int font_size, int height, int width, int padding) {
    this.link_title = link_title;
    this.title = title;
    this.xaxislabel = xaxislabel;
    this.yaxislabel = yaxislabel;
    this.width = width;
    this.height = height;
    this.padding = padding;
    this.font_size = font_size;
    this.x = x;
    this.y = y;
  }

  public D3Plot(float[] x, float[] y, String xaxislabel, String yaxislabel, String title, String link_title) {
    this.yaxislabel = yaxislabel;
    this.xaxislabel = xaxislabel;
    this.title = title;
    this.link_title = link_title;
    this.x = x;
    this.y = y;
  }

  public D3Plot(float[] x, float[] y, String xaxislabel, String yaxislabel, String title) {
    this.yaxislabel = yaxislabel;
    this.xaxislabel = xaxislabel;
    this.title = title;
    this.x = x;
    this.y = y;
    this.link_title = "Toggle view of plot of " + title;
  }

  public void generate(StringBuilder sb) {
    final String plot = title.replaceAll(" ", "");
    assert(x.length == y.length);
    sb.append("<script type=\"text/javascript\" src='/h2o/js/d3.v3.min.js'></script>");
    sb.append("<span style='display: inline-block;'>");
    //sb.append("<div class=\"pull-left\">");
    sb.append("<a href=\"#\" onclick=\'$(\"#" + "plot" + plot
            + "\").toggleClass(\"hide\");\' class=\'btn btn-inverse btn-mini\'>" + link_title + "</a></div>");
    sb.append("<div class=\"hide\" id=\"" + "plot" + plot + "\">");
    sb.append("<style type=\"text/css\">");
    sb.append(".axis path," +
            ".axis line {\n" +
            "fill: none;\n" +
            "stroke: black;\n" +
            "shape-rendering: crispEdges;\n" +
            "}\n" +
            ".axis text {\n" +
            "font-family: sans-serif;\n" +
            "font-size: " + font_size + "px;\n" +
            "}\n");
    sb.append("</style>");
    sb.append("<div id=\"" + "plot" + plot + "\" style=\"display:inline;\">");
    sb.append("<script type=\"text/javascript\">");
    sb.append("//Width and height\n");
    sb.append("var w = " + width + ";\n"+
            "var h = " + height + ";\n"+
            "var padding = " + padding + ";\n"
    );
    sb.append("var dataset = [");

    for(int c = 0; c < x.length; c++) {
      if (c == 0) {
        sb.append("["+String.valueOf(x[c])+",").append(RequestBuilders.ElementBuilder.format(y[c])).append("]");
      }
      sb.append(", ["+String.valueOf(x[c])+",").append(RequestBuilders.ElementBuilder.format(y[c])).append("]");
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
                    "var svg = d3.select(\"#" + "plot" + plot + "\")\n"+
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
                    "d3.select('#" + "plot" + plot + " svg')"+
                    ".append(\"text\")"+
                    ".attr(\"x\",w/2)"+
                    ".attr(\"y\",h - 5)"+
                    ".attr(\"text-anchor\", \"middle\")"+
                    ".text(\"" + xaxislabel + "\");\n"+

                    "//Create Y axis\n"+
                    "svg.append(\"g\")"+
                    ".attr(\"class\", \"axis\")"+
                    ".attr(\"transform\", \"translate(\" + padding + \",0)\")"+
                    ".call(yAxis);\n"+

                    "//Y axis label\n"+
                    "d3.select('#" + "plot" + plot + " svg')"+
                    ".append(\"text\")"+
                    ".attr(\"x\",150)"+
                    ".attr(\"y\",-2)"+
                    ".attr(\"transform\", \"rotate(90)\")"+
                    //".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
                    ".attr(\"text-anchor\", \"middle\")"+
                    ".text(\"" + yaxislabel + "\");\n"+

                    "//Title\n"+
                    "d3.select('#" + "plot" + plot + " svg')"+
                    ".append(\"text\")"+
                    ".attr(\"x\",w/2)"+
                    ".attr(\"y\",padding - 20)"+
                    ".attr(\"text-anchor\", \"middle\")"+
                    ".text(\"" + title + "\");\n");
    sb.append("</script>");
    sb.append("</div>");
    sb.append("</script>");
    sb.append("</div>");
    //sb.append("</div>");
  }
}
