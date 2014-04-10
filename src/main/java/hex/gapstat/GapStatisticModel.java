package hex.gapstat;

import hex.FrameTask;
import hex.nb.NaiveBayes;
import water.Job;
import water.Key;
import water.Model;
import water.api.DocGen;
import water.api.Predict;
import water.api.Request.API;
import water.api.RequestBuilders;
import water.fvec.Frame;


public class GapStatisticModel extends Model implements Job.Progress {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Number of clusters to build in each iteration.")
  final int ks;

  @API(help = "The initial pooled within cluster sum of squares for each iteration.")
  final double[] wks;

  @API(help = "The log of the Wks.")
  final double[] wkbs;

  @API(help = "The standard error from the Monte Carlo simulated data for each iteration.")
  final double[] sk;

  @API(help = "k_max.")
  final int k_max;

  @API(help = "b_max.")
  final int b_max;

  @API(help = "The current value of k_max: (2 <= k <= k_max).")
  int k;

  @API(help = "The current value of B (1 <= b <= B.")
  int b;

  public GapStatisticModel(Key selfKey, Key dataKey, Frame fr, int ks, double[] wks, double[] log_wks, double[] sk, int k_max, int b_max, int k, int b) {
    super(selfKey, dataKey, fr);
    this.ks = ks;
    this.wks = wks;
    this.wkbs = log_wks;
    this.sk = sk;
    this.k_max = k_max;
    this.b_max = b_max;
    this.k = k;
    this.b = b;
  }

  public double[] wks() { return wks; }
  public double[] wkbs() { return wkbs; }
  public double[] sk() {return sk; }

  @Override
  public float progress() {
    float p1 = (float) ((double) (k - 1) / (double) k_max);
    float p2 = (float) (( (double) (k - 1) /  (double) k_max ) +  (double) b / (double) ( b_max * k_max ));
    return  Math.min(p1, p2);
  }

  @Override protected float[] score0(double[] data, float[] preds) {
    throw new UnsupportedOperationException();
  }

  @Override public void delete() { super.delete(); }

  @Override public String toString(){
    StringBuilder sb = new StringBuilder("Gap Statistic Model (key=" + _key + " , trained on " + _dataKey + "):\n");
    return sb.toString();
  }

  public void generateHTML(String title, StringBuilder sb) {
    if(title != null && !title.isEmpty()) DocGen.HTML.title(sb, title);
    DocGen.HTML.paragraph(sb, "Model Key: " + _key);
//    sb.append("<div class='alert'>Actions: " + Predict.link(_key, "Predict on dataset") + ", "
//            + NaiveBayes.link(_dataKey, "Compute new model") + "</div>");

    DocGen.HTML.section(sb, "Gap Statistic Output:");

    //Log Pooled Variances...
    DocGen.HTML.section(sb, "Log of the Pooled Cluster Within Sum of Squares per value of k");
    sb.append("<span style='display: inline-block;'>");
    sb.append("<table class='table table-striped table-bordered'>");

    double[] log_wks = wks();

    sb.append("<tr>");
    for (int i = 0; i <log_wks.length; ++i) {
      if (log_wks[i] == 0) continue;
      sb.append("<th>").append(i).append("</th>");
    }
    sb.append("</tr>");

    sb.append("<tr>");
    for (int i = 0; i < log_wks.length; ++i) {
      if (log_wks[i] == 0) continue;
      sb.append("<td>").append(log_wks[i]).append("</td>");
    }
    sb.append("</tr>");
    sb.append("</table></span>");


    //Monte Carlo Bootstrap averages
    DocGen.HTML.section(sb, "Monte Carlo Bootstrap Replicate Averages of the Log of the Pooled Cluster Within SS per value of k");
    sb.append("<span style='display: inline-block;'>");
    sb.append("<table class='table table-striped table-bordered'>");

    double[] log_wkbs = wkbs();

    sb.append("<tr>");
    for (int i = 0; i <log_wkbs.length; ++i) {
      if (log_wkbs[i] == 0) continue;
      sb.append("<th>").append(i).append("</th>");
    }
    sb.append("</tr>");

    sb.append("<tr>");
    for (int i = 0; i < log_wkbs.length; ++i) {
      if (log_wkbs[i] == 0) continue;
      sb.append("<td>").append(log_wkbs[i]).append("</td>");
    }
    sb.append("</tr>");
    sb.append("</table></span>");

    //standard errors
    DocGen.HTML.section(sb, "Standard Error for the Monte Carlo Bootstrap Replicate Averages of the Log of the Pooled Cluster Within SS per value of k");
    sb.append("<span style='display: inline-block;'>");
    sb.append("<table class='table table-striped table-bordered'>");

    double[] sks = sk();

    sb.append("<tr>");
    for (int i = 0; i <sks.length; ++i) {
      if (sks[i] == 0) continue;
      sb.append("<th>").append(i).append("</th>");
    }
    sb.append("</tr>");

    sb.append("<tr>");
    for (int i = 0; i < sks.length; ++i) {
      if (sks[i] == 0) continue;
      sb.append("<td>").append(sks[i]).append("</td>");
    }
    sb.append("</tr>");
    sb.append("</table></span>");

    //Gap computation
    DocGen.HTML.section(sb, "Gap Statistic per value of k");
    sb.append("<span style='display: inline-block;'>");
    sb.append("<table class='table table-striped table-bordered'>");

    sb.append("<tr>");
    for (int i = 0; i < log_wkbs.length; ++i) {
      if (log_wkbs[i] == 0) continue;
      sb.append("<th>").append(i).append("</th>");
    }
    sb.append("</tr>");

    sb.append("<tr>");
    double prev_val = Double.NEGATIVE_INFINITY;
    int kmin = Integer.MAX_VALUE;
    for (int i = 0; i < log_wkbs.length; ++i) {
      if (log_wkbs[i] == 0) continue;
      double val =  log_wkbs[i] - log_wks[i];
      if (i > 0) {
        if (prev_val >= (val - sks[i])) {
          if (kmin > (i)) {
            kmin = i;
          }
        }
      }
      prev_val = val;
      sb.append("<td>").append(val).append("</td>");
    }
    sb.append("</tr>");
    sb.append("</table></span>");

    if (log_wks[log_wks.length -1] != 0) {
      DocGen.HTML.section(sb, "Best k:");
      if (kmin == Integer.MAX_VALUE) {
        sb.append("k = " + "NA");
      } else {
      sb.append("k = " + kmin);
      }
    } else {
      DocGen.HTML.section(sb, "Best k so far:");
      if (kmin == Integer.MAX_VALUE) {
        sb.append("k = " + "NA");
      } else {
      sb.append("k = " + kmin);
      }
    }
  }
}