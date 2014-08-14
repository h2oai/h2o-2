package hex.nb;

import hex.FrameTask.DataInfo;
import hex.nb.NaiveBayes.NBTask;
import org.apache.commons.math3.distribution.NormalDistribution;
import water.Key;
import water.Model;
import water.Request2;
import water.api.DocGen;
import water.api.Predict;
import water.api.Request.API;
import water.api.RequestBuilders.ElementBuilder;

/**
 * FIXME comment please
 */
public class NBModel extends Model {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Class counts of the dependent variable")
  final double[] rescnt;

  @API(help = "Class distribution of the dependent variable")
  final double[] pprior;

  @API(help = "For every predictor variable, a table giving, for each attribute level, the conditional probabilities given the target class")
  final double[][][] pcond;

  @API(help = "Number of categorical predictor variables")
  final int ncats;

  @API(help = "Number of numeric predictor variables")
  final int nnums;

  @API(help = "Laplace smoothing parameter")
  final double laplace;

  @API(help = "Min. standard deviation to use for observations with not enough data")
  final double min_std_dev;

  @API(help = "Model parameters", json = true)
  private Request2 job;
  @Override public final NaiveBayes get_params() { return (NaiveBayes)job; }
  @Override public final Request2 job() { return job; }

  public NBModel(Key selfKey, Key dataKey, DataInfo dinfo, NBTask tsk, double[] pprior, double[][][] pcond, double laplace, double min_std_dev) {
    super(selfKey, dataKey, dinfo._adaptedFrame, /* priorClassDistribution */ null);
    this.rescnt = tsk._rescnt;
    this.job= tsk._job;
    this.pprior = pprior;
    this.pcond = pcond;
    this.ncats = dinfo._cats;
    this.nnums = dinfo._nums;
    this.laplace = laplace;
    this.min_std_dev = min_std_dev;
  }

  public double[] pprior() { return pprior; }
  public double[][][] pcond() { return pcond; }

  // Note: For small probabilities, product may end up zero due to underflow error. Can circumvent by taking logs.
  @Override protected float[] score0(double[] data, float[] preds) {
    double denom = 0;
    assert preds.length == (pprior.length + 1);   // Note: First column of preds is predicted response class

    // Compute joint probability of predictors for every response class
    for(int rlevel = 0; rlevel < pprior.length; rlevel++) {
      double num = 1;
      for(int col = 0; col < ncats; col++) {
        if(Double.isNaN(data[col])) continue;   // Skip predictor in joint x_1,...,x_m if NA
        int plevel = (int)data[col];
        num *= pcond[col][rlevel][plevel];    // p(x|y) = \Pi_{j = 1}^m p(x_j|y)
      }

      // For numeric predictors, assume Gaussian distribution with sample mean and variance from model
      for(int col = ncats; col < data.length; col++) {
        if(Double.isNaN(data[col])) continue;

        // Two ways to get non-zero std deviation HEX-1852
//        double stddev = pcond[col][rlevel][1] > 0 ? pcond[col][rlevel][1] : min_std_dev; //only use the placeholder for critically low data
        double stddev = Math.max(pcond[col][rlevel][1], min_std_dev); // more stable for almost constant data
        double mean = pcond[col][rlevel][0];
        double x = data[col];
        num *= Math.exp(-((x-mean)*(x-mean)/(2.*stddev*stddev)))/stddev/Math.sqrt(2.*Math.PI); // faster
//        num *= new NormalDistribution(mean, stddev).density(data[col]); //slower
      }

      num *= pprior[rlevel];    // p(x,y) = p(x|y)*p(y)
      denom += num;             // p(x) = \Sum_{levels of y} p(x,y)
      preds[rlevel+1] = (float)num;
    }

    // Select class with highest conditional probability
    float max = -1;
    for(int i = 1; i < preds.length; i++) {
      preds[i] /= denom;    // p(y|x) = p(x,y)/p(x)

      if(preds[i] > max) {
        max = preds[i];
        preds[0] = i-1;
      }
    }
    return preds;
  }

  @Override public String toString(){
    StringBuilder sb = new StringBuilder("Naive Bayes Model (key=" + _key + " , trained on " + _dataKey + "):\n");
    return sb.toString();
  }

  public void generateHTML(String title, StringBuilder sb) {
    if(title != null && !title.isEmpty()) DocGen.HTML.title(sb, title);
    DocGen.HTML.paragraph(sb, "Model Key: " + _key);
    sb.append("<div class='alert'>Actions: " + Predict.link(_key, "Predict on dataset") + ", "
        + NaiveBayes.link(_dataKey, "Compute new model") + "</div>");

    DocGen.HTML.section(sb, "A-Priori Probabilities");
    sb.append("<span style='display: inline-block;'>");
    sb.append("<table class='table table-striped table-bordered'>");

    // Domain of the response variable
    String[] resdom = _domains[_domains.length-1];
    sb.append("<tr>");
    for(int i = 0; i < resdom.length; i++)
      sb.append("<th>").append(resdom[i]).append("</th>");
    sb.append("</tr>");

    // Display table of a-priori response probabilities
    sb.append("<tr>");
    for(int i = 0; i < pprior.length; i++)
      sb.append("<td>").append(ElementBuilder.format(pprior[i])).append("</td>");
    sb.append("</tr>");
    sb.append("</table></span>");

    DocGen.HTML.section(sb, "Conditional Probabilities");
    // Display table of conditional probabilities for categorical predictors
    for(int col = 0; col < ncats; col++) {
      DocGen.HTML.paragraph(sb, "Column: " + _names[col]);
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");

      // Domain of the predictor variable
      sb.append("<tr>");
      sb.append("<th>").append("Response/Predictor").append("</th>");
      for(int i = 0; i < _domains[col].length; i++)
        sb.append("<th>").append(_domains[col][i]).append("</th>");
      sb.append("</tr>");

      // For each predictor, display table of conditional probabilities
      for(int r = 0; r < pcond[col].length; r++) {
        sb.append("<tr>");
        sb.append("<th>").append(resdom[r]).append("</th>");

        for(int c = 0; c < pcond[col][r].length; c++) {
          double e = pcond[col][r][c];
          sb.append("<td>").append(ElementBuilder.format(e)).append("</td>");
        }
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }

    // Display table of statistics for numeric predictors
    for(int col = ncats; col < ncats + nnums; col++) {
      DocGen.HTML.paragraph(sb, "Column: " + _names[col]);
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");

      // Labels for the predictor variable columns
      sb.append("<tr>");
      sb.append("<th>").append("Response/Predictor").append("</th>");
      sb.append("<th>").append("Mean").append("</th>");
      sb.append("<th>").append("Standard Deviation").append("</th>");
      sb.append("</tr>");

      // For each predictor, display mean and standard deviation within every response level
      for(int r = 0; r < pcond[col].length; r++) {
        sb.append("<tr>");
        sb.append("<th>").append(resdom[r]).append("</th>");

        double pmean = pcond[col][r][0];
        double psdev = pcond[col][r][1];
        sb.append("<td>").append(ElementBuilder.format(pmean)).append("</td>");
        sb.append("<td>").append(ElementBuilder.format(psdev)).append("</td>");
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }
  }
}
