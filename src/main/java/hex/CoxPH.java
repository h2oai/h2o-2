package hex;

import Jama.Matrix;
import java.util.Arrays;
import hex.FrameTask.DataInfo;
import water.DKV;
import water.Futures;
import water.Job;
import water.Key;
import water.MemoryManager;
import water.Model;
import water.Request2;
import water.api.CoxPHProgressPage;
import water.api.DocGen;
import water.fvec.Frame;
import water.fvec.Vec;
import water.fvec.Vec.CollectDomain;
import water.util.Utils;

public class CoxPH extends Job {
  @API(help="Data Frame", required=true, filter=Default.class, json=true)
  public Frame source;

  @API(help="start column ignored if unchecked", required=true, filter=Default.class, json=true)
  public boolean use_start_column = true;

  @API(help="Start Time Column", required=false, filter=CoxPHVecSelect.class, json=true)
  public Vec start_column;

  @API(help="Stop Time Column", required=true, filter=CoxPHVecSelect.class, json=true)
  public Vec stop_column;

  @API(help="Event Column", required=true, filter=CoxPHVecSelect.class, json=true)
  public Vec event_column;

  @API(help="X Columns", required=true, filter=CoxPHMultiVecSelect.class, json=true)
  public int[] x_columns;

  @API(help="weights column ignored if unchecked", required=true, filter=Default.class, json=true)
  public boolean use_weights_column = false;

  @API(help="Weights Column", required=false, filter=CoxPHVecSelect.class, json=true)
  public Vec weights_column;

  @API(help="Method for Handling Ties", required=true, filter=Default.class, json=true)
  public CoxPHTies ties = CoxPHTies.efron;

  @API(help="coefficient starting value", required=true, filter=Default.class, json=true)
  public double init = 0;

  @API(help="minimum log-relative error", required=true, filter=Default.class, json=true)
  public double lre_min = 9;

  @API(help="maximum number of iterations", required=true, filter=Default.class, json=true)
  public int iter_max = 20;

  private class CoxPHVecSelect extends VecSelect { CoxPHVecSelect() { super("source"); } }
  private class CoxPHMultiVecSelect extends MultiVecSelect { CoxPHMultiVecSelect() { super("source"); } }

  public static final int MAX_TIME_BINS = 10000;

  public static enum CoxPHTies { efron, breslow }

  public static double[][] malloc2DArray(final int d1, final int d2) {
    final double[][] array = new double[d1][];
    for (int j = 0; j < d1; ++j)
      array[j] = MemoryManager.malloc8d(d2);
    return array;
  }

  public static double[][][] malloc3DArray(final int d1, final int d2, final int d3) {
    final double[][][] array = new double[d1][d2][];
    for (int j = 0; j < d1; ++j)
      for (int k = 0; k < d2; ++k)
        array[j][k] = MemoryManager.malloc8d(d3);
    return array;
  }

  public static class CoxPHModel extends Model implements Job.Progress {
    static final int API_WEAVER = 1; // This file has auto-generated doc & JSON fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from auto-generated code.

    @API(help = "model parameters", json = true)
    final private CoxPH parameters;
    @API(help="Input data info")
    DataInfo data_info;
    @API(help = "names of coefficients")
    String[] coef_names;
    @API(help = "coefficients")
    double[] coef;
    @API(help = "exp(coefficients)")
    double[] exp_coef;
    @API(help = "exp(-coefficients)")
    double[] exp_neg_coef;
    @API(help = "se(coefficients)")
    double[] se_coef;
    @API(help = "z-score")
    double[] z_coef;
    @API(help = "var(coefficients)")
    double[][] var_coef;
    @API(help = "null log-likelihood")
    double null_loglik;
    @API(help = "log-likelihood")
    double loglik;
    @API(help = "log-likelihood test stat")
    double loglik_test;
    @API(help = "Wald test stat")
    double wald_test;
    @API(help = "Score test stat")
    double score_test;
    @API(help = "R-square")
    double rsq;
    @API(help = "Maximum R-square")
    double maxrsq;
    @API(help = "gradient", json = false)
    double[] gradient;
    @API(help = "Hessian", json = false)
    double[][] hessian;
    @API(help = "log relative error")
    double lre;
    @API(help = "number of iterations")
    int iter;
    @API(help = "x weighted mean vector for categorical variables")
    double[] x_mean_cat;
    @API(help = "x weighted mean vector for numeric variables")
    double[] x_mean_num;
    @API(help = "n")
    long n;
    @API(help = "number of rows with missing values")
    long n_missing;
    @API(help = "total events")
    long total_event;
    @API(help = "minimum time")
    long min_time;
    @API(help = "maximum time")
    long max_time;
    @API(help = "time")
    long[] time;
    @API(help = "number at risk")
    double[] n_risk;
    @API(help = "number of events")
    double[] n_event;
    @API(help = "number of censored obs")
    double[] n_censor;
    @API(help = "baseline cumulative hazard")
    double[] cumhaz_0;
    @API(help = "component of var(cumhaz)", json = false)
    double[] var_cumhaz_1;
    @API(help = "component of var(cumhaz)", json = false)
    double[][] var_cumhaz_2;

    public CoxPHModel(CoxPH job, Key selfKey, Key dataKey, Frame fr, float[] priorClassDist) {
      super(selfKey, dataKey, fr, priorClassDist);
      parameters = (CoxPH) job.clone();
    }

    @Override
    public final CoxPH get_params() { return parameters; }
    @Override
    public final Request2 job() { return get_params(); }
    @Override
    public float progress() { return (float) iter / (float) get_params().iter_max; }

    // Following three overrides created for use in super.scoreImpl
    @Override
    public String[] classNames() {
      final String[] names = new String[nclasses()];
      for (int i = 0; i < time.length; ++i) {
        final long t = time[i];
        names[i]               = "cumhaz_"    + t;
        names[i + time.length] = "se_cumhaz_" + t;
      }
      return names;
    }
    @Override
    public boolean isClassifier() { return false; }
    @Override
    public int nclasses() { return 2 * time.length; }

    @Override
    protected float[] score0(double[] data, float[] preds) {
      final int n_time   = time.length;
      final int n_coef   = coef.length;
      final int n_cats   = data_info._cats;
      final int n_num    = data_info._nums;
      final int n_data   = n_cats + n_num;
      final int numStart = data_info.numStart();
      boolean catsAllNA  = true;
      boolean catsHasNA  = false;
      boolean numsHasNA  = false;
      for (int j = 0; j < n_cats; ++j) {
        catsAllNA &= Double.isNaN(data[j]);
        catsHasNA |= Double.isNaN(data[j]);
      }
      for (int j = n_cats; j < n_data; ++j)
        numsHasNA |= Double.isNaN(data[j]);
      if (numsHasNA || (catsHasNA && !catsAllNA) || (n_num == 0 && catsAllNA)) {
        for (int i = 1; i <= 2 * n_time; ++i)
          preds[i] = Float.NaN;
      } else {
        double[] full_data = MemoryManager.malloc8d(n_coef);
        for (int j = 0; j < n_cats; ++j)
          if (Double.isNaN(data[j])) {
            for (int k = data_info._catOffsets[j]; k < data_info._catOffsets[j+1]; ++k)
              full_data[k] = this.x_mean_cat[k];
          } else if (data[j] != 0)
            full_data[data_info._catOffsets[j] + (int) (data[j] - 1)] = 1;
        for (int j = 0; j < n_num; ++j)
          full_data[numStart + j] = data[n_cats + j] - data_info._normSub[j];
        double logRisk = 0;
        for (int j = 0; j < n_coef; ++j)
          logRisk += full_data[j] * coef[j];
        final double risk = Math.exp(logRisk);
        for (int t = 0; t < n_time; ++t)
          preds[t + 1] = (float) (risk * cumhaz_0[t]);
        for (int t = 0; t < n_time; ++t) {
          double var_cumhaz_2_t = 0;
          for (int j = 0; j < n_coef; ++j) {
            double sum = 0;
            for (int k = 0; k < n_coef; ++k)
              sum += var_coef[j][k] * (full_data[k] * cumhaz_0[t] - var_cumhaz_2[k][t]);
            var_cumhaz_2_t += (full_data[j] * cumhaz_0[t] - var_cumhaz_2[j][t]) * sum;
          }
          preds[t + 1 + n_time] = (float) (risk * Math.sqrt(var_cumhaz_1[t] + var_cumhaz_2_t));
        }
      }
      preds[0] = Float.NaN;
      return preds;
    }

    protected void initStats(Frame source, DataInfo dinfo) {
      n = source.numRows();
      data_info = dinfo;
      final int n_coef = data_info.fullN();
      coef_names   = data_info.coefNames();
      coef         = MemoryManager.malloc8d(n_coef);
      exp_coef     = MemoryManager.malloc8d(n_coef);
      exp_neg_coef = MemoryManager.malloc8d(n_coef);
      se_coef      = MemoryManager.malloc8d(n_coef);
      z_coef       = MemoryManager.malloc8d(n_coef);
      gradient     = MemoryManager.malloc8d(n_coef);
      hessian      = malloc2DArray(n_coef, n_coef);
      var_coef     = malloc2DArray(n_coef, n_coef);

      final Vec start_column = source.vec(source.numCols() - 3);
      final Vec stop_column  = source.vec(source.numCols() - 2);
      min_time = parameters.use_start_column ? (long) start_column.min() + 1 :
                                               (long) stop_column.min();
      max_time = (long) stop_column.max();

      final int n_time = new CollectDomain(stop_column).doAll(stop_column).domain().length;
      time         = MemoryManager.malloc8(n_time);
      n_risk       = MemoryManager.malloc8d(n_time);
      n_event      = MemoryManager.malloc8d(n_time);
      n_censor     = MemoryManager.malloc8d(n_time);
      cumhaz_0     = MemoryManager.malloc8d(n_time);
      var_cumhaz_1 = MemoryManager.malloc8d(n_time);
      var_cumhaz_2 = malloc2DArray(n_coef, n_time);
    }

    protected void calcCounts(CoxPHTask coxMR) {
      n_missing = n - coxMR.n;
      n         = coxMR.n;
      x_mean_cat = coxMR.sumWeightedCatX.clone();
      for (int j = 0; j < x_mean_cat.length; j++)
        x_mean_cat[j] /= coxMR.sumWeights;
      x_mean_num = coxMR._dinfo._normSub.clone();
      for (int j = 0; j < x_mean_num.length; j++)
        x_mean_num[j] += coxMR.sumWeightedNumX[j] / coxMR.sumWeights;
      int nz = 0;
      for (int t = 0; t < coxMR.countEvents.length; ++t) {
        total_event += coxMR.countEvents[t];
        if (coxMR.sizeEvents[t] > 0 || coxMR.sizeCensored[t] > 0) {
          time[nz]     = min_time + t;
          n_risk[nz]   = coxMR.sizeRiskSet[t];
          n_event[nz]  = coxMR.sizeEvents[t];
          n_censor[nz] = coxMR.sizeCensored[t];
          nz++;
        }
      }
      if (!parameters.use_start_column)
        for (int t = n_risk.length - 2; t >= 0; --t)
          n_risk[t] += n_risk[t + 1];
    }

    protected double calcLoglik(CoxPHTask coxMR) {
      final int n_coef = coef.length;
      double newLoglik = 0;
      for (int j = 0; j < n_coef; ++j) {
        gradient[j] = 0;
        for (int k = 0; k < n_coef; ++k)
          hessian[j][k] = 0;
      }
      switch (parameters.ties) {
        case efron:
          for (int t = coxMR.sizeEvents.length - 1; t >= 0; --t) {
            if (coxMR.sizeEvents[t] > 0) {
              final double avgSize = coxMR.sizeEvents[t] / coxMR.countEvents[t];
              newLoglik += coxMR.sumLogRiskEvents[t];
              for (int j = 0; j < n_coef; ++j)
                gradient[j] += coxMR.sumXEvents[j][t];
              for (long e = 0; e < coxMR.countEvents[t]; ++e) {
                final double frac = ((double) e) / ((double) coxMR.countEvents[t]);
                final double term = coxMR.rcumsumRisk[t] - frac * coxMR.sumRiskEvents[t];
                newLoglik -= avgSize * Math.log(term);
                for (int j = 0; j < n_coef; ++j) {
                  final double djTerm    = coxMR.rcumsumXRisk[j][t] - frac * coxMR.sumXRiskEvents[j][t];
                  final double djLogTerm = djTerm / term;
                  gradient[j] -= avgSize * djLogTerm;
                  for (int k = 0; k < n_coef; ++k) {
                    final double dkTerm  = coxMR.rcumsumXRisk[k][t]     - frac * coxMR.sumXRiskEvents[k][t];
                    final double djkTerm = coxMR.rcumsumXXRisk[j][k][t] - frac * coxMR.sumXXRiskEvents[j][k][t];
                    hessian[j][k] -= avgSize * (djkTerm / term - (djLogTerm * (dkTerm / term)));
                  }
                }
              }
            }
          }
          break;
        case breslow:
          for (int t = coxMR.sizeEvents.length - 1; t >= 0; --t) {
            if (coxMR.sizeEvents[t] > 0) {
              newLoglik += coxMR.sumLogRiskEvents[t];
              newLoglik -= coxMR.sizeEvents[t] * Math.log(coxMR.rcumsumRisk[t]);
              for (int j = 0; j < n_coef; ++j) {
                final double dlogTerm = coxMR.rcumsumXRisk[j][t] / coxMR.rcumsumRisk[t];
                gradient[j] += coxMR.sumXEvents[j][t];
                gradient[j] -= coxMR.sizeEvents[t] * dlogTerm;
                for (int k = 0; k < n_coef; ++k)
                  hessian[j][k] -= coxMR.sizeEvents[t] *
                    (((coxMR.rcumsumXXRisk[j][k][t] / coxMR.rcumsumRisk[t]) -
                      (dlogTerm * (coxMR.rcumsumXRisk[k][t] / coxMR.rcumsumRisk[t]))));
              }
            }
          }
          break;
        default:
          throw new IllegalArgumentException("ties method must be either efron or breslow");
      }
      return newLoglik;
    }

    protected void calcModelStats(double[] newCoef, double newLoglik) {
      final int n_coef = coef.length;
      final Matrix inv_hessian = new Matrix(hessian).inverse();
      for (int j = 0; j < n_coef; ++j) {
        for (int k = 0; k <= j; ++k) {
          final double elem = -inv_hessian.get(j, k);
          var_coef[j][k] = elem;
          var_coef[k][j] = elem;
        }
      }
      for (int j = 0; j < n_coef; ++j) {
        coef[j]         = newCoef[j];
        exp_coef[j]     = Math.exp(coef[j]);
        exp_neg_coef[j] = Math.exp(- coef[j]);
        se_coef[j]      = Math.sqrt(var_coef[j][j]);
        z_coef[j]       = coef[j] / se_coef[j];
      }
      if (iter == 0) {
        null_loglik = newLoglik;
        maxrsq      = 1 - Math.exp(2 * null_loglik / n);
        score_test  = 0;
        for (int j = 0; j < n_coef; ++j) {
          double sum = 0;
          for (int k = 0; k < n_coef; ++k)
            sum +=  var_coef[j][k] * gradient[k];
          score_test += gradient[j] * sum;
        }
      }
      loglik      = newLoglik;
      loglik_test = - 2 * (null_loglik - loglik);
      rsq         = 1 - Math.exp(- loglik_test / n);
      wald_test   = 0;
      for (int j = 0; j < n_coef; ++j) {
        double sum = 0;
        for (int k = 0; k < n_coef; ++k)
          sum -= hessian[j][k] * (coef[k] - parameters.init);
        wald_test += (coef[j] - parameters.init) * sum;
      }
    }

    protected void calcCumhaz_0(CoxPHTask coxMR) {
      final int n_coef = coef.length;
      int nz = 0;
      switch (parameters.ties) {
        case efron:
          for (int t = 0; t < coxMR.sizeEvents.length; ++t) {
            if (coxMR.sizeEvents[t] > 0 || coxMR.sizeCensored[t] > 0) {
              final double avgSize = coxMR.sizeEvents[t] / coxMR.countEvents[t];
              cumhaz_0[nz]     = 0;
              var_cumhaz_1[nz] = 0;
              for (int j = 0; j < n_coef; ++j)
                var_cumhaz_2[j][nz] = 0;
              for (long e = 0; e < coxMR.countEvents[t]; ++e) {
                final double frac   = ((double) e) / ((double) coxMR.countEvents[t]);
                final double haz    = 1 / (coxMR.rcumsumRisk[t] - frac * coxMR.sumRiskEvents[t]);
                final double haz_sq = haz * haz;
                cumhaz_0[nz]     += avgSize * haz;
                var_cumhaz_1[nz] += avgSize * haz_sq;
                for (int j = 0; j < n_coef; ++j)
                  var_cumhaz_2[j][nz] +=
                    avgSize * ((coxMR.rcumsumXRisk[j][t] - frac * coxMR.sumXRiskEvents[j][t]) * haz_sq);
              }
              nz++;
            }
          }
          break;
        case breslow:
          for (int t = 0; t < coxMR.sizeEvents.length; ++t) {
            if (coxMR.sizeEvents[t] > 0 || coxMR.sizeCensored[t] > 0) {
              cumhaz_0[nz]     = coxMR.sizeEvents[t] / coxMR.rcumsumRisk[t];
              var_cumhaz_1[nz] = coxMR.sizeEvents[t] / (coxMR.rcumsumRisk[t] * coxMR.rcumsumRisk[t]);
              for (int j = 0; j < n_coef; ++j)
                var_cumhaz_2[j][nz] = (coxMR.rcumsumXRisk[j][t] / coxMR.rcumsumRisk[t]) * cumhaz_0[nz];
              nz++;
            }
          }
          break;
        default:
          throw new IllegalArgumentException("ties method must be either efron or breslow");
      }

      for (int t = 1; t < cumhaz_0.length; ++t) {
        cumhaz_0[t]     = cumhaz_0[t - 1]     + cumhaz_0[t];
        var_cumhaz_1[t] = var_cumhaz_1[t - 1] + var_cumhaz_1[t];
        for (int j = 0; j < n_coef; ++j)
          var_cumhaz_2[j][t] = var_cumhaz_2[j][t - 1] + var_cumhaz_2[j][t];
      }
    }

    public Frame makeSurvfit(Key key, double x_new) { // FIXME
      int j = 0;
      if (Double.isNaN(x_new))
        x_new = data_info._normSub[j];
      final int n_time = time.length;
      final Vec[] vecs = Vec.makeNewCons((long) n_time, 4, 0, null);
      final Vec timevec   = vecs[0];
      final Vec cumhaz    = vecs[1];
      final Vec se_cumhaz = vecs[2];
      final Vec surv      = vecs[3];
      final double x_centered = x_new - data_info._normSub[j];
      final double risk = Math.exp(coef[j] * x_centered);
      for (int t = 0; t < n_time; ++t)
        timevec.set(t, time[t]);
      for (int t = 0; t < n_time; ++t) {
        final double cumhaz_1 = risk * cumhaz_0[t];
        cumhaz.set(t, cumhaz_1);
        surv.set(t, Math.exp(-cumhaz_1));
      }
      for (int t = 0; t < n_time; ++t) {
        final double gamma = x_centered * cumhaz_0[t] - var_cumhaz_2[j][t];
        se_cumhaz.set(t, risk * Math.sqrt(var_cumhaz_1[t] + (gamma * var_coef[j][j] * gamma)));
      }
      final Frame fr = new Frame(key, new String[] {"time", "cumhaz", "se_cumhaz", "surv"}, vecs);
      final Futures fs = new Futures();
      DKV.put(key, fr, fs);
      fs.blockForPending();
      return fr;
    }

    public void generateHTML(String title, StringBuilder sb) {
      DocGen.HTML.title(sb, title);

      sb.append("<h4>Data</h4>");
      sb.append("<table class='table table-striped table-bordered table-condensed'><col width=\"25%\"><col width=\"75%\">");
      sb.append("<tr><th>Number of Complete Cases</th><td>");          sb.append(n);          sb.append("</td></tr>");
      sb.append("<tr><th>Number of Non Complete Cases</th><td>");      sb.append(n_missing);  sb.append("</td></tr>");
      sb.append("<tr><th>Number of Events in Complete Cases</th><td>");sb.append(total_event);sb.append("</td></tr>");
      sb.append("</table>");

      sb.append("<h4>Coefficients</h4>");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr><th></th><th>coef</th><th>exp(coef)</th><th>se(coef)</th><th>z</th></tr>");
      for (int j = 0; j < coef.length; ++j) {
        sb.append("<tr><th>");
        sb.append(coef_names[j]);sb.append("</th><td>");sb.append(coef[j]);   sb.append("</td><td>");
        sb.append(exp_coef[j]);  sb.append("</td><td>");sb.append(se_coef[j]);sb.append("</td><td>");
        sb.append(z_coef[j]);
        sb.append("</td></tr>");
      }
      sb.append("</table>");

      sb.append("<h4>Model Statistics</h4>");
      sb.append("<table class='table table-striped table-bordered table-condensed'><col width=\"15%\"><col width=\"85%\">");
      sb.append("<tr><th>Rsquare</th><td>");sb.append(String.format("%.3f", rsq));
      sb.append(" (max possible = ");       sb.append(String.format("%.3f", maxrsq));sb.append(")</td></tr>");
      sb.append("<tr><th>Likelihood ratio test</th><td>");sb.append(String.format("%.2f", loglik_test));
      sb.append(" on ");sb.append(coef.length);sb.append(" df</td></tr>");
      sb.append("<tr><th>Wald test            </th><td>");sb.append(String.format("%.2f", wald_test));
      sb.append(" on ");sb.append(coef.length);sb.append(" df</td></tr>");
      sb.append("<tr><th>Score (logrank) test </th><td>");sb.append(String.format("%.2f", score_test));
      sb.append(" on ");sb.append(coef.length);sb.append(" df</td></tr>");
      sb.append("</table>");
    }

    public void toJavaHtml(StringBuilder sb) {
    }
  }

  private CoxPHModel model;

  @Override
  protected void init() {
    super.init();

    if (use_start_column && !start_column.isInt())
      throw new IllegalArgumentException("start time must be null or of type integer");

    if (!stop_column.isInt())
      throw new IllegalArgumentException("stop time must be of type integer");

    if (!event_column.isInt() && !event_column.isEnum())
      throw new IllegalArgumentException("event must be of type integer or factor");

    if (Double.isNaN(lre_min) || lre_min <= 0)
      throw new IllegalArgumentException("lre_min must be a positive number");

    if (iter_max < 1)
      throw new IllegalArgumentException("iter_max must be a positive integer");

    final long min_time = use_start_column ? (long) start_column.min() + 1 : (long) stop_column.min();
    final int n_time = (int) (stop_column.max() - min_time + 1);
    if (n_time < 1)
      throw new IllegalArgumentException("start times must be strictly less than stop times");
    if (n_time > MAX_TIME_BINS)
      throw new IllegalArgumentException("number of distinct stop times is " + n_time +
                                         "; maximum number allowed is " + MAX_TIME_BINS);

    source = getSubframe();
    int n_resp = 2;
    if (use_weights_column)
      n_resp++;
    if (use_start_column)
      n_resp++;
    final DataInfo dinfo = new DataInfo(source, n_resp, false, DataInfo.TransformType.DEMEAN);
    model = new CoxPHModel(this, dest(), source._key, source, null);
    model.initStats(source, dinfo);
  }

  @Override
  protected void execImpl() {
    final DataInfo dinfo   = model.data_info;
    final int n_coef       = dinfo.fullN();
    final double[] step    = MemoryManager.malloc8d(n_coef);
    final double[] oldCoef = MemoryManager.malloc8d(n_coef);
    final double[] newCoef = MemoryManager.malloc8d(n_coef);
    Arrays.fill(step,    Double.NaN);
    Arrays.fill(oldCoef, Double.NaN);
    for (int j = 0; j < n_coef; ++j)
      newCoef[j] = init;
    double oldLoglik = - Double.MAX_VALUE;
    final int n_time = (int) (model.max_time - model.min_time + 1);
    for (int i = 0; i <= iter_max; ++i) {
      model.iter = i;

      final CoxPHTask coxMR = new CoxPHTask(self(), dinfo, newCoef, model.min_time, n_time,
                                            use_start_column, use_weights_column).doAll(dinfo._adaptedFrame);

      if (i == 0)
        model.calcCounts(coxMR);

      final double newLoglik = model.calcLoglik(coxMR);
      if (newLoglik > oldLoglik) {
        model.calcModelStats(newCoef, newLoglik);
        model.calcCumhaz_0(coxMR);

        if (newLoglik == 0)
          model.lre = - Math.log10(Math.abs(oldLoglik - newLoglik));
        else
          model.lre = - Math.log10(Math.abs((oldLoglik - newLoglik) / newLoglik));
        if (model.lre >= lre_min)
          break;

        Arrays.fill(step, 0);
        for (int j = 0; j < n_coef; ++j)
          for (int k = 0; k < n_coef; ++k)
            step[j] -= model.var_coef[j][k] * model.gradient[k];
        for (int j = 0; j < n_coef; ++j)
          if (Double.isNaN(step[j]) || Double.isInfinite(step[j]))
            break;

        oldLoglik = newLoglik;
        System.arraycopy(newCoef, 0, oldCoef, 0, oldCoef.length);
      } else {
        for (int j = 0; j < n_coef; ++j)
          step[j] /= 2;
      }

      for (int j = 0; j < n_coef; ++j)
        newCoef[j] = oldCoef[j] - step[j];
    }

    final Futures fs = new Futures();
    DKV.put(dest(), model, fs);
    fs.blockForPending();
  }

  @Override
  protected Response redirect() {
    return CoxPHProgressPage.redirect(this, self(), dest());
  }

  private Frame getSubframe() {
    final int x_ncol = x_columns.length;
    int ncol = x_ncol + 2;
    if (use_weights_column)
      ncol++;
    if (use_start_column)
      ncol++;
    final String[] names = new String[ncol];
    for (int j = 0; j < x_ncol; ++j)
      names[j] = source.names()[x_columns[j]];
    if (use_weights_column)
      names[x_ncol]   = source.names()[source.find(weights_column)];
    if (use_start_column)
      names[ncol - 3] = source.names()[source.find(start_column)];
    names[ncol - 2]   = source.names()[source.find(stop_column)];
    names[ncol - 1]   = source.names()[source.find(event_column)];
    return source.subframe(names);
  }

  protected static class CoxPHTask extends FrameTask<CoxPHTask> {
    private final double[] _beta;
    private final int      _n_time;
    private final long     _min_time;
    private final boolean  _use_start_column;
    private final boolean  _use_weights_column;

    protected long         n;
    protected long         n_missing;
    protected double       sumWeights;
    protected double[]     sumWeightedCatX;
    protected double[]     sumWeightedNumX;
    protected double[]     sizeRiskSet;
    protected double[]     sizeCensored;
    protected double[]     sizeEvents;
    protected long[]       countEvents;
    protected double[][]   sumXEvents;
    protected double[]     sumRiskEvents;
    protected double[][]   sumXRiskEvents;
    protected double[][][] sumXXRiskEvents;
    protected double[]     sumLogRiskEvents;
    protected double[]     rcumsumRisk;
    protected double[][]   rcumsumXRisk;
    protected double[][][] rcumsumXXRisk;

    CoxPHTask(Key jobKey, DataInfo dinfo, final double[] beta, final long min_time, final int n_time,
              final boolean use_start_column, final boolean use_weights_column) {
      super(jobKey, dinfo);
      _beta               = beta;
      _n_time             = n_time;
      _min_time           = min_time;
      _use_start_column   = use_start_column;
      _use_weights_column = use_weights_column;
    }

    @Override
    protected void chunkInit(){
      final int n_coef = _beta.length;
      sumWeightedCatX  = MemoryManager.malloc8d(n_coef - _dinfo._nums);
      sumWeightedNumX  = MemoryManager.malloc8d(_dinfo._nums);
      sizeRiskSet      = MemoryManager.malloc8d(_n_time);
      sizeCensored     = MemoryManager.malloc8d(_n_time);
      sizeEvents       = MemoryManager.malloc8d(_n_time);
      countEvents      = MemoryManager.malloc8(_n_time);
      sumRiskEvents    = MemoryManager.malloc8d(_n_time);
      sumLogRiskEvents = MemoryManager.malloc8d(_n_time);
      rcumsumRisk      = MemoryManager.malloc8d(_n_time);
      sumXEvents       = malloc2DArray(n_coef, _n_time);
      sumXRiskEvents   = malloc2DArray(n_coef, _n_time);
      rcumsumXRisk     = malloc2DArray(n_coef, _n_time);
      sumXXRiskEvents  = malloc3DArray(n_coef, n_coef, _n_time);
      rcumsumXXRisk    = malloc3DArray(n_coef, n_coef, _n_time);
    }

    @Override
    protected void processRow(long gid, double [] nums, int ncats, int [] cats, double [] response) {
      n++;
      final double weight = _use_weights_column ? response[0] : 1.0;
      if (weight <= 0)
        throw new IllegalArgumentException("weights must be positive values");
      final long event = (long) response[response.length - 1];
      final int t1 = _use_start_column ? (int) (((long) response[response.length - 3] + 1) - _min_time) : -1;
      final int t2 = (int) (((long) response[response.length - 2]) - _min_time);
      if (t1 > t2)
        throw new IllegalArgumentException("start times must be strictly less than stop times");
      final int numStart = _dinfo.numStart();
      sumWeights += weight;
      for (int j = 0; j < ncats; ++j)
        sumWeightedCatX[cats[j]] += weight;
      for (int j = 0; j < nums.length; ++j)
        sumWeightedNumX[j] += weight * nums[j];
      double logRisk = 0;
      for (int j = 0; j < ncats; ++j)
        logRisk += _beta[cats[j]];
      for (int j = 0; j < nums.length; ++j)
        logRisk += nums[j] * _beta[numStart + j];
      final double risk = weight * Math.exp(logRisk);
      logRisk *= weight;
      if (event > 0) {
        countEvents[t2]++;
        sizeEvents[t2]       += weight;
        sumLogRiskEvents[t2] += logRisk;
        sumRiskEvents[t2]    += risk;
      } else
        sizeCensored[t2] += weight;
      if (_use_start_column) {
        for (int t = t1; t <= t2; ++t)
          sizeRiskSet[t] += weight;
        for (int t = t1; t <= t2; ++t)
          rcumsumRisk[t] += risk;
      } else {
        sizeRiskSet[t2]  += weight;
        rcumsumRisk[t2]  += risk;
      }

      final int ntotal = ncats + nums.length;
      final int numStartIter = numStart - ncats;
      for (int jit = 0; jit < ntotal; ++jit) {
        final boolean jIsCat = jit < ncats;
        final int j          = jIsCat ? cats[jit] : numStartIter + jit;
        final double x1      = jIsCat ? 1.0 : nums[jit - ncats];
        final double xRisk   = x1 * risk;
        if (event > 0) {
          sumXEvents[j][t2]     += weight * x1;
          sumXRiskEvents[j][t2] += xRisk;
        }
        if (_use_start_column) {
          for (int t = t1; t <= t2; ++t)
            rcumsumXRisk[j][t]  += xRisk;
        } else {
          rcumsumXRisk[j][t2]   += xRisk;
        }
        for (int kit = 0; kit < ntotal; ++kit) {
          final boolean kIsCat = kit < ncats;
          final int k          = kIsCat ? cats[kit] : numStartIter + kit;
          final double x2      = kIsCat ? 1.0 : nums[kit - ncats];
          final double xxRisk  = x2 * xRisk;
          if (event > 0)
            sumXXRiskEvents[j][k][t2] += xxRisk;
          if (_use_start_column) {
            for (int t = t1; t <= t2; ++t)
              rcumsumXXRisk[j][k][t]  += xxRisk;
          } else {
            rcumsumXXRisk[j][k][t2]   += xxRisk;
          }
        }
      }
    }

    @Override
    public void reduce(CoxPHTask that) {
      n += that.n;
      sumWeights += that.sumWeights;
      Utils.add(sumWeightedCatX,  that.sumWeightedCatX);
      Utils.add(sumWeightedNumX,  that.sumWeightedNumX);
      Utils.add(sizeRiskSet,      that.sizeRiskSet);
      Utils.add(sizeCensored,     that.sizeCensored);
      Utils.add(sizeEvents,       that.sizeEvents);
      Utils.add(countEvents,      that.countEvents);
      Utils.add(sumXEvents,       that.sumXEvents);
      Utils.add(sumRiskEvents,    that.sumRiskEvents);
      Utils.add(sumXRiskEvents,   that.sumXRiskEvents);
      Utils.add(sumXXRiskEvents,  that.sumXXRiskEvents);
      Utils.add(sumLogRiskEvents, that.sumLogRiskEvents);
      Utils.add(rcumsumRisk,      that.rcumsumRisk);
      Utils.add(rcumsumXRisk,     that.rcumsumXRisk);
      Utils.add(rcumsumXXRisk,    that.rcumsumXXRisk);
    }

    @Override
    protected void postGlobal() {
      if (!_use_start_column) {
        for (int t = rcumsumRisk.length - 2; t >= 0; --t)
          rcumsumRisk[t] += rcumsumRisk[t + 1];

        for (int j = 0; j < rcumsumXRisk.length; ++j)
          for (int t = rcumsumXRisk[j].length - 2; t >= 0; --t)
            rcumsumXRisk[j][t] += rcumsumXRisk[j][t + 1];

        for (int j = 0; j < rcumsumXXRisk.length; ++j)
          for (int k = 0; k < rcumsumXXRisk[j].length; ++k)
            for (int t = rcumsumXXRisk[j][k].length - 2; t >= 0; --t)
              rcumsumXXRisk[j][k][t] += rcumsumXXRisk[j][k][t + 1];
      }
    }
  }
}
