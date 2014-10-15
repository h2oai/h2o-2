package hex;

import Jama.Matrix;
import water.DKV;
import water.Futures;
import water.Job;
import water.Key;
import water.MemoryManager;
import water.Model;
import water.MRTask2;
import water.Request2;
import water.api.CoxPHProgressPage;
import water.api.DocGen;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.fvec.Vec.CollectDomain;
import water.util.Utils;

public class CoxPH extends Job {
  @API(help="Data Frame", required=true, filter=Default.class, json=true)
  public Frame source;

  @API(help="start column ignore if unchecked", required=true, filter=Default.class, json=true)
  public boolean use_start_column = true;

  @API(help="Start Time Column", required=false, filter=CoxPHVecSelect.class, json=true)
  public Vec start_column;

  @API(help="Stop Time Column", required=true, filter=CoxPHVecSelect.class, json=true)
  public Vec stop_column;

  @API(help="Event Column", required=true, filter=CoxPHVecSelect.class, json=true)
  public Vec event_column;

  @API(help="X Columns", required=true, filter=CoxPHMultiVecSelect.class, json=true)
  public int[] x_columns;

  @API(help="Method for Handling Ties", required=true, filter=Default.class, json=true)
  public CoxPHTies ties = CoxPHTies.efron;

  @API(help="coefficient starting value", required=true, filter=Default.class, json=true)
  public double init = 0;

  @API(help="minimum log-relative error", required=true, filter=Default.class, json=true)
  public double lre_min = 9;

  @API(help="maximum number of iterations", required=true, filter=Default.class, json=true)
  public int iter_max = 20;

  public static final int MAX_TIME_BINS = 10000;

  public static enum CoxPHTies { efron, breslow }

  private class CoxPHVecSelect extends VecSelect { CoxPHVecSelect() { super("source"); } }
  private class CoxPHMultiVecSelect extends MultiVecSelect { CoxPHMultiVecSelect() { super("source"); } }

  public static class CoxPHModel extends Model implements Job.Progress {
    static final int API_WEAVER = 1; // This file has auto-generated doc & JSON fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from auto-generated code.

    @API(help = "model parameters", json = true)
    final private CoxPH parameters;
    @API(help = "names of coefficients")
    String[] names_coef;
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
    @API(help = "x mean vector")
    double[] x_mean;
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
    long[] n_risk;
    @API(help = "number of events")
    long[] n_event;
    @API(help = "number of censored obs")
    long[] n_censor;
    @API(help = "baseline cumulative hazard")
    double[] cumhaz_0;
    @API(help = "component of var(cumhaz)", json = false)
    double[] var_cumhaz_1;
    @API(help = "component of var(cumhaz)", json = false)
    double[][] var_cumhaz_2;

    public CoxPHModel(CoxPH job, Key selfKey, Key dataKey, String names[], String domains[][],
                      float[] priorClassDist, float[] modelClassDist) {
      super(selfKey, dataKey, names, domains, priorClassDist, modelClassDist);
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
      String[] names = new String[nclasses()];
      for (int i = 0; i < time.length; i++) {
        long t = time[i];
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
      final int n_coef = coef.length;
      final int n_time = time.length;
      boolean hasNA = false;
      for (int j = 0; j < n_coef; j++)
        hasNA |= Double.isNaN(data[j]);
      if (hasNA) {
        for (int i = 1; i <= 2 * n_time; i++)
          preds[i] = Float.NaN;
      } else {
        double logRisk = 0;
        double[] data_centered = MemoryManager.malloc8d(n_coef);
        for (int j = 0; j < n_coef; j++) {
          data_centered[j] = data[j] - x_mean[j];
          logRisk += data_centered[j] * coef[j];
        }
        final double risk = Math.exp(logRisk);
        for (int t = 0; t < n_time; t++) {
          int i = t + 1;
          double cumhaz_1 = risk * cumhaz_0[t];
          double var_cumhaz_2_t = 0;
          for (int j = 0; j < n_coef; j++) {
            double sum = 0;
            for (int k = 0; k < n_coef; k++)
              sum += var_coef[j][k] * (data_centered[k] * cumhaz_0[t] - var_cumhaz_2[k][t]);
            var_cumhaz_2_t += (data_centered[j] * cumhaz_0[t] - var_cumhaz_2[j][t]) * sum;
          }
          double se_cumhaz_1 = risk * Math.sqrt(var_cumhaz_1[t] + var_cumhaz_2_t);
          preds[i]           = (float) cumhaz_1;
          preds[i + n_time]  = (float) se_cumhaz_1;
        }
      }
      preds[0] = Float.NaN;
      return preds;
    }

    protected void initStats(Frame source) {
      // X columns, start (optional), stop, event
      int n_coef;
      if (parameters.use_start_column)
        n_coef = source.numCols() - 3;
      else
        n_coef = source.numCols() - 2;
      names_coef       = new String[n_coef];
      coef             = MemoryManager.malloc8d(n_coef);
      exp_coef         = MemoryManager.malloc8d(n_coef);
      exp_neg_coef     = MemoryManager.malloc8d(n_coef);
      se_coef          = MemoryManager.malloc8d(n_coef);
      z_coef           = MemoryManager.malloc8d(n_coef);
      x_mean           = MemoryManager.malloc8d(n_coef);
      gradient         = MemoryManager.malloc8d(n_coef);
      hessian          = new double[n_coef][];
      var_coef         = new double[n_coef][];
      for (int j = 0; j < n_coef; j++) {
        hessian[j]     = MemoryManager.malloc8d(n_coef);
        var_coef[j]    = MemoryManager.malloc8d(n_coef);
        names_coef[j]  = source.names()[j];
        x_mean[j]      = source.vec(j).mean();
      }

      Vec start_column = source.vec(source.numCols() - 3);
      Vec stop_column  = source.vec(source.numCols() - 2);
      if (parameters.use_start_column) {
        min_time = (long) start_column.min() + 1;
      } else {
        min_time = (long) stop_column.min();
      }
      max_time   = (long) stop_column.max();

      final int n_time = new CollectDomain(stop_column).doAll(stop_column).domain().length;
      time         = MemoryManager.malloc8(n_time);
      n_risk       = MemoryManager.malloc8(n_time);
      n_event      = MemoryManager.malloc8(n_time);
      n_censor     = MemoryManager.malloc8(n_time);
      cumhaz_0     = MemoryManager.malloc8d(n_time);
      var_cumhaz_1 = MemoryManager.malloc8d(n_time);
      var_cumhaz_2 = new double[n_coef][];
      for (int j = 0; j < n_coef; j++)
        var_cumhaz_2[j] = MemoryManager.malloc8d(n_time);
    }

    protected void calcCounts(CoxPHMRTask coxMR) {
      n         = coxMR.n;
      n_missing = coxMR.n_missing;
      int nz = 0;
      for (int t = 0; t < coxMR.countEvents.length; t++) {
        total_event += coxMR.countEvents[t];
        if (coxMR.countEvents[t] > 0 || coxMR.countCensored[t] > 0) {
          time[nz]     = min_time + t;
          n_risk[nz]   = coxMR.countRiskSet[t];
          n_event[nz]  = coxMR.countEvents[t];
          n_censor[nz] = coxMR.countCensored[t];
          nz++;
        }
      }
      if (!parameters.use_start_column)
        for (int t = n_risk.length - 2; t >= 0; t--)
          n_risk[t] += n_risk[t + 1];
    }

    protected double calcLoglik(CoxPHMRTask coxMR) {
      final int n_coef = coef.length;
      double newLoglik = 0;
      for (int j = 0; j < n_coef; j++) {
        gradient[j] = 0;
        for (int k = 0; k < n_coef; k++)
          hessian[j][k] = 0;
      }
      switch (parameters.ties) {
        case efron:
          for (int t = coxMR.countEvents.length - 1; t >= 0; t--) {
            if (coxMR.countEvents[t] > 0) {
              newLoglik += coxMR.sumLogRiskEvents[t];
              for (int j = 0; j < n_coef; j++)
                gradient[j] += coxMR.sumXEvents[j][t];
              for (long e = 0; e < coxMR.countEvents[t]; e++) {
                double frac  = ((double) e) / ((double) coxMR.countEvents[t]);
                double term  = coxMR.rcumsumRisk[t] - frac * coxMR.sumRiskEvents[t];
                newLoglik   -= Math.log(term);
                for (int j = 0; j < n_coef; j++) {
                  double djTerm     = coxMR.rcumsumXRisk[j][t] - frac * coxMR.sumXRiskEvents[j][t];
                  double djLogTerm  = djTerm / term;
                  gradient[j]      -= djLogTerm;
                  for (int k = 0; k < n_coef; k++) {
                    double dkTerm   = coxMR.rcumsumXRisk[k][t]     - frac * coxMR.sumXRiskEvents[k][t];
                    double djkTerm  = coxMR.rcumsumXXRisk[j][k][t] - frac * coxMR.sumXXRiskEvents[j][k][t];
                    hessian[j][k]  -= djkTerm / term - (djLogTerm * (dkTerm / term));
                  }
                }
              }
            }
          }
          break;
        case breslow:
          for (int t = coxMR.countEvents.length - 1; t >= 0; t--) {
            if (coxMR.countEvents[t] > 0) {
              newLoglik += coxMR.sumLogRiskEvents[t];
              newLoglik -= coxMR.countEvents[t] * Math.log(coxMR.rcumsumRisk[t]);
              for (int j = 0; j < n_coef; j++) {
                double dlogTerm  = coxMR.rcumsumXRisk[j][t] / coxMR.rcumsumRisk[t];
                gradient[j]     += coxMR.sumXEvents[j][t];
                gradient[j]     -= coxMR.countEvents[t] * dlogTerm;
                for (int k = 0; k < n_coef; k++)
                  hessian[j][k] -= coxMR.countEvents[t] *
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
      Matrix inv_hessian = new Matrix(hessian).inverse();
      for (int j = 0; j < n_coef; j++) {
        for (int k = 0; k <= j; k++) {
          double elem    = -inv_hessian.get(j, k);
          var_coef[j][k] = elem;
          var_coef[k][j] = elem;
        }
      }
      for (int j = 0; j < n_coef; j++) {
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
        for (int j = 0; j < n_coef; j++) {
          double sum = 0;
          for (int k = 0; k < n_coef; k++)
            sum +=  var_coef[j][k] * gradient[k];
          score_test += gradient[j] * sum;
        }
      }
      loglik      = newLoglik;
      loglik_test = - 2 * (null_loglik - loglik);
      rsq         = 1 - Math.exp(- loglik_test / n);
      wald_test   = 0;
      for (int j = 0; j < n_coef; j++) {
        double sum = 0;
        for (int k = 0; k < n_coef; k++)
          sum -= hessian[j][k] * (coef[k] - parameters.init);
        wald_test += (coef[j] - parameters.init) * sum;
      }
    }

    protected void calcCumhaz_0(CoxPHMRTask coxMR) {
      final int n_coef = coef.length;
      int nz = 0;
      switch (parameters.ties) {
        case efron:
          for (int t = 0; t < coxMR.countEvents.length; t++) {
            if (coxMR.countEvents[t] > 0 || coxMR.countCensored[t] > 0) {
              cumhaz_0[nz]     = 0;
              var_cumhaz_1[nz] = 0;
              for (int j = 0; j < n_coef; j++)
                var_cumhaz_2[j][nz] = 0;
              for (long e = 0; e < coxMR.countEvents[t]; e++) {
                double frac       = ((double) e) / ((double) coxMR.countEvents[t]);
                double haz        = 1 / (coxMR.rcumsumRisk[t] - frac * coxMR.sumRiskEvents[t]);
                double haz_sq     = haz * haz;
                cumhaz_0[nz]     += haz;
                var_cumhaz_1[nz] += haz_sq;
                for (int j = 0; j < n_coef; j++)
                  var_cumhaz_2[j][nz] += (coxMR.rcumsumXRisk[j][t] - frac * coxMR.sumXRiskEvents[j][t]) * haz_sq;
              }
              nz++;
            }
          }
          break;
        case breslow:
          for (int t = 0; t < coxMR.countEvents.length; t++) {
            if (coxMR.countEvents[t] > 0 || coxMR.countCensored[t] > 0) {
              cumhaz_0[nz]     = coxMR.countEvents[t] / coxMR.rcumsumRisk[t];
              var_cumhaz_1[nz] = coxMR.countEvents[t] / (coxMR.rcumsumRisk[t] * coxMR.rcumsumRisk[t]);
              for (int j = 0; j < n_coef; j++)
                var_cumhaz_2[j][nz] = (coxMR.rcumsumXRisk[j][t] / coxMR.rcumsumRisk[t]) * cumhaz_0[nz];
              nz++;
            }
          }
          break;
        default:
          throw new IllegalArgumentException("ties method must be either efron or breslow");
      }

      for (int t = 1; t < cumhaz_0.length; t++) {
        cumhaz_0[t]     = cumhaz_0[t - 1]     + cumhaz_0[t];
        var_cumhaz_1[t] = var_cumhaz_1[t - 1] + var_cumhaz_1[t];
        for (int j = 0; j < n_coef; j++)
          var_cumhaz_2[j][t] = var_cumhaz_2[j][t - 1] + var_cumhaz_2[j][t];
      }
    }

    public Frame makeSurvfit(Key key, double x_new) { // FIXME
      int j = 0;
      if (Double.isNaN(x_new))
        x_new = x_mean[j];
      final int n_time = time.length;
      Vec[] vecs = Vec.makeNewCons((long) n_time, 4, 0, null);
      Vec timevec   = vecs[0];
      Vec cumhaz    = vecs[1];
      Vec se_cumhaz = vecs[2];
      Vec surv      = vecs[3];
      double x_centered = x_new - x_mean[j];
      final double risk = Math.exp(coef[j] * x_centered);
      for (int t = 0; t < n_time; t++) {
        double gamma    = x_centered * cumhaz_0[t] - var_cumhaz_2[j][t];
        double cumhaz_1 = risk * cumhaz_0[t];
        timevec.set(t,   time[t]);
        cumhaz.set(t,    cumhaz_1);
        se_cumhaz.set(t, risk * Math.sqrt(var_cumhaz_1[t] + (gamma * var_coef[j][j] * gamma)));
        surv.set(t,      Math.exp(- cumhaz_1));
      }
      Frame fr = new Frame(key, new String[] {"time", "cumhaz", "se_cumhaz", "surv"}, vecs);
      Futures fs = new Futures();
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
      sb.append("<tr><th></th><td>coef</td><td>exp(coef)</td><td>se(coef)</td><td>z</td></tr>");
      for (int j = 0; j < coef.length; j++) {
        sb.append("<tr><th>");
        sb.append(names_coef[j]);sb.append("</th><td>");sb.append(coef[j]);   sb.append("</td><td>");
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
      sb.append(" on 1 df</td></tr>");
      sb.append("<tr><th>Wald test            </th><td>");sb.append(String.format("%.2f", wald_test));
      sb.append(" on 1 df</td></tr>");
      sb.append("<tr><th>Score (logrank) test </th><td>");sb.append(String.format("%.2f", score_test));
      sb.append(" on 1 df</td></tr>");
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

    long min_time;
    if (use_start_column)
      min_time = (long) start_column.min() + 1;
    else
      min_time = (long) stop_column.min();

    int n_time = (int) (stop_column.max() - min_time + 1);
    if (n_time < 1)
      throw new IllegalArgumentException("start times must be strictly less than stop times");
    if (n_time > MAX_TIME_BINS)
      throw new IllegalArgumentException("number of distinct stop times is " + n_time +
                                         "; maximum number allowed is " + MAX_TIME_BINS);

    source = getSubframe();
    // Use names and domains from fr with source subframe to satisfy model scoring
    final int x_ncol = x_columns.length;
    String[] names = new String[x_ncol + 1];
    for (int j = 0; j < x_ncol; j++)
      names[j] = source.names()[j];
    names[x_ncol] = source.names()[source.numCols() - 1];
    Frame fr = source.subframe(names);
    model = new CoxPHModel(this, dest(), source._key, fr.names(), fr.domains(), null, null);
    model.initStats(source);
  }

  @Override
  protected void execImpl() {
    // X columns, start (optional), stop, event
    int n_coef;
    if (use_start_column)
      n_coef = source.numCols() - 3;
    else
      n_coef = source.numCols() - 2;
    double[] step    = MemoryManager.malloc8d(n_coef);
    double[] oldCoef = MemoryManager.malloc8d(n_coef);
    double[] newCoef = MemoryManager.malloc8d(n_coef);
    for (int j = 0; j < n_coef; j++) {
      step[j]    = Double.NaN;
      oldCoef[j] = Double.NaN;
      newCoef[j] = init;
    }
    double oldLoglik = - Double.MAX_VALUE;
    final int n_time = (int) (model.max_time - model.min_time + 1);
    for (int i = 0; i <= iter_max; i++) {
      model.iter = i;

      // Map, Reduce & Finalize
      CoxPHMRTask coxMR = new CoxPHMRTask(newCoef, model.min_time, n_time, use_start_column,
                                          model.x_mean).doAll(source.vecs());
      coxMR.finish();

      if (i == 0)
        model.calcCounts(coxMR);

      double newLoglik = model.calcLoglik(coxMR);
      if (newLoglik > oldLoglik) {
        model.calcModelStats(newCoef, newLoglik);
        model.calcCumhaz_0(coxMR);

        if (newLoglik == 0)
          model.lre = - Math.log10(Math.abs(oldLoglik - newLoglik));
        else
          model.lre = - Math.log10(Math.abs((oldLoglik - newLoglik) / newLoglik));
        if (model.lre >= lre_min)
          break;

        for (int j = 0; j < n_coef; j++)
          step[j] = 0;
        for (int j = 0; j < n_coef; j++)
          for (int k = 0; k < n_coef; k++)
            step[j] -= model.var_coef[j][k] * model.gradient[k];
        for (int j = 0; j < n_coef; j++)
          if (Double.isNaN(step[j]) || Double.isInfinite(step[j]))
            break;

        oldLoglik = newLoglik;
        for (int j = 0; j < n_coef; j++)
          oldCoef[j] = newCoef[j];
      } else {
        for (int j = 0; j < n_coef; j++)
          step[j] /= 2;
      }

      for (int j = 0; j < n_coef; j++)
        newCoef[j] = oldCoef[j] - step[j];
    }

    Futures fs = new Futures();
    DKV.put(dest(), model, fs);
    fs.blockForPending();
  }

  @Override
  protected Response redirect() {
    return CoxPHProgressPage.redirect(this, self(), dest());
  }

  private Frame getSubframe() {
    final int x_ncol = x_columns.length;
    final int ncol   = use_start_column ? x_ncol + 3 : x_ncol + 2;
    String[] names = new String[ncol];
    for (int j = 0; j < x_ncol; j++)
      names[j] = source.names()[x_columns[j]];
    if (use_start_column)
      names[ncol - 3] = source.names()[source.find(start_column)];
    names[ncol - 2]   = source.names()[source.find(stop_column)];
    names[ncol - 1]   = source.names()[source.find(event_column)];
    return source.subframe(names);
  }

  protected static class CoxPHMRTask extends MRTask2<CoxPHMRTask> {
    private final double[] _beta;
    private final int      _n_time;
    private final long     _min_time;
    private final boolean  _use_start_column;
    private final double[] _x_mean;

    protected long         n;
    protected long         n_missing;
    protected long[]       countRiskSet;
    protected long[]       countCensored;
    protected long[]       countEvents;
    protected double[][]   sumXEvents;
    protected double[]     sumRiskEvents;
    protected double[][]   sumXRiskEvents;
    protected double[][][] sumXXRiskEvents;
    protected double[]     sumLogRiskEvents;
    protected double[]     rcumsumRisk;
    protected double[][]   rcumsumXRisk;
    protected double[][][] rcumsumXXRisk;

    CoxPHMRTask(final double[] beta, final long min_time, final int n_time, final boolean use_start_column,
                final double[] x_mean) {
      _beta             = beta;
      _n_time           = n_time;
      _min_time         = min_time;
      _use_start_column = use_start_column;
      _x_mean           = x_mean;
    }

    private void mapAllocMemory() {
      final int n_coef = _beta.length;
      countRiskSet              = MemoryManager.malloc8(_n_time);
      countCensored             = MemoryManager.malloc8(_n_time);
      countEvents               = MemoryManager.malloc8(_n_time);
      sumRiskEvents             = MemoryManager.malloc8d(_n_time);
      sumLogRiskEvents          = MemoryManager.malloc8d(_n_time);
      rcumsumRisk               = MemoryManager.malloc8d(_n_time);
      sumXEvents                = new double[n_coef][];
      sumXRiskEvents            = new double[n_coef][];
      rcumsumXRisk              = new double[n_coef][];
      sumXXRiskEvents           = new double[n_coef][n_coef][];
      rcumsumXXRisk             = new double[n_coef][n_coef][];
      for (int j = 0; j < n_coef; j++) {
        sumXEvents[j]           = MemoryManager.malloc8d(_n_time);
        sumXRiskEvents[j]       = MemoryManager.malloc8d(_n_time);
        rcumsumXRisk[j]         = MemoryManager.malloc8d(_n_time);
        for (int k = 0; k < n_coef; k++) {
          sumXXRiskEvents[j][k] = MemoryManager.malloc8d(_n_time);
          rcumsumXXRisk[j][k]   = MemoryManager.malloc8d(_n_time);
        }
      }
    }

    private boolean[] mapRowHasNA(Chunk[] cols) {
      final int x_ncol = _use_start_column ? cols.length - 3 : cols.length - 2;
      Chunk start  = null;
      if (_use_start_column)
            start  = cols[cols.length - 3];
      Chunk stop   = cols[cols.length - 2];
      Chunk events = cols[cols.length - 1];
      final int nrow = stop._len;
      boolean[] hasNA = MemoryManager.mallocZ(nrow);
      for (int i = 0; i < nrow; i++)
        hasNA[i] = stop.isNA0(i);
      for (int i = 0; i < nrow; i++)
        hasNA[i] |= events.isNA0(i);
      if (_use_start_column) {
        for (int i = 0; i < nrow; i++) {
          hasNA[i] |= start.isNA0(i);
          if (start.at80(i) >= stop.at80(i))
            throw new IllegalArgumentException("start times must be strictly less than stop times");
        }
      }
      for (int j = 0; j < x_ncol; j++)
        for (int i = 0; i < nrow; i++)
          hasNA[i] |= cols[j].isNA0(i);
      return hasNA;
    }

    private int[] mapCalcTime1(Chunk start) {
      int[] t1 = null;
      if (_use_start_column) {
        final int nrow = start._len;
        t1 = MemoryManager.malloc4(nrow);
        for (int i = 0; i < nrow; i++) {
          if (!start.isNA0(i)) {
            long start_i = start.at80(i);
            t1[i] = (int) ((start_i + 1) - _min_time);
          }
        }
      }
      return t1;
    }

    private int[] mapCalcTime2(Chunk stop) {
      final int nrow = stop._len;
      int[] t2 = MemoryManager.malloc4(nrow);
      for (int i = 0; i < nrow; i++) {
        if (!stop.isNA0(i)) {
          long stop_i = stop.at80(i);
          t2[i] = (int) (stop_i - _min_time);
        }
      }
      return t2;
    }

    private double[] mapCalcLogRisk(Chunk[] cols, boolean[] hasNA) {
      final int nrow = hasNA.length;
      final int x_ncol = _use_start_column ? cols.length - 3 : cols.length - 2;
      double[] logRisk = MemoryManager.malloc8d(nrow);
      for (int j = 0; j < x_ncol; j++) {
        for (int i = 0; i < nrow; i++) {
          if (!hasNA[i]) {
            double x_i = cols[j].at0(i) - _x_mean[j];
            logRisk[i] += x_i * _beta[j];
          }
        }
      }
      return logRisk;
    }

    private double[] mapCalcRisk(double[] logRisk, boolean[] hasNA) {
      final int nrow = logRisk.length;
      double[] risk = MemoryManager.malloc8d(nrow);
      for (int i = 0; i < nrow; i++) {
        if (!hasNA[i])
          risk[i] = Math.exp(logRisk[i]);
      }
      return risk;
    }

    private void mapCalcStats(Chunk[] cols) {
      final int x_ncol = _use_start_column ? cols.length - 3 : cols.length - 2;
      Chunk start  = null;
      if (_use_start_column)
            start  = cols[cols.length - 3];
      Chunk stop   = cols[cols.length - 2];
      Chunk events = cols[cols.length - 1];

      boolean[] hasNA  = mapRowHasNA(cols);
      int[] time1      = mapCalcTime1(start);
      int[] time2      = mapCalcTime2(stop);
      double[] logRisk = mapCalcLogRisk(cols, hasNA);
      double[] risk    = mapCalcRisk(logRisk, hasNA);

      final int nrow = cols[0]._len;
      for (int i = 0; i < nrow; i++) {
        if (hasNA[i])
          n_missing++;
        else {
          n++;
          int t2 = time2[i];
          double risk_i = risk[i];
          if (events.at80(i) > 0) {
            countEvents[t2]++;
            sumLogRiskEvents[t2] += logRisk[i];
            sumRiskEvents[t2]    += risk_i;
          } else
            countCensored[t2]++;
          if (_use_start_column) {
            int t1 = time1[i];
            for (int t = t1; t <= t2; t++) {
              countRiskSet[t]++;
              rcumsumRisk[t]     += risk_i;
            }
          } else {
            countRiskSet[t2]++;
            rcumsumRisk[t2]      += risk_i;
          }
        }
      }

      for (int j = 0; j < x_ncol; j++) {
        for (int i = 0; i < nrow; i++) {
          if (!hasNA[i]) {
            int t1 = -1;
            if (_use_start_column)
                t1 = time1[i];
            int t2 = time2[i];
            long event_i   = events.at80(i);
            double x1_i    = cols[j].at0(i) - _x_mean[j];
            double risk_i  = risk[i];
            double xRisk_i = x1_i * risk_i;
            if (event_i > 0) {
              sumXEvents[j][t2]     += x1_i;
              sumXRiskEvents[j][t2] += xRisk_i;
            }
            if (_use_start_column) {
              for (int t = t1; t <= t2; t++)
                rcumsumXRisk[j][t] += xRisk_i;
            } else {
              rcumsumXRisk[j][t2]  += xRisk_i;
            }
            for (int k = 0; k < x_ncol; k++) {
              double x2_i = cols[k].at0(i) - _x_mean[k];
              double xxRisk_i = x2_i * xRisk_i;
              if (event_i > 0)
                sumXXRiskEvents[j][k][t2] += xxRisk_i;
              if (_use_start_column) {
                for (int t = t1; t <= t2; t++)
                  rcumsumXXRisk[j][k][t]  += xxRisk_i;
              } else {
                rcumsumXXRisk[j][k][t2]   += xxRisk_i;
              }
            }
          }
        }
      }
    }

    @Override
    public void map(Chunk[] cols) {
      mapAllocMemory();
      mapCalcStats(cols);
    }

    @Override
    public void reduce(CoxPHMRTask that) {
      n         += that.n;
      n_missing += that.n_missing;
      Utils.add(countRiskSet,     that.countRiskSet);
      Utils.add(countCensored,    that.countCensored);
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

    protected void finish() {
      if (!_use_start_column) {
        for (int t = rcumsumRisk.length - 2; t >= 0; t--)
          rcumsumRisk[t] += rcumsumRisk[t + 1];

        for (int j = 0; j < rcumsumXRisk.length; j++)
          for (int t = rcumsumXRisk[j].length - 2; t >= 0; t--)
            rcumsumXRisk[j][t] += rcumsumXRisk[j][t + 1];

        for (int j = 0; j < rcumsumXXRisk.length; j++)
          for (int k = 0; k < rcumsumXXRisk[j].length; k++)
            for (int t = rcumsumXXRisk[j][k].length - 2; t >= 0; t--)
              rcumsumXXRisk[j][k][t] += rcumsumXXRisk[j][k][t + 1];
      }
    }
  }
}
