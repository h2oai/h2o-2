package hex;

import water.DKV;
import water.Futures;
import water.H2O;
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
  @API(help="Data Frame",        required=true,  filter=Default.class, json=true)
  public Frame source;

  @API(help="",                  required=true,  filter=Default.class, json=true)
  public boolean use_start_column = true;

  @API(help="Start Time Column", required=false, filter=CoxPHVecSelect.class, json=true)
  public Vec start_column;

  @API(help="Stop Time Column",  required=true,  filter=CoxPHVecSelect.class, json=true)
  public Vec stop_column;

  @API(help="Event Column",      required=true,  filter=CoxPHVecSelect.class, json=true)
  public Vec event_column;

  @API(help="X Column",          required=true,  filter=CoxPHVecSelect.class, json=true)
  public Vec x_column;

  @API(help="Method for Handling Ties", required=true, filter=Default.class, json=true)
  public CoxPHTies ties = CoxPHTies.efron;

  @API(help="",                  required=true, filter=Default.class, json=true)
  public double init = 0;

  @API(help="",                  required=true,  filter=Default.class, json=true)
  public double lre_min = 9;

  @API(help="",                  required=true,  filter=Default.class, json=true)
  public int iter_max = 20;

  public static final int MAX_TIME_BINS = 10000;

  private class CoxPHVecSelect extends VecSelect { CoxPHVecSelect() { super("source"); } }

  public static enum CoxPHTies { efron, breslow }

  public static class CoxPHModel extends Model implements Job.Progress {
    static final int API_WEAVER = 1; // This file has auto-generated doc & JSON fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from auto-generated code.

    @API(help = "model parameters", json = true)
    final private CoxPH parameters;
    @API(help = "names of coefficients")
    String names_coef;     // vector
    @API(help = "coefficients")
    double coef;           // vector
    @API(help = "exp(coefficients)")
    double exp_coef;       // vector
    @API(help = "exp(-coefficients)")
    double exp_neg_coef;   // vector
    @API(help = "se(coefficients)")
    double se_coef;        // vector
    @API(help = "z-score")
    double z_coef;         // vector
    @API(help = "var(coefficients)")
    double var_coef;       // matrix
    @API(help = "null log-likelihood")
    double null_loglik;    // scalar
    @API(help = "log-likelihood")
    double loglik;         // scalar
    @API(help = "log-likelihood test stat")
    double loglik_test;    // scalar
    @API(help = "Wald test stat")
    double wald_test;      // scalar
    @API(help = "Score test stat")
    double score_test;     // scalar
    @API(help = "R-square")
    double rsq;            // scalar
    @API(help = "Maximum R-square")
    double maxrsq;         // scalar
    @API(help = "gradient")
    double gradient;       // vector
    @API(help = "Hessian")
    double hessian;        // matrix
    @API(help = "log relative error")
    double lre;            // scalar
    @API(help = "number of iterations")
    int iter;              // scalar
    @API(help = "mean of x column")
    double x_mean;         // scalar
    @API(help = "n")
    long n;                // scalar
    @API(help = "number of rows with missing values")
    long n_missing;        // scalar
    @API(help = "total events")
    long total_event;      // scalar
    @API(help = "minimum time")
    long min_time;         // scalar
    @API(help = "maximum time")
    long max_time;         // scalar
    @API(help = "time")
    long[] time;           // vector
    @API(help = "number at risk")
    long[] n_risk;         // vector
    @API(help = "number of events")
    long[] n_event;        // vector
    @API(help = "number of censored obs")
    long[] n_censor;       // vector
    @API(help = "baseline cumulative hazard")
    double[] cumhaz_0;     // vector
    @API(help = "component of var(cumhaz)")
    double[] var_cumhaz_1; // vector
    @API(help = "component of var(cumhaz)")
    double[] var_cumhaz_2; // vector

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

    // For use in super.scoreImpl
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
      double x_new = data[0];
      if (Double.isNaN(x_new))
        x_new = x_mean;
      int n_time = time.length;
      double x_centered = x_new - x_mean;
      double risk_new = Math.exp(coef * x_centered);
      preds[0] = Float.NaN;
      for (int t = 0; t < n_time; t++) {
        int i = t + 1;
        double gamma     = x_centered * cumhaz_0[t] - var_cumhaz_2[t];
        double cumhaz_1    = risk_new * cumhaz_0[t];
        double se_cumhaz_1 = risk_new * Math.sqrt(var_cumhaz_1[t] + (gamma * var_coef * gamma));
        preds[i]           = (float) cumhaz_1;
        preds[i + n_time]  = (float) se_cumhaz_1;
      }
      return preds;
    }

    protected void initStats(Frame source, Vec start_column, Vec stop_column, Vec x_column) {
      if (parameters.use_start_column) {
        names_coef = source.names()[3];
        min_time   = (long) start_column.min() + 1;
      } else {
        names_coef = source.names()[2];
        min_time   = (long) stop_column.min();
      }
      max_time     = (long) stop_column.max();
      x_mean       = x_column.mean();
      int n_time   = new CollectDomain(stop_column).doAll(stop_column).domain().length;
      time         = MemoryManager.malloc8(n_time);
      n_risk       = MemoryManager.malloc8(n_time);
      n_event      = MemoryManager.malloc8(n_time);
      n_censor     = MemoryManager.malloc8(n_time);
      cumhaz_0     = MemoryManager.malloc8d(n_time);
      var_cumhaz_1 = MemoryManager.malloc8d(n_time);
      var_cumhaz_2 = MemoryManager.malloc8d(n_time);
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
      double newLoglik = 0;
      gradient = 0;
      hessian  = 0;
      switch (parameters.ties) {
        case efron:
          for (int t = coxMR.countEvents.length - 1; t >= 0; t--) {
            if (coxMR.countEvents[t] > 0) {
              newLoglik += coxMR.sumLogRiskEvents[t];
              gradient  += coxMR.sumXEvents[t];
              for (long e = 0; e < coxMR.countEvents[t]; e++) {
                double frac      = ((double) e) / ((double) coxMR.countEvents[t]);
                double term      = coxMR.rcumsumRisk[t]   - frac * coxMR.sumRiskEvents[t];
                double dterm     = coxMR.rcumsumXRisk[t]  - frac * coxMR.sumXRiskEvents[t];
                double d2term    = coxMR.rcumsumXXRisk[t] - frac * coxMR.sumXXRiskEvents[t];
                double dlogTerm  = dterm / term;
                newLoglik       -= Math.log(term);
                gradient        -= dlogTerm;
                hessian         -= d2term / term - (dlogTerm * (dterm / term));
              }
            }
          }
          break;
        case breslow:
          for (int t = coxMR.countEvents.length - 1; t >= 0; t--) {
            if (coxMR.countEvents[t] > 0) {
              newLoglik       += coxMR.sumLogRiskEvents[t];
              gradient        += coxMR.sumXEvents[t];
              double dlogTerm  = coxMR.rcumsumXRisk[t] / coxMR.rcumsumRisk[t];
              newLoglik       -= coxMR.countEvents[t] * Math.log(coxMR.rcumsumRisk[t]);
              gradient        -= coxMR.countEvents[t] * dlogTerm;
              hessian         -= coxMR.countEvents[t] *
                (((coxMR.rcumsumXXRisk[t] / coxMR.rcumsumRisk[t]) -
                  (dlogTerm * (coxMR.rcumsumXRisk[t] / coxMR.rcumsumRisk[t]))));
            }
          }
          break;
        default:
          throw new IllegalArgumentException("ties method must be either efron or breslow");
      }
      return newLoglik;
    }

    protected void calcModelStats(double newCoef, double newLoglik) {
      if (iter == 0) {
        null_loglik = newLoglik;
        maxrsq      = 1 - Math.exp(2 * null_loglik / n);
        score_test  = -  gradient * gradient / hessian;
      }
      coef          = newCoef;
      exp_coef      = Math.exp(coef);
      exp_neg_coef  = Math.exp(- coef);
      var_coef      = - 1 / hessian;
      se_coef       = Math.sqrt(var_coef);
      z_coef        = coef / se_coef;
      loglik        = newLoglik;
      loglik_test   = - 2 * (null_loglik - loglik);
      double diff_init = coef - parameters.init;
      wald_test     = (diff_init * diff_init) / var_coef;
      rsq           = 1 - Math.exp(- loglik_test / n);
    }

    protected void calcCumhaz_0(CoxPHMRTask coxMR) {
      int nz = 0;
      switch (parameters.ties) {
        case efron:
          for (int t = 0; t < coxMR.countEvents.length; t++) {
            if (coxMR.countEvents[t] > 0 || coxMR.countCensored[t] > 0) {
              cumhaz_0[nz]     = 0;
              var_cumhaz_1[nz] = 0;
              var_cumhaz_2[nz] = 0;
              for (long e = 0; e < coxMR.countEvents[t]; e++) {
                double frac       = ((double) e) / ((double) coxMR.countEvents[t]);
                double haz        = 1 / (coxMR.rcumsumRisk[t] - frac * coxMR.sumRiskEvents[t]);
                cumhaz_0[nz]     += haz;
                var_cumhaz_1[nz] += haz * haz;
                var_cumhaz_2[nz] += (coxMR.rcumsumXRisk[t] - frac * coxMR.sumXRiskEvents[t]) * haz * haz;
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
              var_cumhaz_2[nz] = (coxMR.rcumsumXRisk[t] / coxMR.rcumsumRisk[t]) * cumhaz_0[nz];
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
        var_cumhaz_2[t] = var_cumhaz_2[t - 1] + var_cumhaz_2[t];
      }
    }

    public Frame makeSurvfit(Key key, double x_new) {
      if (Double.isNaN(x_new))
        x_new = x_mean;
      int n_time = time.length;
      Vec[] vecs = Vec.makeNewCons((long) n_time, 4, 0, null);
      Vec timevec   = vecs[0];
      Vec cumhaz    = vecs[1];
      Vec se_cumhaz = vecs[2];
      Vec surv      = vecs[3];
      double x_centered = x_new - x_mean;
      double risk_new = Math.exp(coef * x_centered);
      for (int t = 0; t < n_time; t++) {
        double gamma    = x_centered * cumhaz_0[t] - var_cumhaz_2[t];
        double cumhaz_1 = risk_new * cumhaz_0[t];
        timevec.set(t,   time[t]);
        cumhaz.set(t,    cumhaz_1);
        se_cumhaz.set(t, risk_new * Math.sqrt(var_cumhaz_1[t] + (gamma * var_coef * gamma)));
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
      sb.append("<tr><th>");sb.append(names_coef);sb.append("</th><td>");sb.append(coef);   sb.append("</td><td>");
                            sb.append(exp_coef);  sb.append("</td><td>");sb.append(se_coef);sb.append("</td><td>");
                            sb.append(z_coef);    sb.append("</td></tr>");
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
    Frame fr = source.subframe(new String[] {source.names()[0], source.names()[source.numCols() - 1]});
    model = new CoxPHModel(this, dest(), source._key, fr.names(), fr.domains(), null, null);
    model.initStats(source, start_column, stop_column, x_column);
  }

  @Override
  protected void execImpl() {
    Vec[] cols = source.vecs();

    int n_time       = (int) (model.max_time - model.min_time + 1);
    double step      = Double.NaN;
    double oldCoef   = Double.NaN;
    double oldLoglik = - Double.MAX_VALUE;
    double newCoef   = init;
    for (int i = 0; i <= iter_max; i++) {
      model.iter = i;

      // Map, Reduce & Finalize
      CoxPHMRTask coxMR = new CoxPHMRTask(newCoef, model.min_time, n_time, use_start_column, model.x_mean).doAll(cols);
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

        step = model.gradient / model.hessian;
        if (Double.isNaN(step) || Double.isInfinite(step))
          break;

        oldCoef   = newCoef;
        oldLoglik = newLoglik;
      } else
        step /= 2;

        newCoef = oldCoef - step;
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
    String[] names;
    if (use_start_column) {
      names = new String[4];
      names[0] = source.names()[source.find(x_column)];
      names[1] = source.names()[source.find(start_column)];
      names[2] = source.names()[source.find(stop_column)];
      names[3] = source.names()[source.find(event_column)];
    } else {
      names = new String[3];
      names[0] = source.names()[source.find(x_column)];
      names[1] = source.names()[source.find(stop_column)];
      names[2] = source.names()[source.find(event_column)];
    }
    return source.subframe(names);
  }

  protected static class CoxPHMRTask extends MRTask2<CoxPHMRTask> {
    private final double  _beta;
    private final int     _n_time;
    private final long    _min_time;
    private final boolean _use_start_column;
    private final double  _x_mean;

    protected long     n;
    protected long     n_missing;
    protected long[]   countRiskSet;
    protected long[]   countCensored;
    protected long[]   countEvents;
    protected double[] sumXEvents;
    protected double[] sumRiskEvents;
    protected double[] sumXRiskEvents;
    protected double[] sumXXRiskEvents;
    protected double[] sumLogRiskEvents;
    protected double[] rcumsumRisk;
    protected double[] rcumsumXRisk;
    protected double[] rcumsumXXRisk;

    CoxPHMRTask(final double beta, final long min_time, final int n_time, final boolean use_start_column,
                final double x_mean) {
      _beta             = beta;
      _n_time           = n_time;
      _min_time         = min_time;
      _use_start_column = use_start_column;
      _x_mean           = x_mean;
    }

    @Override
    public void map(Chunk[] cols) {
      Chunk xs     = cols[0];
      Chunk start  = null;
      if (_use_start_column)
            start  = cols[cols.length - 3];
      Chunk stop   = cols[cols.length - 2];
      Chunk events = cols[cols.length - 1];

      int i, t, t1 = -1, t2;
      long start_i, stop_i, event_i;
      double x_i, logRisk_i, risk_i, xRisk_i, xxRisk_i;
      countRiskSet     = MemoryManager.malloc8(_n_time);
      countCensored    = MemoryManager.malloc8(_n_time);
      countEvents      = MemoryManager.malloc8(_n_time);
      sumXEvents       = MemoryManager.malloc8d(_n_time);
      sumRiskEvents    = MemoryManager.malloc8d(_n_time);
      sumXRiskEvents   = MemoryManager.malloc8d(_n_time);
      sumXXRiskEvents  = MemoryManager.malloc8d(_n_time);
      sumLogRiskEvents = MemoryManager.malloc8d(_n_time);
      rcumsumRisk      = MemoryManager.malloc8d(_n_time);
      rcumsumXRisk     = MemoryManager.malloc8d(_n_time);
      rcumsumXXRisk    = MemoryManager.malloc8d(_n_time);
      for (i = 0; i < stop._len; i++) {
        boolean missing_obs = stop.isNA0(i) || events.isNA0(i) || xs.isNA0(i);
        if (_use_start_column)
          missing_obs = missing_obs || start.isNA0(i);

        if (missing_obs)
          n_missing++;
        else {
          event_i = events.at80(i);
          stop_i  = stop.at80(i);
          t2 = (int) (stop_i - _min_time);
          if (_use_start_column) {
            start_i = start.at80(i);
            if (start_i >= stop_i)
              throw new IllegalArgumentException("start times must be strictly less than stop times");
            t1 = (int) ((start_i + 1) - _min_time);
          }
          x_i = xs.at0(i) - _x_mean;
          logRisk_i = x_i * _beta;
          risk_i    = Math.exp(logRisk_i);
          xRisk_i   = x_i * risk_i;
          xxRisk_i  = x_i * xRisk_i;
          n++;
          if (event_i > 0) {
            countEvents[t2]++;
            sumXEvents[t2]       += x_i;
            sumRiskEvents[t2]    += risk_i;
            sumXRiskEvents[t2]   += xRisk_i;
            sumXXRiskEvents[t2]  += xxRisk_i;
            sumLogRiskEvents[t2] += logRisk_i;
          } else
            countCensored[t2]++;
          if (_use_start_column) {
            for (t = t1; t <= t2; t++) {
              countRiskSet[t]++;
              rcumsumRisk[t]   += risk_i;
              rcumsumXRisk[t]  += xRisk_i;
              rcumsumXXRisk[t] += xxRisk_i;
            }
          } else {
            countRiskSet[t2]++;
            rcumsumRisk[t2]   += risk_i;
            rcumsumXRisk[t2]  += xRisk_i;
            rcumsumXXRisk[t2] += xxRisk_i;
          }
        }
      }
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
        for (int t = rcumsumRisk.length - 2; t >= 0; t--) {
          rcumsumRisk[t]   += rcumsumRisk[t + 1];
          rcumsumXRisk[t]  += rcumsumXRisk[t + 1];
          rcumsumXXRisk[t] += rcumsumXXRisk[t + 1];
        }
      }
    }
  }
}
