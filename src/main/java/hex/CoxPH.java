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
import water.util.Utils;

public class CoxPH extends Job {
  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Cox Proportional Hazards Model with 1 predictor";

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

  private class CoxPHVecSelect extends VecSelect { CoxPHVecSelect() { super("source"); } }

  public static enum CoxPHTies { efron, breslow; }

  public static final int MAX_TIME_BINS = 10000;

  public static class CoxPHModel extends Model implements Job.Progress {
    static final int API_WEAVER = 1; // This file has auto-generated doc & JSON fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from auto-generated code.

    @API(help = "Model parameters", json = true)
    final private CoxPH parameters;

    @Override public final CoxPH get_params() { return parameters; }
    @Override public final Request2 job() { return get_params(); }
    @Override public float progress() { return (float) iter / (float) get_params().iter_max; }

    @API(help = "names of coefficients")
    String names_coef;   // vector
    @API(help = "coefficients")
    double coef;         // vector
    @API(help = "exp(coefficients)")
    double exp_coef;     // vector
    @API(help = "exp(-coefficients)")
    double exp_neg_coef; // vector
    @API(help = "se(coefficients)")
    double se_coef;      // vector
    @API(help = "z-score")
    double z_coef;       // vector
    @API(help = "var(coefficients)")
    double var_coef;     // matrix
    @API(help = "null log-likelihood")
    double null_loglik;  // scalar
    @API(help = "log-likelihood")
    double loglik;       // scalar
    @API(help = "log-likelihood test stat")
    double loglik_test;  // scalar
    @API(help = "Wald test stat")
    double wald_test;    // scalar
    @API(help = "Score test stat")
    double score_test;   // scalar
    @API(help = "R-square")
    double rsq;          // scalar
    @API(help = "Maximum R-square")
    double maxrsq;       // scalar
    @API(help = "gradient")
    double gradient;     // vector
    @API(help = "Hessian")
    double hessian;      // matrix
    @API(help = "log relative error")
    double lre;          // scalar
    @API(help = "number of iterations")
    int iter;            // scalar
    @API(help = "mean of x column")
    double x_mean;       // scalar
    @API(help = "n")
    long n;              // scalar
    @API(help = "number of rows with missing values")
    long n_missing;      // scalar
    @API(help = "total events")
    long total_event;    // scalar
    @API(help = "minimum time")
    long min_time;       // scalar
    @API(help = "maximum time")
    long max_time;       // scalar
    @API(help = "number at risk")
    long[] n_risk;       // vector
    @API(help = "number of events")
    long[] n_event;      // vector
    @API(help = "number of censored obs")
    long[] n_censor;     // vector
    @API(help = "cumulative hazard")
    double[] cumhaz;     // vector
    @API(help = "se(cumulative hazard)")
    double[] se_cumhaz;  // vector
    @API(help = "survival function")
    double[] surv;       // vector

    public CoxPHModel(CoxPH job, Key selfKey, Key dataKey, Frame fr, float[] priorClassDist) {
      super(selfKey, dataKey, fr, priorClassDist);
      parameters = (CoxPH) job.clone();
    }

    public void generateHTML(String title, StringBuilder sb) {
      DocGen.HTML.title(sb, title);

      sb.append("<h4>Data</h4>");
      sb.append("<table class='table table-striped table-bordered table-condensed'><col width=\"25%\"><col width=\"75%\">");
      sb.append("<tr><th>Number of Complete Cases</th><td>"           + n           + "</td></tr>");
      sb.append("<tr><th>Number of Non Complete Cases</th><td>"       + n_missing   + "</td></tr>");
      sb.append("<tr><th>Number of Events in Complete Cases</th><td>" + total_event + "</td></tr>");
      sb.append("</table>");

      sb.append("<h4>Coefficients</h4>");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr><th></th><td>coef</td><td>exp(coef)</td><td>se(coef)</td><td>z</td></tr>");
      sb.append("<tr><th>" + names_coef + "</th><td>" + coef + "</td><td>" + exp_coef + "</td><td>" + se_coef + "</td><td>" + z_coef + "</td></tr>");
      sb.append("</table>");

      sb.append("<h4>Model Statistics</h4>");
      sb.append("<table class='table table-striped table-bordered table-condensed'><col width=\"15%\"><col width=\"85%\">");
      sb.append("<tr><th>Rsquare</th><td>" + String.format("%.3f", rsq) + " (max possible = " + String.format("%.3f", maxrsq) + ")</td></tr>");
      sb.append("<tr><th>Likelihood ratio test</th><td>" + String.format("%.2f", loglik_test) + " on 1 df</td></tr>");
      sb.append("<tr><th>Wald test            </th><td>" + String.format("%.2f", wald_test)   + " on 1 df</td></tr>");
      sb.append("<tr><th>Score (logrank) test </th><td>" + String.format("%.2f", score_test)  + " on 1 df</td></tr>");
      sb.append("</table>");
    }

    public void toJavaHtml(StringBuilder sb) {
    }

    @Override
    protected float[] score0(double[] data, float[] preds) {
      throw H2O.unimpl();
    }
  }

  CoxPHModel model;

  @Override
  public void execImpl() {
    try {
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
      if (n_time > MAX_TIME_BINS)
        throw new IllegalArgumentException("The time number of time points is " + n_time +
                                           "; allowed maximum is " + MAX_TIME_BINS);

      String[] names;
      if (use_start_column) {
        names = new String[4];
        names[0] = source.names()[source.find(start_column)];
        names[1] = source.names()[source.find(stop_column)];
        names[2] = source.names()[source.find(event_column)];
        names[3] = source.names()[source.find(x_column)];
      } else {
        names = new String[3];
        names[0] = source.names()[source.find(stop_column)];
        names[1] = source.names()[source.find(event_column)];
        names[2] = source.names()[source.find(x_column)];
      }
      source = source.subframe(names);

      model = new CoxPHModel(this, dest(), source._key, source, null);

      H2O.H2OCountedCompleter task = new H2O.H2OCountedCompleter() {
        @Override
        public void compute2() {
          Vec[] cols = source.vecs();

          if (use_start_column) {
            model.names_coef = source.names()[3];
            model.min_time   = (long) start_column.min() + 1;
          } else {
            model.names_coef = source.names()[2];
            model.min_time   = (long) stop_column.min();
          }
          model.max_time     = (long) stop_column.max();
          int n_time         = (int) (model.max_time - model.min_time + 1);
          model.x_mean       = x_column.mean();
          model.cumhaz       = MemoryManager.malloc8d(n_time);
          model.se_cumhaz    = MemoryManager.malloc8d(n_time);
          model.surv         = MemoryManager.malloc8d(n_time);
          double[] se_term   = MemoryManager.malloc8d(n_time);

          int i, t;
          double step      = Double.NaN;
          double oldCoef   = Double.NaN;
          double oldLoglik = - Double.MAX_VALUE;
          double newCoef   = init;
          double newLoglik;
          for (i = 0; i <= iter_max; i++) {
            model.iter = i;

            // Map & Reduce
            CoxPHFitTask coxFit = new CoxPHFitTask(newCoef, model.min_time, n_time,
                                                   use_start_column, model.x_mean).doAll(cols);
            // Finalize
            if (!use_start_column) {
              for (t = n_time - 2; t >= 0; t--) {
                coxFit.rcumsumRisk[t]   += coxFit.rcumsumRisk[t + 1];
                coxFit.rcumsumXRisk[t]  += coxFit.rcumsumXRisk[t + 1];
                coxFit.rcumsumXXRisk[t] += coxFit.rcumsumXXRisk[t + 1];
              }
            }

            if (i == 0) {
              model.n              = coxFit.n;
              model.n_missing      = coxFit.n_missing;
              for (t = 0; t < n_time; t++)
                model.total_event += coxFit.countEvents[t];
              model.n_risk         = coxFit.countRiskSet.clone();
              model.n_event        = coxFit.countEvents.clone();
              model.n_censor       = coxFit.countCensored.clone();
              if (!use_start_column)
                for (t = n_time - 2; t >= 0; t--)
                  model.n_risk[t] += model.n_risk[t + 1];
            }

            newLoglik      = 0;
            model.gradient = 0;
            model.hessian  = 0;
            switch (ties) {
              case efron:
                for (t = n_time - 1; t >= 0; t--) {
                  if (coxFit.countEvents[t] > 0) {
                    newLoglik      += coxFit.sumLogRiskEvents[t];
                    model.gradient += coxFit.sumXEvents[t];
                    for (long e = 0; e < coxFit.countEvents[t]; e++) {
                      double frac      = ((double) e) / ((double) coxFit.countEvents[t]);
                      double term      = coxFit.rcumsumRisk[t]   - frac * coxFit.sumRiskEvents[t];
                      double dterm     = coxFit.rcumsumXRisk[t]  - frac * coxFit.sumXRiskEvents[t];
                      double d2term    = coxFit.rcumsumXXRisk[t] - frac * coxFit.sumXXRiskEvents[t];
                      double dlogTerm  = dterm / term;
                      newLoglik       -= Math.log(term);
                      model.gradient  -= dlogTerm;
                      model.hessian   -= d2term / term - (dlogTerm * (dterm / term));
                    }
                  }
                }
                break;
              case breslow:
                for (t = n_time - 1; t >= 0; t--) {
                  if (coxFit.countEvents[t] > 0) {
                    newLoglik       += coxFit.sumLogRiskEvents[t];
                    model.gradient  += coxFit.sumXEvents[t];
                    double dlogTerm  = coxFit.rcumsumXRisk[t] / coxFit.rcumsumRisk[t];
                    newLoglik       -= coxFit.countEvents[t] * Math.log(coxFit.rcumsumRisk[t]);
                    model.gradient  -= coxFit.countEvents[t] * dlogTerm;
                    model.hessian   -= coxFit.countEvents[t] *
                      (((coxFit.rcumsumXXRisk[t] / coxFit.rcumsumRisk[t]) -
                        (dlogTerm * (coxFit.rcumsumXRisk[t] / coxFit.rcumsumRisk[t]))));
                  }
                }
                break;
              default:
                throw new IllegalArgumentException("ties method must be either efron or breslow");
            }

            if (newLoglik > oldLoglik) {
              if (i == 0) {
                model.null_loglik = newLoglik;
                model.maxrsq      = 1 - Math.exp(2 * model.null_loglik / model.n);
                model.score_test  = -  model.gradient * model.gradient / model.hessian;
              }
              model.coef          = newCoef;
              model.exp_coef      = Math.exp(model.coef);
              model.exp_neg_coef  = Math.exp(- model.coef);
              model.var_coef      = - 1 / model.hessian;
              model.se_coef       = Math.sqrt(model.var_coef);
              model.z_coef        = model.coef / model.se_coef;
              model.loglik        = newLoglik;
              model.loglik_test   = - 2 * (model.null_loglik - model.loglik);
              double diff_init    = model.coef - init;
              model.wald_test     = (diff_init * diff_init) / model.var_coef;
              model.rsq           = 1 - Math.exp(- model.loglik_test / model.n);

              switch (ties) {
                case efron:
                  for (t = 0; t < n_time; t++) {
                    model.cumhaz[t]    = 0;
                    model.se_cumhaz[t] = 0;
                    se_term[t]         = 0;
                    for (long e = 0; e < coxFit.countEvents[t]; e++) {
                      double frac = ((double) e) / ((double) coxFit.countEvents[t]);
                      double haz  = 1 / (coxFit.rcumsumRisk[t] - frac * coxFit.sumRiskEvents[t]);
                      model.cumhaz[t]    += haz;
                      model.se_cumhaz[t] += haz * haz;
                      se_term[t]         += (coxFit.rcumsumXRisk[t] - frac * coxFit.sumXRiskEvents[t]) * haz * haz;
                    }
                  }
                  break;
                case breslow:
                  for (t = 0; t < n_time; t++) {
                    model.cumhaz[t]    = coxFit.countEvents[t] / coxFit.rcumsumRisk[t];
                    model.se_cumhaz[t] = coxFit.countEvents[t] / (coxFit.rcumsumRisk[t] * coxFit.rcumsumRisk[t]);
                    se_term[t]         = (coxFit.rcumsumXRisk[t] / coxFit.rcumsumRisk[t]) * model.cumhaz[t];
                  }
                  break;
                default:
                  throw new IllegalArgumentException("ties method must be either efron or breslow");
              }

              for (t = 1; t < n_time; t++) {
                model.cumhaz[t]    = model.cumhaz[t - 1]    + model.cumhaz[t];
                model.se_cumhaz[t] = model.se_cumhaz[t - 1] + model.se_cumhaz[t];
                se_term[t]         = se_term[t - 1] + se_term[t];
              }

              for (t = 0; t < n_time; t++) {
                model.se_cumhaz[t] = Math.sqrt(model.se_cumhaz[t] + (se_term[t] * model.var_coef * se_term[t]));
                model.surv[t]      = Math.exp(- model.cumhaz[t]);
              }

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
          remove();
          tryComplete();
        }
      };
      start(task);
      H2O.submitTask(task);
    } catch (Throwable t) {
      t.printStackTrace();
      cancel(t);
    }
  }

  @Override public Response serve() {
    execImpl();
    return CoxPHProgressPage.redirect(this, self(), dest());
  }

  public static class CoxPHFitTask extends MRTask2<CoxPHFitTask> {
    private final double  _beta;
    private final int     _n_time;
    private final long    _min_time;
    private final boolean _use_start_column;
    private final double  _x_mean;

    long     n;
    long     n_missing;
    long[]   countRiskSet;
    long[]   countCensored;
    long[]   countEvents;
    double[] sumXEvents;
    double[] sumRiskEvents;
    double[] sumXRiskEvents;
    double[] sumXXRiskEvents;
    double[] sumLogRiskEvents;
    double[] rcumsumRisk;
    double[] rcumsumXRisk;
    double[] rcumsumXXRisk;

    CoxPHFitTask(final double beta, final long min_time, final int n_time, final boolean use_start_column,
                 final double x_mean) {
      _beta             = beta;
      _n_time           = n_time;
      _min_time         = min_time;
      _use_start_column = use_start_column;
      _x_mean           = x_mean;
    }

    @Override public void map(Chunk[] cols) {
      Chunk start, stop, events, xs;
      if (_use_start_column) {
        start  = cols[0];
        stop   = cols[1];
        events = cols[2];
        xs     = cols[3];
      } else {
        start  = null;
        stop   = cols[0];
        events = cols[1];
        xs     = cols[2];
      }

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
              throw new IllegalArgumentException("start values must be strictly less than stop values");
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

    @Override public void reduce(CoxPHFitTask that) {
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
  }
}
