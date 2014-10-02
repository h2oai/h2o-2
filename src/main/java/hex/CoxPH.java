package hex;

import jsr166y.CountedCompleter;
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
import water.util.RString;

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

  public static class CoxPHModel extends Model {
    static final int API_WEAVER = 1; // This file has auto-generated doc & JSON fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from auto-generated code.

    @API(help = "Model parameters", json = true)
    final private CoxPH parameters;

    @Override public final CoxPH get_params() { return parameters; }
    @Override public final Request2 job() { return get_params(); }

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
    }

    public void toJavaHtml(StringBuilder sb) {
    }

    @Override
    protected float[] score0(double[] data, float[] preds) {
      return new float[0];
    }
  }

  CoxPHModel output;

  @Override public Response serve() {
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

    String[] names;
    if (use_start_column) {
      names = new String[4];
      int i = 0;
      for (int j = 0; j < source.numCols(); j++) {
        Vec vec = source.vec(j);
        if (vec == start_column ||
            vec == stop_column  ||
            vec == event_column ||
            vec == x_column) {
          names[i] = source.names()[j];
          i++;
        }
      }
    } else {
      names = new String[3];
      int i = 0;
      for (int j = 0; j < source.numCols(); j++) {
        Vec vec = source.vec(j);
        if (vec == stop_column  ||
            vec == event_column ||
            vec == x_column) {
          names[i] = source.names()[j];
          i++;
        }
      }
    }
    Frame cols = source.subframe(names);

    output = new CoxPHModel(this, dest(), cols._key, cols, null);

    H2O.H2OCountedCompleter task = new H2O.H2OCountedCompleter() {
      @Override
      public void compute2() {
        Vec[] cols;
        if (use_start_column) {
          cols = new Vec[4];
          cols[0] = start_column;
          cols[1] = stop_column;
          cols[2] = event_column;
          cols[3] = x_column;
        } else {
          cols    = new Vec[3];
          cols[0] = stop_column;
          cols[1] = event_column;
          cols[2] = x_column;
        }

        if (use_start_column)
          output.min_time = (long) start_column.min() + 1;
        else
          output.min_time = (long) stop_column.min();
        output.max_time   = (long) stop_column.max();
        int n_time = (int) (output.max_time - output.min_time + 1);
        output.x_mean     = x_column.mean();
        output.cumhaz     = MemoryManager.malloc8d(n_time);
        output.se_cumhaz  = MemoryManager.malloc8d(n_time);
        output.surv       = MemoryManager.malloc8d(n_time);
        double[] se_term  = MemoryManager.malloc8d(n_time);

        int    i, t;
        double step      = Double.NaN;
        double oldCoef   = Double.NaN;
        double oldLoglik = - Double.MAX_VALUE;
        double newCoef   = init;
        double newLoglik;
        for (i = 0; i <= iter_max; i++) {
          output.iter = i;

          // Map & Reduce
          CoxPHFitTask coxFit = new CoxPHFitTask(newCoef, output.min_time, n_time, use_start_column, output.x_mean).doAll(cols);
          // Finalize
          if (!use_start_column) {
            for (t = n_time - 2; t >= 0; t--) {
              coxFit.rcumsumRisk[t]   += coxFit.rcumsumRisk[t+1];
              coxFit.rcumsumXRisk[t]  += coxFit.rcumsumXRisk[t+1];
              coxFit.rcumsumXXRisk[t] += coxFit.rcumsumXXRisk[t+1];
            }
          }

          if (i == 0) {
            output.n = coxFit.n;
            for (t = 0; t < n_time; t++)
              output.total_event += coxFit.countEvents[t];
            output.n_risk   = coxFit.countRiskSet.clone();
            output.n_event  = coxFit.countEvents.clone();
            output.n_censor = coxFit.countCensored.clone();
            if (!use_start_column)
              for (t = n_time - 2; t >= 0; t--)
                output.n_risk[t] += output.n_risk[t+1];
          }

          newLoglik       = 0;
          output.gradient = 0;
          output.hessian  = 0;
          switch (ties) {
            case efron:
              for (t = n_time - 1; t >= 0; t--) {
                if (coxFit.countEvents[t] > 0) {
                  newLoglik += coxFit.sumLogRiskEvents[t];
                  output.gradient  += coxFit.sumXEvents[t];
                  for (long e = 0; e < coxFit.countEvents[t]; e++) {
                    double frac   = ((double) e) / ((double) coxFit.countEvents[t]);
                    double term   = coxFit.rcumsumRisk[t]   - frac * coxFit.sumRiskEvents[t];
                    double dterm  = coxFit.rcumsumXRisk[t]  - frac * coxFit.sumXRiskEvents[t];
                    double d2term = coxFit.rcumsumXXRisk[t] - frac * coxFit.sumXXRiskEvents[t];
                    double dlogTerm  = dterm / term;
                    newLoglik       -= Math.log(term);
                    output.gradient -= dlogTerm;
                    output.hessian  -= d2term / term - (dlogTerm * (dterm / term));
                  }
                }
              }
              break;
            case breslow:
              for (t = n_time - 1; t >= 0; t--) {
                if (coxFit.countEvents[t] > 0) {
                  newLoglik        += coxFit.sumLogRiskEvents[t];
                  output.gradient  += coxFit.sumXEvents[t];
                  double dlogTerm   = coxFit.rcumsumXRisk[t] / coxFit.rcumsumRisk[t];
                  newLoglik        -= coxFit.countEvents[t] * Math.log(coxFit.rcumsumRisk[t]);
                  output.gradient  -= coxFit.countEvents[t] * dlogTerm;
                  output.hessian   -= coxFit.countEvents[t] *
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
              output.null_loglik = newLoglik;
              output.maxrsq      = 1 - Math.exp(2 * output.null_loglik / output.n);
              output.score_test  = - output.gradient * output.gradient / output.hessian;
            }
            output.coef          = newCoef;
            output.exp_coef      = Math.exp(output.coef);
            output.exp_neg_coef  = Math.exp(- output.coef);
            output.var_coef      = - 1 / output.hessian;
            output.se_coef       = Math.sqrt(output.var_coef);
            output.z_coef        = output.coef / output.se_coef;
            output.loglik        = newLoglik;
            output.loglik_test   = - 2 * (output.null_loglik - output.loglik);
            double diff_init     = output.coef - init;
            output.wald_test     = (diff_init * diff_init) / output.var_coef;
            output.rsq           = 1 - Math.exp(- output.loglik_test / output.n);

            switch (ties) {
              case efron:
                for (t = 0; t < n_time; t++) {
                  output.cumhaz[t]    = 0;
                  output.se_cumhaz[t] = 0;
                  se_term[t]          = 0;
                  for (long e = 0; e < coxFit.countEvents[t]; e++) {
                    double frac = ((double) e) / ((double) coxFit.countEvents[t]);
                    double haz  = 1 / (coxFit.rcumsumRisk[t] - frac * coxFit.sumRiskEvents[t]);
                    output.cumhaz[t]    += haz;
                    output.se_cumhaz[t] += haz * haz;
                    se_term[t]          += (coxFit.rcumsumXRisk[t] - frac * coxFit.sumXRiskEvents[t]) * haz * haz;
                  }
                }
                break;
              case breslow:
                for (t = 0; t < n_time; t++) {
                  output.cumhaz[t]    = coxFit.countEvents[t] / coxFit.rcumsumRisk[t];
                  output.se_cumhaz[t] = coxFit.countEvents[t] / (coxFit.rcumsumRisk[t] * coxFit.rcumsumRisk[t]);
                  se_term[t]          = (coxFit.rcumsumXRisk[t] / coxFit.rcumsumRisk[t]) * output.cumhaz[t];
                }
                break;
              default:
                throw new IllegalArgumentException("ties method must be either efron or breslow");
            }

            for (t = 1; t < n_time; t++) {
              output.cumhaz[t]    = output.cumhaz[t - 1] + output.cumhaz[t];
              output.se_cumhaz[t] = output.se_cumhaz[t - 1] + output.se_cumhaz[t];
              se_term[t]   = se_term[t - 1] + se_term[t];
            }

            for (t = 0; t < n_time; t++) {
              output.se_cumhaz[t] = Math.sqrt(output.se_cumhaz[t] + (se_term[t] * output.var_coef * se_term[t]));
              output.surv[t]      = Math.exp(- output.cumhaz[t]);
            }

            if (newLoglik == 0)
              output.lre = - Math.log10(Math.abs(oldLoglik - newLoglik));
            else
              output.lre = - Math.log10(Math.abs((oldLoglik - newLoglik) / newLoglik));
            if (output.lre >= lre_min)
              break;

            step = output.gradient / output.hessian;
            if (Double.isNaN(step) || Double.isInfinite(step))
              break;

            oldCoef   = newCoef;
            oldLoglik = newLoglik;
          }
          else
            step /= 2;

          newCoef = oldCoef - step;
        }
        tryComplete();
      }

      @Override public void onCompletion(CountedCompleter cc) {
        Futures fs = new Futures();
        DKV.put(dest(), output, fs);
        fs.blockForPending();
        remove();
      }
    };
    start(task);
    H2O.submitTask(task);

    return CoxPHProgressPage.redirect(this, self(), dest());
  }

  public static class CoxPHFitTask extends MRTask2<CoxPHFitTask> {
    private final double  _beta;
    private final int     _n_time;
    private final long    _min_time;
    private final boolean _use_start_column;
    private final double  _x_mean;

    long     n;
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
        if (!Double.isNaN(x_i)) {
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
      n += that.n;
      for (int t = 0; t < _n_time; t++) {
        countRiskSet[t]     += that.countRiskSet[t];
        countCensored[t]    += that.countCensored[t];
        countEvents[t]      += that.countEvents[t];
        sumXEvents[t]       += that.sumXEvents[t];
        sumRiskEvents[t]    += that.sumRiskEvents[t];
        sumXRiskEvents[t]   += that.sumXRiskEvents[t];
        sumXXRiskEvents[t]  += that.sumXXRiskEvents[t];
        sumLogRiskEvents[t] += that.sumLogRiskEvents[t];
        rcumsumRisk[t]      += that.rcumsumRisk[t];
        rcumsumXRisk[t]     += that.rcumsumXRisk[t];
        rcumsumXXRisk[t]    += that.rcumsumXXRisk[t];
      }
    }
  }
}
