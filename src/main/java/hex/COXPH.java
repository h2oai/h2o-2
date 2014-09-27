package hex;

import water.Key;
import water.MemoryManager;
import water.MRTask2;
import water.Request2;
import water.api.DocGen;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.RString;

public class COXPH extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-generated doc & JSON fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from auto-generated code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Cox Proportional Hazards Model with 1 predictor";

  @API(help="Data Frame",        required=true,  filter=Default.class)
  public Frame source;

  @API(help="",                  required=true,  filter=Default.class)
  public boolean use_start_column = true;

  @API(help="Start Time Column", required=false, filter=COXPHVecSelect.class)
  public Vec start_column;

  @API(help="Stop Time Column",  required=true,  filter=COXPHVecSelect.class)
  public Vec stop_column;

  @API(help="Event Column",      required=true,  filter=COXPHVecSelect.class)
  public Vec event_column;

  @API(help="X Column",          required=true,  filter=COXPHVecSelect.class)
  public Vec x_column;

  private class COXPHVecSelect extends VecSelect { COXPHVecSelect() { super("source"); } }

  @API(help="coefficients")             double   coef;         // vector
  @API(help="exp(coefficients)")        double   exp_coef;     // vector
  @API(help="exp(-coefficients)")       double   exp_neg_coef; // vector
  @API(help="se(coefficients)")         double   se_coef;      // vector
  @API(help="z-score")                  double   z_coef;       // vector
  @API(help="var(coefficients)")        double   var_coef;     // matrix
  @API(help="null log-likelihood")      double   null_loglik;  // scalar
  @API(help="log-likelihood")           double   loglik;       // scalar
  @API(help="log-likelihood test stat") double   loglik_test;  // scalar
  @API(help="Wald test stat")           double   wald_test;    // scalar
  @API(help="gradient")                 double   gradient;     // vector
  @API(help="Hessian")                  double   hessian;      // matrix
  @API(help="log relative error")       double   lre;          // scalar
  @API(help="number of iterations")     int      iter;         // scalar
  @API(help="mean of x column")         double   x_mean;       // scalar
  @API(help="n")                        long     n;            // scalar
  @API(help="total events")             long     total_event;  // scalar
  @API(help="minimum time")             long     min_time;     // scalar
  @API(help="maximum time")             long     max_time;     // scalar
  @API(help="number at risk")           long[]   n_risk;       // vector
  @API(help="number of events")         long[]   n_event;      // vector
  @API(help="number of censored obs")   long[]   n_censor;     // vector
  @API(help="cumulative hazard")        double[] cumhaz;       // vector
  @API(help="se(cumulative hazard)")    double[] se_cumhaz;    // vector
  @API(help="survival function")        double[] surv;         // vector

  @Override public Response serve() {
    if (use_start_column && !start_column.isInt())
      throw new IllegalArgumentException("start time must be null or of type integer");

    if (!stop_column.isInt())
      throw new IllegalArgumentException("stop time must be of type integer");

    if (!event_column.isInt() && !event_column.isEnum())
      throw new IllegalArgumentException("event must be of type integer or factor");

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
      min_time = (long) start_column.min() + 1;
    else
      min_time = (long) stop_column.min();
    max_time   = (long) stop_column.max();
    int n_time = (int) (max_time - min_time + 1);
    x_mean     = x_column.mean();
    cumhaz     = MemoryManager.malloc8d(n_time);
    se_cumhaz  = MemoryManager.malloc8d(n_time);
    surv       = MemoryManager.malloc8d(n_time);
    double[] se_term = MemoryManager.malloc8d(n_time);

    int    i, t;
    double step      = Double.NaN;
    double oldCoef   = Double.NaN;
    double oldLoglik = - Double.MAX_VALUE;
    double newCoef   = 0;
    double newLoglik;
    for (i = 0; i < 100; i++) {
      iter = i + 1;

      // Map & Reduce
      CoxphFitTask cox1 = new CoxphFitTask(newCoef, min_time, n_time, use_start_column, x_mean).doAll(cols);
      // Finalize
      if (!use_start_column) {
        for (t = n_time - 2; t >= 0; t--) {
          cox1.rcumsumRisk[t]   += cox1.rcumsumRisk[t+1];
          cox1.rcumsumXRisk[t]  += cox1.rcumsumXRisk[t+1];
          cox1.rcumsumXXRisk[t] += cox1.rcumsumXXRisk[t+1];
        }
      }

      if (i == 0) {
        n = cox1.n;
        for (t = 0; t < n_time; t++)
          total_event += cox1.countEvents[t];
        n_risk   = cox1.countRiskSet.clone();
        n_event  = cox1.countEvents.clone();
        n_censor = cox1.countCensored.clone();
        if (!use_start_column)
          for (t = n_time - 2; t >= 0; t--)
            n_risk[t] += n_risk[t+1];
      }

      newLoglik = 0;
      gradient  = 0;
      hessian   = 0;
      for (t = n_time - 1; t >= 0; t--) {
        if (cox1.countEvents[t] > 0) {
          newLoglik += cox1.sumLogRiskEvents[t];
          gradient  += cox1.sumXEvents[t];
          double gamma = cox1.rcumsumXRisk[t] / cox1.rcumsumRisk[t];
          newLoglik -= cox1.countEvents[t] * Math.log(cox1.rcumsumRisk[t]);
          gradient  -= cox1.countEvents[t] * gamma;
          hessian   -= cox1.countEvents[t] *
                       (((cox1.rcumsumXXRisk[t]  / cox1.rcumsumRisk[t]) -
                         (gamma * (cox1.rcumsumXRisk[t] / cox1.rcumsumRisk[t]))));
        }
      }

      if (i == 0)
        null_loglik = newLoglik;

      if (newLoglik > oldLoglik) {
        coef         = newCoef;
        exp_coef     = Math.exp(coef);
        exp_neg_coef = Math.exp(-coef);
        var_coef     = -1 / hessian;
        se_coef      = Math.sqrt(var_coef);
        z_coef       = coef / se_coef;
        loglik       = newLoglik;
        loglik_test  = -2 * (null_loglik - loglik);
        wald_test    = coef * coef / var_coef;

        for (t = 0; t < n_time; t++) {
          cumhaz[t]    = cox1.countEvents[t] / cox1.rcumsumRisk[t];
          se_cumhaz[t] = cox1.countEvents[t] / (cox1.rcumsumRisk[t] * cox1.rcumsumRisk[t]);
          se_term[t]   = (cox1.rcumsumXRisk[t] / cox1.rcumsumRisk[t]) * cumhaz[t];
        }

        for (t = 1; t < n_time; t++) {
          cumhaz[t]    = cumhaz[t - 1] + cumhaz[t];
          se_cumhaz[t] = se_cumhaz[t - 1] + se_cumhaz[t];
          se_term[t]   = se_term[t - 1] + se_term[t];
        }

        for (t = 0; t < n_time; t++) {
          se_cumhaz[t] = Math.sqrt(se_cumhaz[t] + (se_term[t] * var_coef * se_term[t]));
          surv[t]      = Math.exp(-cumhaz[t]);
        }

        if (newLoglik == 0)
          lre = -Math.log10(Math.abs(oldLoglik - newLoglik));
        else
          lre = -Math.log10(Math.abs((oldLoglik - newLoglik) / newLoglik));
        if (lre > 9)
          break;

        step = gradient / hessian;
        if (Double.isNaN(step) || Double.isInfinite(step))
          break;

        oldCoef   = newCoef;
        oldLoglik = newLoglik;
      }
      else
        step /= 2;

      newCoef = oldCoef - step;
    }

    return Response.done(this);
  }

  public static class CoxphFitTask extends MRTask2<CoxphFitTask> {
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
    double[] sumLogRiskEvents;
    double[] rcumsumRisk;
    double[] rcumsumXRisk;
    double[] rcumsumXXRisk;

    CoxphFitTask(final double beta, final long min_time, final int n_time, final boolean use_start_column,
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
    @Override public void reduce(CoxphFitTask that) {
      n += that.n;
      for (int t = 0; t < _n_time; t++) {
        countRiskSet[t]  += that.countRiskSet[t];
        countCensored[t] += that.countCensored[t];
        countEvents[t]   += that.countEvents[t];
        sumXEvents[t]    += that.sumXEvents[t];
        rcumsumRisk[t]   += that.rcumsumRisk[t];
        rcumsumXRisk[t]  += that.rcumsumXRisk[t];
        rcumsumXXRisk[t] += that.rcumsumXXRisk[t];
      }
    }
  }

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='COXPH.query?data_key=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }
}
