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

  @API(help="coef")                     double coef;         // vector
  @API(help="exp(coef)")                double exp_coef;     // vector
  @API(help="exp(-coef)")               double exp_neg_coef; // vector
  @API(help="se(coef)")                 double se_coef;      // vector
  @API(help="z-score")                  double z_coef;       // vector
  @API(help="var(coef)")                double var_coef;     // matrix
  @API(help="null log-likelihood")      double null_loglik;  // scalar
  @API(help="log-likelihood")           double loglik;       // scalar
  @API(help="log-likelihood test stat") double loglik_test;  // scalar
  @API(help="Wald test stat")           double wald_test;    // scalar
  @API(help="gradient")                 double gradient;     // vector
  @API(help="Hessian")                  double hessian;      // matrix
  @API(help="log relative error")       double lre;          // scalar
  @API(help="number of iterations")     int    iter;         // scalar
  @API(help="mean of x column")         double x_mean;       // scalar
  @API(help="n")                        long   n;            // scalar
  @API(help="total events")             long   total_event;  // scalar
  @API(help="minimum stop time")        long   min_time;     // scalar
  @API(help="maximum stop time")        long   max_time;     // scalar
  @API(help="number at risk")           long[] n_risk;       // vector
  @API(help="number of events")         long[] n_event;      // vector
  @API(help="number of censored obs")   long[] n_censor;     // vector

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
          cox1.cumsumExpXBeta[t]   += cox1.cumsumExpXBeta[t+1];
          cox1.cumsumXExpXBeta[t]  += cox1.cumsumXExpXBeta[t+1];
          cox1.cumsumXXExpXBeta[t] += cox1.cumsumXXExpXBeta[t+1];
        }
      }

      if (i == 0) {
        n = cox1.n;
        for (t = 0; t < n_time; t++)
          total_event += cox1.countEvents[t];
        n_risk   = cox1.countRisk.clone();
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
          newLoglik += cox1.sumXBetaEvents[t];
          gradient  += cox1.sumXEvents[t];
          double gamma = cox1.cumsumXExpXBeta[t] / cox1.cumsumExpXBeta[t];
          newLoglik -= cox1.countEvents[t] * Math.log(cox1.cumsumExpXBeta[t]);
          gradient  -= cox1.countEvents[t] * gamma;
          hessian   -= cox1.countEvents[t] *
                       (((cox1.cumsumXXExpXBeta[t]  / cox1.cumsumExpXBeta[t]) -
                         (gamma * (cox1.cumsumXExpXBeta[t] / cox1.cumsumExpXBeta[t]))));
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
    long[]   countRisk;
    long[]   countCensored;
    long[]   countEvents;
    double[] sumXEvents;
    double[] sumXBetaEvents;
    double[] cumsumExpXBeta;
    double[] cumsumXExpXBeta;
    double[] cumsumXXExpXBeta;

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
      double x_i, xbeta_i, expXBeta_i, xExpXBeta_i, xxExpXBeta_i;
      countRisk        = MemoryManager.malloc8(_n_time);
      countCensored    = MemoryManager.malloc8(_n_time);
      countEvents      = MemoryManager.malloc8(_n_time);
      sumXEvents       = MemoryManager.malloc8d(_n_time);
      sumXBetaEvents   = MemoryManager.malloc8d(_n_time);
      cumsumExpXBeta   = MemoryManager.malloc8d(_n_time);
      cumsumXExpXBeta  = MemoryManager.malloc8d(_n_time);
      cumsumXXExpXBeta = MemoryManager.malloc8d(_n_time);
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
          xbeta_i      = x_i * _beta;
          expXBeta_i   = Math.exp(xbeta_i);
          xExpXBeta_i  = x_i * expXBeta_i;
          xxExpXBeta_i = x_i * xExpXBeta_i;
          n++;
          if (event_i > 0) {
            countEvents[t2]++;
            sumXEvents[t2] += x_i;
            sumXBetaEvents[t2] += xbeta_i;
          } else
            countCensored[t2]++;
          if (_use_start_column) {
            for (t = t1; t <= t2; t++) {
              countRisk[t]++;
              cumsumExpXBeta[t]   += expXBeta_i;
              cumsumXExpXBeta[t]  += xExpXBeta_i;
              cumsumXXExpXBeta[t] += xxExpXBeta_i;
            }
          } else {
            countRisk[t2]++;
            cumsumExpXBeta[t2]   += expXBeta_i;
            cumsumXExpXBeta[t2]  += xExpXBeta_i;
            cumsumXXExpXBeta[t2] += xxExpXBeta_i;
          }
        }
      }
    }
    @Override public void reduce(CoxphFitTask that) {
      n += that.n;
      for (int t = 0; t < _n_time; t++) {
        countRisk[t]     += that.countRisk[t];
        countCensored[t] += that.countCensored[t];
        countEvents[t]   += that.countEvents[t];
        sumXEvents[t]    += that.sumXEvents[t];
        cumsumExpXBeta[t]   += that.cumsumExpXBeta[t];
        cumsumXExpXBeta[t]  += that.cumsumXExpXBeta[t];
        cumsumXXExpXBeta[t] += that.cumsumXXExpXBeta[t];
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
