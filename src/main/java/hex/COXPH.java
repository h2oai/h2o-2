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

public class COXPH extends Request2
{
  static final int API_WEAVER = 1; // This file has auto-generated doc & JSON fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from auto-generated code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Cox Proportional Hazards Model with 1 predictor";

  @API(help="Data Frame",        required=true,  filter=Default.class)
  Frame source;

  @API(help="Column Start Time", required=false, filter=COXPHVecSelect.class)
  Vec vec_start;

  @API(help="Column Stop Time",  required=true,  filter=COXPHVecSelect.class)
  Vec vec_stop;

  @API(help="Column Event",      required=true,  filter=COXPHVecSelect.class)
  Vec vec_event;

  @API(help="Column X",          required=true,  filter=COXPHVecSelect.class)
  Vec vec_x;

  class COXPHVecSelect extends VecSelect { COXPHVecSelect() { super("source"); } }

  @API(help="coef")                     double coef;        // vector
  @API(help="exp(coef)")                double exp_coef;    // vector
  @API(help="se(coef)")                 double se_coef;     // vector
  @API(help="z-score")                  double z_coef;      // vector
  @API(help="var(coef)")                double var_coef;    // matrix
  @API(help="log-likelihood")           double loglik;      // scalar
  @API(help="gradient")                 double gradient;    // vector
  @API(help="Hessian")                  double hessian;     // matrix
  @API(help="log relative error")       double lre;         // scalar
  @API(help="number of iterations")     int    iter;        // scalar
  @API(help="n")                        long   n;           // scalar
  @API(help="total events")             long   total_event; // scalar
  @API(help="minimum stop time")        long   min_time;    // scalar
  @API(help="maximum stop time")        long   max_time;    // scalar
  @API(help="number at risk")           long[] n_risk;      // vector
  @API(help="number of events")         long[] n_event;     // vector
  @API(help="number of censored obs")   long[] n_censor;    // vector

  @Override public Response serve()
  {
    if (vec_start != null && !vec_start.isInt())
      throw new IllegalArgumentException("start time must be null or of type integer");

    if (!vec_stop.isInt())
      throw new IllegalArgumentException("stop time must be of type integer");

    if (!vec_event.isInt() && !vec_event.isEnum())
      throw new IllegalArgumentException("event must be of type integer or factor");

    min_time    = (long) vec_stop.min();
    max_time    = (long) vec_stop.max();
    int n_time  = (int) (max_time - min_time + 1);
    n_risk   = MemoryManager.malloc8(n_time);
    n_event  = MemoryManager.malloc8(n_time);
    n_censor = MemoryManager.malloc8(n_time);

    int    i, t;
    double step      = Double.NaN;
    double oldCoef   = Double.NaN;
    double oldLoglik = - Double.MAX_VALUE;
    double newCoef   = 0;
    double newLoglik;
    for (i = 0; i < 100; i++)
    {
      iter = i + 1;

      CoxphFitTask cox1 = new CoxphFitTask(newCoef, min_time, n_time).doAll(vec_stop, vec_event, vec_x);
      if (i == 0)
      {
        n = cox1.n;
        t = n_time - 1;
        total_event = cox1.countEvents[t];
        n_risk[t]   = cox1.countEvents[t] + cox1.countCensored[t];
        n_event[t]  = cox1.countEvents[t];
        for (t = n_time - 2; t >= 0; t--) {
          total_event += cox1.countEvents[t];
          n_risk[t]    = n_risk[t + 1] + (cox1.countEvents[t] + cox1.countCensored[t]);
          n_event[t]   = cox1.countEvents[t];
          n_censor[t]  = cox1.countCensored[t];
        }
      }

      newLoglik = 0;
      gradient  = 0;
      hessian   = 0;
      double cumsumExpXBeta   = 0;
      double cumsumXExpXBeta  = 0;
      double cumsumXXExpXBeta = 0;
      for (t = n_time - 1; t >= 0; t--)
      {
        cumsumExpXBeta   += cox1.sumExpXBeta[t];
        cumsumXExpXBeta  += cox1.sumXExpXBeta[t];
        cumsumXXExpXBeta += cox1.sumXXExpXBeta[t];
        if (cox1.countEvents[t] > 0)
        {
          newLoglik += cox1.sumXBetaEvents[t];
          gradient  += cox1.sumXEvents[t];
          double gamma = cumsumXExpXBeta / cumsumExpXBeta;
          newLoglik -= cox1.countEvents[t] * Math.log(cumsumExpXBeta);
          gradient  -= cox1.countEvents[t] * gamma;
          hessian   -= cox1.countEvents[t] *
                       (((cumsumXXExpXBeta  / cumsumExpXBeta) - (gamma * (cumsumXExpXBeta / cumsumExpXBeta))));
        }
      }

      if (newLoglik > oldLoglik)
      {
        coef     = newCoef;
        exp_coef = Math.exp(coef);
        var_coef = -1 / hessian;
        se_coef  = Math.sqrt(var_coef);
        z_coef   = coef / se_coef;
        loglik   = newLoglik;

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

  public static class CoxphFitTask extends MRTask2<CoxphFitTask>
  {
    private final double _beta;
    private final int    _n_time;
    private final long   _min_time;

    long     n;
    long[]   countCensored;
    long[]   countEvents;
    double[] sumXEvents;
    double[] sumXBetaEvents;
    double[] sumExpXBeta;
    double[] sumXExpXBeta;
    double[] sumXXExpXBeta;

    CoxphFitTask(final double beta, final long min_time, final int n_time)
    {
      _beta     = beta;
      _n_time   = n_time;
      _min_time = min_time;
    }

    @Override public void map(Chunk stop, Chunk events, Chunk xs)
    {
      countCensored  = MemoryManager.malloc8(_n_time);
      countEvents    = MemoryManager.malloc8(_n_time);
      sumXEvents     = MemoryManager.malloc8d(_n_time);
      sumXBetaEvents = MemoryManager.malloc8d(_n_time);
      sumExpXBeta    = MemoryManager.malloc8d(_n_time);
      sumXExpXBeta   = MemoryManager.malloc8d(_n_time);
      sumXXExpXBeta  = MemoryManager.malloc8d(_n_time);
      for (int i = 0; i < stop._len; i++)
      {
        long stop_i  = stop.at80(i);
        long event_i = events.at80(i);
        double x_i   = xs.at0(i);
        if (!Double.isNaN(x_i))
        {
          int index = (int) (stop_i - _min_time);
          double xbeta = x_i * _beta;
          if (event_i > 0)
          {
            countEvents[index]++;
            sumXEvents[index]     += x_i;
            sumXBetaEvents[index] += xbeta;
          }
          else
            countCensored[index]++;
          double expXBeta = Math.exp(xbeta);
          sumExpXBeta[index]   += expXBeta;
          sumXExpXBeta[index]  += x_i * expXBeta;
          sumXXExpXBeta[index] += x_i * x_i * expXBeta;
          n++;
        }
      }
    }
    @Override public void reduce(CoxphFitTask that)
    {
      for (int t = 0; t < _n_time; t++)
      {
        countCensored[t] += that.countCensored[t];
        countEvents[t]   += that.countEvents[t];
        sumXEvents[t]    += that.sumXEvents[t];
        sumExpXBeta[t]   += that.sumExpXBeta[t];
        sumXExpXBeta[t]  += that.sumXExpXBeta[t];
        sumXXExpXBeta[t] += that.sumXXExpXBeta[t];
      }
      n += that.n;
    }
  }

  /** Return the query link to this page */
  public static String link(Key k, String content)
  {
    RString rs = new RString("<a href='COXPH.query?data_key=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }
}
