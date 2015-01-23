package hex;

import Jama.Matrix;
import java.util.Arrays;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
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

  @API(help="Start Time Column", required=false, filter=CoxPHVecSelect.class, json=true)
  public Vec start_column = null;

  @API(help="Stop Time Column", required=true, filter=CoxPHVecSelect.class, json=true)
  public Vec stop_column;

  @API(help="Event Column", required=true, filter=CoxPHVecSelect.class, json=true)
  public Vec event_column;

  @API(help="X Columns", required=true, filter=CoxPHMultiVecSelect.class, json=true)
  public int[] x_columns;

  @API(help="Weights Column", required=false, filter=CoxPHVecSelect.class, json=true)
  public Vec weights_column = null;

  @API(help="Offset Columns", required=false, filter=CoxPHMultiVecSelect.class, json=true)
  public int[] offset_columns;

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
    @API(help = "unweighted mean vector for numeric offsets")
    double[] mean_offset;
    @API(help = "names of offsets")
    String[] offset_names;
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
    /*
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
      final int n_offsets = (parameters.offset_columns == null) ? 0 : parameters.offset_columns.length;
      final int n_time    = time.length;
      final int n_coef    = coef.length;
      final int n_cats    = data_info._cats;
      final int n_nums    = data_info._nums;
      final int n_data    = n_cats + n_nums;
      final int n_full    = n_coef + n_offsets;
      final int numStart  = data_info.numStart();
      boolean catsAllNA   = true;
      boolean catsHasNA   = false;
      boolean numsHasNA   = false;
      for (int j = 0; j < n_cats; ++j) {
        catsAllNA &= Double.isNaN(data[j]);
        catsHasNA |= Double.isNaN(data[j]);
      }
      for (int j = n_cats; j < n_data; ++j)
        numsHasNA |= Double.isNaN(data[j]);
      if (numsHasNA || (catsHasNA && !catsAllNA)) {
        for (int i = 1; i <= 2 * n_time; ++i)
          preds[i] = Float.NaN;
      } else {
        double[] full_data = MemoryManager.malloc8d(n_full);
        for (int j = 0; j < n_cats; ++j)
          if (Double.isNaN(data[j])) {
            final int kst = data_info._catOffsets[j];
            final int klen = data_info._catOffsets[j+1] - kst;
            System.arraycopy(x_mean_cat, kst, full_data, kst, klen);
          } else if (data[j] != 0)
            full_data[data_info._catOffsets[j] + (int) (data[j] - 1)] = 1;
        for (int j = 0; j < n_nums; ++j)
          full_data[numStart + j] = data[n_cats + j] - data_info._normSub[j];
        double logRisk = 0;
        for (int j = 0; j < n_coef; ++j)
          logRisk += full_data[j] * coef[j];
        for (int j = n_coef; j < full_data.length; ++j)
          logRisk += full_data[j];
        final double risk = Math.exp(logRisk);
        for (int t = 0; t < n_time; ++t)
          preds[t + 1] = (float) (risk * cumhaz_0[t]);
        for (int t = 0; t < n_time; ++t) {
          final double cumhaz_0_t = cumhaz_0[t];
          double var_cumhaz_2_t = 0;
          for (int j = 0; j < n_coef; ++j) {
            double sum = 0;
            for (int k = 0; k < n_coef; ++k)
              sum += var_coef[j][k] * (full_data[k] * cumhaz_0_t - var_cumhaz_2[t][k]);
            var_cumhaz_2_t += (full_data[j] * cumhaz_0_t - var_cumhaz_2[t][j]) * sum;
          }
          preds[t + 1 + n_time] = (float) (risk * Math.sqrt(var_cumhaz_1[t] + var_cumhaz_2_t));
        }
      }
      preds[0] = Float.NaN;
      return preds;
    }
    */

    @Override
    protected float[] score0(double[] data, float[] preds) {
      final int n_offsets = (parameters.offset_columns == null) ? 0 : parameters.offset_columns.length;
      final int n_cats    = data_info._cats;
      final int n_nums    = data_info._nums;
      final int n_data    = n_cats + n_nums;
      final int numStart  = data_info.numStart();
      final int n_non_offsets = n_nums - n_offsets;
      boolean catsAllNA   = true;
      boolean catsHasNA   = false;
      boolean numsHasNA   = false;
      for (int j = 0; j < n_cats; ++j) {
        catsAllNA &= Double.isNaN(data[j]);
        catsHasNA |= Double.isNaN(data[j]);
      }
      for (int j = n_cats; j < n_data; ++j)
        numsHasNA |= Double.isNaN(data[j]);
      if (numsHasNA || (catsHasNA && !catsAllNA)) {
        preds[0] = Float.NaN;
      } else {
        double logRisk = 0;
        for (int j = 0; j < n_cats; ++j) {
          final int k_start = data_info._catOffsets[j];
          final int k_end   = data_info._catOffsets[j + 1];
          if (Double.isNaN(data[j]))
            for (int k = k_start; k < k_end; ++k)
              logRisk += x_mean_cat[k] * coef[k];
          else if (data[j] != 0)
            logRisk += coef[k_start + (int) (data[j] - 1)];
        }
        for (int j = 0; j < n_non_offsets; ++j)
          logRisk += (data[n_cats + j] - data_info._normSub[j]) * coef[numStart + j];
        for (int j = n_non_offsets; j < n_nums; ++j)
          logRisk += (data[n_cats + j] - data_info._normSub[j]);
        preds[0] = (float) Math.exp(logRisk);
      }
      return preds;
    }

    protected void initStats(final Frame source, final DataInfo dinfo) {
      n = source.numRows();
      data_info = dinfo;
      final int n_offsets = (parameters.offset_columns == null) ? 0 : parameters.offset_columns.length;
      final int n_coef    = data_info.fullN() - n_offsets;
      final String[] coefNames = data_info.coefNames();
      coef_names   = new String[n_coef];
      System.arraycopy(coefNames, 0, coef_names, 0, n_coef);
      coef         = MemoryManager.malloc8d(n_coef);
      exp_coef     = MemoryManager.malloc8d(n_coef);
      exp_neg_coef = MemoryManager.malloc8d(n_coef);
      se_coef      = MemoryManager.malloc8d(n_coef);
      z_coef       = MemoryManager.malloc8d(n_coef);
      gradient     = MemoryManager.malloc8d(n_coef);
      hessian      = malloc2DArray(n_coef, n_coef);
      var_coef     = malloc2DArray(n_coef, n_coef);
      x_mean_cat   = MemoryManager.malloc8d(n_coef - (data_info._nums - n_offsets));
      x_mean_num   = MemoryManager.malloc8d(data_info._nums - n_offsets);
      mean_offset  = MemoryManager.malloc8d(n_offsets);
      offset_names = new String[n_offsets];
      System.arraycopy(coefNames, n_coef, offset_names, 0, n_offsets);

      final Vec start_column = source.vec(source.numCols() - 3);
      final Vec stop_column  = source.vec(source.numCols() - 2);
      min_time = parameters.start_column == null ? (long) stop_column.min():
                                                   (long) start_column.min() + 1;
      max_time = (long) stop_column.max();

      final int n_time = new CollectDomain(stop_column).doAll(stop_column).domain().length;
      time         = MemoryManager.malloc8(n_time);
      n_risk       = MemoryManager.malloc8d(n_time);
      n_event      = MemoryManager.malloc8d(n_time);
      n_censor     = MemoryManager.malloc8d(n_time);
      cumhaz_0     = MemoryManager.malloc8d(n_time);
      var_cumhaz_1 = MemoryManager.malloc8d(n_time);
      var_cumhaz_2 = malloc2DArray(n_time, n_coef);
    }

    protected void calcCounts(final CoxPHTask coxMR) {
      n_missing = n - coxMR.n;
      n         = coxMR.n;
      for (int j = 0; j < x_mean_cat.length; j++)
        x_mean_cat[j] = coxMR.sumWeightedCatX[j] / coxMR.sumWeights;
      for (int j = 0; j < x_mean_num.length; j++)
        x_mean_num[j] = coxMR._dinfo._normSub[j] + coxMR.sumWeightedNumX[j] / coxMR.sumWeights;
      System.arraycopy(coxMR._dinfo._normSub, x_mean_num.length, mean_offset, 0, mean_offset.length);
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
      if (parameters.start_column == null)
        for (int t = n_risk.length - 2; t >= 0; --t)
          n_risk[t] += n_risk[t + 1];
    }

    protected double calcLoglik(final CoxPHTask coxMR) {
      final int n_coef = coef.length;
      final int n_time = coxMR.sizeEvents.length;
      double newLoglik = 0;
      for (int j = 0; j < n_coef; ++j)
        gradient[j] = 0;
      for (int j = 0; j < n_coef; ++j)
        for (int k = 0; k < n_coef; ++k)
          hessian[j][k] = 0;

      switch (parameters.ties) {
        case efron:
          final double[]   newLoglik_t = MemoryManager.malloc8d(n_time);
          final double[][]  gradient_t = malloc2DArray(n_time, n_coef);
          final double[][][] hessian_t = malloc3DArray(n_time, n_coef, n_coef);
          ForkJoinTask[] fjts = new ForkJoinTask[n_time];
          for (int t = n_time - 1; t >= 0; --t) {
            final int _t = t;
            fjts[t] = new RecursiveAction() {
              @Override protected void compute() {
                final double sizeEvents_t = coxMR.sizeEvents[_t];
                if (sizeEvents_t > 0) {
                  final long   countEvents_t      = coxMR.countEvents[_t];
                  final double sumLogRiskEvents_t = coxMR.sumLogRiskEvents[_t];
                  final double sumRiskEvents_t    = coxMR.sumRiskEvents[_t];
                  final double rcumsumRisk_t      = coxMR.rcumsumRisk[_t];
                  final double avgSize            = sizeEvents_t / countEvents_t;
                  newLoglik_t[_t] = sumLogRiskEvents_t;
                  System.arraycopy(coxMR.sumXEvents[_t], 0, gradient_t[_t], 0, n_coef);
                  for (long e = 0; e < countEvents_t; ++e) {
                    final double frac = ((double) e) / ((double) countEvents_t);
                    final double term = rcumsumRisk_t - frac * sumRiskEvents_t;
                    newLoglik_t[_t] -= avgSize * Math.log(term);
                    for (int j = 0; j < n_coef; ++j) {
                      final double djTerm    = coxMR.rcumsumXRisk[_t][j] - frac * coxMR.sumXRiskEvents[_t][j];
                      final double djLogTerm = djTerm / term;
                      gradient_t[_t][j] -= avgSize * djLogTerm;
                      for (int k = 0; k < n_coef; ++k) {
                        final double dkTerm  = coxMR.rcumsumXRisk[_t][k] - frac * coxMR.sumXRiskEvents[_t][k];
                        final double djkTerm = coxMR.rcumsumXXRisk[_t][j][k] - frac * coxMR.sumXXRiskEvents[_t][j][k];
                        hessian_t[_t][j][k] -= avgSize * (djkTerm / term - (djLogTerm * (dkTerm / term)));
                      }
                    }
                  }
                }
              }
            };
          }
          ForkJoinTask.invokeAll(fjts);

          for (int t = 0; t < n_time; ++t)
            newLoglik += newLoglik_t[t];

          for (int t = 0; t < n_time; ++t)
            for (int j = 0; j < n_coef; ++j)
              gradient[j] += gradient_t[t][j];

          for (int t = 0; t < n_time; ++t)
            for (int j = 0; j < n_coef; ++j)
              for (int k = 0; k < n_coef; ++k)
                hessian[j][k] += hessian_t[t][j][k];
          break;
        case breslow:
          for (int t = n_time - 1; t >= 0; --t) {
            final double sizeEvents_t = coxMR.sizeEvents[t];
            if (sizeEvents_t > 0) {
              final double sumLogRiskEvents_t = coxMR.sumLogRiskEvents[t];
              final double rcumsumRisk_t      = coxMR.rcumsumRisk[t];
              newLoglik += sumLogRiskEvents_t;
              newLoglik -= sizeEvents_t * Math.log(rcumsumRisk_t);
              for (int j = 0; j < n_coef; ++j) {
                final double dlogTerm = coxMR.rcumsumXRisk[t][j] / rcumsumRisk_t;
                gradient[j] += coxMR.sumXEvents[t][j];
                gradient[j] -= sizeEvents_t * dlogTerm;
                 for (int k = 0; k < n_coef; ++k)
                  hessian[j][k] -= sizeEvents_t *
                    (((coxMR.rcumsumXXRisk[t][j][k] / rcumsumRisk_t) -
                      (dlogTerm * (coxMR.rcumsumXRisk[t][k] / rcumsumRisk_t))));
              }
            }
          }
          break;
        default:
          throw new IllegalArgumentException("ties method must be either efron or breslow");
      }
      return newLoglik;
    }

    protected void calcModelStats(final double[] newCoef, final double newLoglik) {
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

    protected void calcCumhaz_0(final CoxPHTask coxMR) {
      final int n_coef = coef.length;
      int nz = 0;
      switch (parameters.ties) {
        case efron:
          for (int t = 0; t < coxMR.sizeEvents.length; ++t) {
            final double sizeEvents_t   = coxMR.sizeEvents[t];
            final double sizeCensored_t = coxMR.sizeCensored[t];
            if (sizeEvents_t > 0 || sizeCensored_t > 0) {
              final long   countEvents_t   = coxMR.countEvents[t];
              final double sumRiskEvents_t = coxMR.sumRiskEvents[t];
              final double rcumsumRisk_t   = coxMR.rcumsumRisk[t];
              final double avgSize = sizeEvents_t / countEvents_t;
              cumhaz_0[nz]     = 0;
              var_cumhaz_1[nz] = 0;
              for (int j = 0; j < n_coef; ++j)
                var_cumhaz_2[nz][j] = 0;
              for (long e = 0; e < countEvents_t; ++e) {
                final double frac   = ((double) e) / ((double) countEvents_t);
                final double haz    = 1 / (rcumsumRisk_t - frac * sumRiskEvents_t);
                final double haz_sq = haz * haz;
                cumhaz_0[nz]     += avgSize * haz;
                var_cumhaz_1[nz] += avgSize * haz_sq;
                for (int j = 0; j < n_coef; ++j)
                  var_cumhaz_2[nz][j] +=
                    avgSize * ((coxMR.rcumsumXRisk[t][j] - frac * coxMR.sumXRiskEvents[t][j]) * haz_sq);
              }
              nz++;
            }
          }
          break;
        case breslow:
          for (int t = 0; t < coxMR.sizeEvents.length; ++t) {
            final double sizeEvents_t   = coxMR.sizeEvents[t];
            final double sizeCensored_t = coxMR.sizeCensored[t];
            if (sizeEvents_t > 0 || sizeCensored_t > 0) {
              final double rcumsumRisk_t = coxMR.rcumsumRisk[t];
              final double cumhaz_0_nz   = sizeEvents_t / rcumsumRisk_t;
              cumhaz_0[nz]     = cumhaz_0_nz;
              var_cumhaz_1[nz] = sizeEvents_t / (rcumsumRisk_t * rcumsumRisk_t);
              for (int j = 0; j < n_coef; ++j)
                var_cumhaz_2[nz][j] = (coxMR.rcumsumXRisk[t][j] / rcumsumRisk_t) * cumhaz_0_nz;
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
          var_cumhaz_2[t][j] = var_cumhaz_2[t - 1][j] + var_cumhaz_2[t][j];
      }
    }

    public Frame makeSurvfit(final Key key, double x_new) { // FIXME
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
        final double gamma = x_centered * cumhaz_0[t] - var_cumhaz_2[t][j];
        se_cumhaz.set(t, risk * Math.sqrt(var_cumhaz_1[t] + (gamma * var_coef[j][j] * gamma)));
      }
      final Frame fr = new Frame(key, new String[] {"time", "cumhaz", "se_cumhaz", "surv"}, vecs);
      final Futures fs = new Futures();
      DKV.put(key, fr, fs);
      fs.blockForPending();
      return fr;
    }

    public void generateHTML(final String title, final StringBuilder sb) {
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

    if ((start_column != null) && !start_column.isInt())
      throw new IllegalArgumentException("start time must be null or of type integer");

    if (!stop_column.isInt())
      throw new IllegalArgumentException("stop time must be of type integer");

    if (!event_column.isInt() && !event_column.isEnum())
      throw new IllegalArgumentException("event must be of type integer or factor");

    if ((event_column.isInt()  && (event_column.min() == event_column.max())) ||
        (event_column.isEnum() && (event_column.cardinality() < 2)))
      throw new IllegalArgumentException("event column contains less than two distinct values");

    if (Double.isNaN(lre_min) || lre_min <= 0)
      throw new IllegalArgumentException("lre_min must be a positive number");

    if (iter_max < 1)
      throw new IllegalArgumentException("iter_max must be a positive integer");

    final long min_time = (start_column == null) ? (long) stop_column.min() : (long) start_column.min() + 1;
    final int n_time = (int) (stop_column.max() - min_time + 1);
    if (n_time < 1)
      throw new IllegalArgumentException("start times must be strictly less than stop times");
    if (n_time > MAX_TIME_BINS)
      throw new IllegalArgumentException("number of distinct stop times is " + n_time +
                                         "; maximum number allowed is " + MAX_TIME_BINS);

    source = getSubframe();
    int n_resp = 2;
    if (weights_column != null)
      n_resp++;
    if (start_column != null)
      n_resp++;
    final DataInfo dinfo = new DataInfo(source, n_resp, false, false, DataInfo.TransformType.DEMEAN);
    model = new CoxPHModel(this, dest(), source._key, source, null);
    model.initStats(source, dinfo);
  }

  @Override
  protected void execImpl() {
    final DataInfo dinfo   = model.data_info;
    final int n_offsets    = (model.parameters.offset_columns == null) ? 0 : model.parameters.offset_columns.length;
    final int n_coef       = dinfo.fullN() - n_offsets;
    final double[] step    = MemoryManager.malloc8d(n_coef);
    final double[] oldCoef = MemoryManager.malloc8d(n_coef);
    final double[] newCoef = MemoryManager.malloc8d(n_coef);
    Arrays.fill(step,    Double.NaN);
    Arrays.fill(oldCoef, Double.NaN);
    for (int j = 0; j < n_coef; ++j)
      newCoef[j] = init;
    double oldLoglik = - Double.MAX_VALUE;
    final int n_time = (int) (model.max_time - model.min_time + 1);
    final boolean has_start_column   = (model.parameters.start_column != null);
    final boolean has_weights_column = (model.parameters.weights_column != null);
    for (int i = 0; i <= iter_max; ++i) {
      model.iter = i;

      final CoxPHTask coxMR = new CoxPHTask(self(), dinfo, newCoef, model.min_time, n_time, n_offsets,
                                            has_start_column, has_weights_column).doAll(dinfo._adaptedFrame);

      final double newLoglik = model.calcLoglik(coxMR);
      if (newLoglik > oldLoglik) {
        if (i == 0)
          model.calcCounts(coxMR);

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
    final boolean use_start_column   = (start_column != null);
    final boolean use_weights_column = (weights_column != null);
    final int x_ncol = x_columns.length;
    final int offset_ncol = offset_columns == null ? 0 : offset_columns.length;
    int ncol = x_ncol + offset_ncol + 2;
    if (use_weights_column)
      ncol++;
    if (use_start_column)
      ncol++;
    final String[] names = new String[ncol];
    for (int j = 0; j < x_ncol; ++j)
      names[j] = source.names()[x_columns[j]];
    for (int j = 0; j < offset_ncol; ++j)
      names[x_ncol + j] = source.names()[offset_columns[j]];
    if (use_weights_column)
      names[x_ncol + offset_ncol] = source.names()[source.find(weights_column)];
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
    private final int      _n_offsets;
    private final boolean  _has_start_column;
    private final boolean  _has_weights_column;

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
              final int n_offsets, final boolean has_start_column, final boolean has_weights_column) {
      super(jobKey, dinfo);
      _beta               = beta;
      _n_time             = n_time;
      _min_time           = min_time;
      _n_offsets          = n_offsets;
      _has_start_column   = has_start_column;
      _has_weights_column = has_weights_column;
    }

    @Override
    protected void chunkInit(){
      final int n_coef = _beta.length;
      sumWeightedCatX  = MemoryManager.malloc8d(n_coef - (_dinfo._nums - _n_offsets));
      sumWeightedNumX  = MemoryManager.malloc8d(_dinfo._nums);
      sizeRiskSet      = MemoryManager.malloc8d(_n_time);
      sizeCensored     = MemoryManager.malloc8d(_n_time);
      sizeEvents       = MemoryManager.malloc8d(_n_time);
      countEvents      = MemoryManager.malloc8(_n_time);
      sumRiskEvents    = MemoryManager.malloc8d(_n_time);
      sumLogRiskEvents = MemoryManager.malloc8d(_n_time);
      rcumsumRisk      = MemoryManager.malloc8d(_n_time);
      sumXEvents       = malloc2DArray(_n_time, n_coef);
      sumXRiskEvents   = malloc2DArray(_n_time, n_coef);
      rcumsumXRisk     = malloc2DArray(_n_time, n_coef);
      sumXXRiskEvents  = malloc3DArray(_n_time, n_coef, n_coef);
      rcumsumXXRisk    = malloc3DArray(_n_time, n_coef, n_coef);
    }

    @Override
    protected void processRow(long gid, double [] nums, int ncats, int [] cats, double [] response) {
      n++;
      final double weight = _has_weights_column ? response[0] : 1.0;
      if (weight <= 0)
        throw new IllegalArgumentException("weights must be positive values");
      final long event = (long) response[response.length - 1];
      final int t1 = _has_start_column ? (int) (((long) response[response.length - 3] + 1) - _min_time) : -1;
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
      for (int j = 0; j < nums.length - _n_offsets; ++j)
        logRisk += nums[j] * _beta[numStart + j];
      for (int j = nums.length - _n_offsets; j < nums.length; ++j)
        logRisk += nums[j];
      final double risk = weight * Math.exp(logRisk);
      logRisk *= weight;
      if (event > 0) {
        countEvents[t2]++;
        sizeEvents[t2]       += weight;
        sumLogRiskEvents[t2] += logRisk;
        sumRiskEvents[t2]    += risk;
      } else
        sizeCensored[t2] += weight;
      if (_has_start_column) {
        for (int t = t1; t <= t2; ++t)
          sizeRiskSet[t] += weight;
        for (int t = t1; t <= t2; ++t)
          rcumsumRisk[t] += risk;
      } else {
        sizeRiskSet[t2]  += weight;
        rcumsumRisk[t2]  += risk;
      }

      final int ntotal = ncats + (nums.length - _n_offsets);
      final int numStartIter = numStart - ncats;
      for (int jit = 0; jit < ntotal; ++jit) {
        final boolean jIsCat = jit < ncats;
        final int j          = jIsCat ? cats[jit] : numStartIter + jit;
        final double x1      = jIsCat ? 1.0 : nums[jit - ncats];
        final double xRisk   = x1 * risk;
        if (event > 0) {
          sumXEvents[t2][j]     += weight * x1;
          sumXRiskEvents[t2][j] += xRisk;
        }
        if (_has_start_column) {
          for (int t = t1; t <= t2; ++t)
            rcumsumXRisk[t][j]  += xRisk;
        } else {
          rcumsumXRisk[t2][j]   += xRisk;
        }
        for (int kit = 0; kit < ntotal; ++kit) {
          final boolean kIsCat = kit < ncats;
          final int k          = kIsCat ? cats[kit] : numStartIter + kit;
          final double x2      = kIsCat ? 1.0 : nums[kit - ncats];
          final double xxRisk  = x2 * xRisk;
          if (event > 0)
            sumXXRiskEvents[t2][j][k] += xxRisk;
          if (_has_start_column) {
            for (int t = t1; t <= t2; ++t)
              rcumsumXXRisk[t][j][k]  += xxRisk;
          } else {
            rcumsumXXRisk[t2][j][k]   += xxRisk;
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
      if (!_has_start_column) {
        for (int t = rcumsumRisk.length - 2; t >= 0; --t)
          rcumsumRisk[t] += rcumsumRisk[t + 1];

        for (int t = rcumsumXRisk.length - 2; t >= 0; --t)
          for (int j = 0; j < rcumsumXRisk[t].length; ++j)
            rcumsumXRisk[t][j] += rcumsumXRisk[t + 1][j];

        for (int t = rcumsumXXRisk.length - 2; t >= 0; --t)
          for (int j = 0; j < rcumsumXXRisk[t].length; ++j)
            for (int k = 0; k < rcumsumXXRisk[t][j].length; ++k)
              rcumsumXXRisk[t][j][k] += rcumsumXXRisk[t + 1][j][k];
      }
    }
  }
}
