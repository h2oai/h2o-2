package hex.gapstat;

import hex.KMeans2;
import water.*;
import water.Job;
import water.api.DocGen;
import water.fvec.*;
import water.util.Log;
import java.util.Random;
import static water.util.Utils.getDeterRNG;


/**
 * Gap Statistic
 * This is an algorithm for estimating the optimal number of clusters in p-dimensional data.
 * @author spencer_aiello
 *
 */
public class GapStatistic extends Job.ColumnsJob {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "gap statistic";

  @API(help = "Number of Monte Carlo Bootstrap Replicates", filter = Default.class, lmin = 1, lmax = 100000, json = true)
  public int b_max = 10;

  @API(help = "The number maximum number of clusters to consider, must be at least 2.", filter = Default.class, json = true, lmin = 2, lmax = 10000)
  public int k_max = 10;

  @API(help = "Fraction of data size to replicate in each MC simulation.", filter = Default.class, json = true, dmin = 0, dmax = 1)
  public double bootstrap_fraction = .1;

  @API(help = "Max iteratiors per clustering.")
  public int max_iter = 50;

  @API(help = "A random seed.", filter = Default.class, json = true)
  public long seed = new Random().nextLong();

  @Override protected void execImpl() {
    logStart();
    GapStatisticModel model = initModel();
    buildModel(model);
    cleanup();
    remove();
  }

  private GapStatisticModel initModel() {
    try {
      source.read_lock(self());
      int ks = k_max;
      double[] wks = new double[ks];
      double[] wkbs = new double[ks];
      double[] sk = new double[ks];
      return new GapStatisticModel(destination_key, source._key, source, k_max, wks, wkbs, sk, k_max, b_max, 1, 0);
    }
    finally {
      source.unlock(self());
    }
  }

  private void buildModel(GapStatisticModel gs_model) {
    try {
      source.read_lock(self());
      if (gs_model == null) gs_model = UKV.get(dest());
      gs_model.delete_and_lock(self());
      for (int k = 1; k <= k_max; ++k) {
        if (this.isCancelledOrCrashed()) {
          throw new JobCancelledException();
        }
        KMeans2 km = new KMeans2();
        km.source = source;
        km.cols = cols;
        km.max_iter = max_iter;
        km.k = k;
        km.initialization = KMeans2.Initialization.Furthest;
        km.invoke();
        KMeans2.KMeans2Model res = UKV.get(km.dest());
        Futures fs = new Futures();
        DKV.remove(Key.make(km.dest()+"_clusters"), fs);
        gs_model.wks[k - 1] = Math.log(res.mse());

        double[] bwkbs = new double[b_max];
        for (int b = 0; b < b_max; ++b) {
          if (this.isCancelledOrCrashed()) {
            throw new JobCancelledException();
          }
          Frame bs = new MRTask2() {
            @Override public void map(Chunk[] chks, NewChunk[] nchks) {
              final Random rng = getDeterRNG(seed + chks[0].cidx());

              for (int row = 0; row < Math.floor(bootstrap_fraction * chks[0]._len); ++row) {
                for (int col = 0; col < chks.length; ++ col) {
                  if (source.vecs()[col].isConst()) {
                    nchks[col].addNum(source.vecs()[col].max());
                    continue;
                  }
                  if (source.vecs()[col].isEnum()) {
                    nchks[col].addEnum((int)chks[col].at8(row));
                    continue;
                  }
                  double d = rng.nextDouble() * source.vecs()[col].max() + source.vecs()[col].min();
                  nchks[col].addNum(d);
                }
              }
            }
          }.doAll(source.numCols(), source).outputFrame(source.names(), source.domains());
          KMeans2 km_bs = new KMeans2();
          km_bs.source = bs;
          km_bs.cols = cols;
          km_bs.max_iter = max_iter;
          km_bs.k = k;
          km_bs.initialization = KMeans2.Initialization.Furthest;
          km_bs.invoke();
          KMeans2.KMeans2Model res_bs = UKV.get(km_bs.dest());
          fs = new Futures();
          DKV.remove(Key.make(km_bs.dest()+"_clusters"), fs);
          bwkbs[b] = Math.log(res_bs.mse());
          gs_model.b = b+1;
          gs_model.update(self());
        }
        double sum_bwkbs = 0.;
        for (double d: bwkbs) sum_bwkbs += d;
        gs_model.wkbs[k - 1] = sum_bwkbs / b_max;
        double sk_2 = 0.;
        for (double d: bwkbs) {
          sk_2 += (d - gs_model.wkbs[k - 1]) * (d - gs_model.wkbs[k - 1]) * 1. / (double) b_max;
        }
        gs_model.sk[k - 1] = Math.sqrt(sk_2) * Math.sqrt(1 + 1. / (double) b_max);
        gs_model.k = k;
        for(int i = 0; i < gs_model.wks.length; ++i) gs_model.gap_stats[i] = gs_model.wkbs[i] - gs_model.wks[i];
        gs_model.update(self());
      }
      gs_model.compute_k_best();
    }
    catch(JobCancelledException ex) {
      Log.info("Gap Statistic Computation was cancelled.");
    }
    catch(Exception ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
    }
    finally {
      if (gs_model != null) {
        gs_model = UKV.get(dest());
        gs_model.unlock(self());
      }
      source.unlock(self());
      emptyLTrash();
    }
  }

  @Override protected Response redirect() {
    return GapStatisticProgressPage.redirect(this, self(), dest());
  }
}