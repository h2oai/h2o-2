package hex;

import java.util.Arrays;

import water.*;
import water.fvec.*;

/**
 * @author smcinerney
 *
 * Quantiles by exact multipass bucket-based method.
 * Error bound is +/- half of bin-size containing quantile.
 * Error can be made arbitrarily small by increasing number of passes and buckets.
 * Currently uses 2 passes which is adequate.
 *
 * Reference:
 * The 9 methods used in R stats::quantile() are defined in:
 * "Sample quantiles in statistical packages" - Hyndman & Fan - ASA 1996
 *
 * Future improvements:
 * 1) If vec.min(),.max(), are available (this is not always guaranteed), could bound initial range of quantiles to vec.min(),max()
 * 2) Speedup binarySearch by switching based on (sign,exponent,mantissa) bitfields of double
 */

public class Quantiles extends Iced {

  public final int NQ = 6; // no of quantiles

  public int[] qbin; // index of bin containing quantile
  public double[] qval; // actual quantile results
  public double[] qbot, qtop; // lower- and upper-bounds on each quantile, from bin


  public Quantiles(Vec vec, double[] quantiles) {
    this(vec, quantiles[0], quantiles[1], quantiles[2], quantiles[3], quantiles[4], quantiles[5], 0L,0L);
  }

  public Quantiles(Vec vec, double quantile_a, double quantile_b,
      double quantile_c, double quantile_d, double quantile_e, double quantile_f,
      long pass1time, long pass2time) {

    double[] quantile = new double[NQ];
    quantile[0] = quantile_a;
    quantile[1] = quantile_b;
    quantile[2] = quantile_c;
    quantile[3] = quantile_d;
    quantile[4] = quantile_e;
    quantile[5] = quantile_f;

    qbin = new int[NQ];
    qval = new double[NQ];
    qbot = new double[NQ];
    qtop = new double[NQ];

    // Future optimization if vec is available (this is not always guaranteed), could bound initial range of quantiles to vec.min(),max()
    assert vec != null;
    if (vec==null) {
      System.out.println("Vec" + vec + " unavailable");
    } else {
      if (Double.isNaN(vec.min())) {
        System.out.println("Vec" + vec + " exists but min,max unavailable)");
      } else {
        System.out.println("Vec" + vec + "\nhas min:" + vec.min() + ", max:" + vec.max());
      }
    }

    // Pass 1
    long start = System.currentTimeMillis();
    QuantilesTask qt1 = new QuantilesTask().doAll(vec);
    calculateResults(qt1, quantile);
    long pass1 = System.currentTimeMillis();
    pass1time = pass1 - start;

    // Pass 2
    start = System.currentTimeMillis();
    QuantilesTask qt2 = new QuantilesTask(qbot, qtop).doAll(vec);
    calculateResults(qt2, quantile);
    long pass2 = System.currentTimeMillis();
    pass2time = pass1 - start;
  }

  protected void calculateResults(QuantilesTask t, double[] quantile) {

    // Get count of non-NA values
    long _n = 0;
    for (int b=0; b<t._bins.length; b++)
      _n += t._counts[b];

    // NOTES:
    // 1) Currently we simply return the bin midpoint: (qbot[i]+qtop[i])/2.
    // 2) although if we just took qbot[i], we could guarantee exact integer results on integer input
    // 3a) See the 9 quantile methods in Hyndman & Fan
    // 3b) 'Correct' result is to take value at corresponding indices phi*n,
    //     although some some methods use (n-1) or other 'plotting position'
    // 4) Doing interpolation within a bin (or its neighbors) is slightly tricky.

    for (int i=0; i<NQ; i++) {
      qbin[i] = locateQuantileBinIdx(t._counts, t._bins, _n, quantile[i]);

      qbot[i] = t._bins[qbin[i]];
      qtop[i] = t._bins[qbin[i]+1];

      qval[i] = (qbot[i]+qtop[i])/2.;
    }
  }

  protected int locateQuantileBinIdx(long[] counts, double[] bins, long n, double quantile) {

    // Compute the index corresponding to quantile...

    double phi_times_n = quantile * n; // NOTE: some methods use (n-1) or other 'plotting position'
    long phi_times_n_ = (int) Math.round(phi_times_n);
      // TODO 1a) interpolate the two nearest values, rather than use nearest index
      // TODO 1b) that would introduce the very rare special-case where quantile straddles 2 or more bins

    // Now count through the bins, to locate the bin where that index occurs
    int b = 0;
    for (long i=0; i<phi_times_n_; ) {
      i += counts[b++];
    }
    return b-1; // index must be in previous bin
  }

  public static class QuantilesTask extends MRTask2<QuantilesTask> {

    // The parameters NQ*NBINS controls the amount of memory used in Pass2
    public final int NQ=6; // no of quantiles
    public final int NBINS=10000; // no of bins we will use for each quantile

    public long[] _counts;
    public double[] _bins; // don't use MemoryManager.malloc8d(_bins._len);

    public int _bfirst, _blast; // begin and end bins for binary-search

    /**
     *  Constructor QuantilesTask() is for Pass1:
     *   initialize bins to cover entire dynamic range of double
     *
     *  Constructor QuantilesTask(double[] qbot, double[] qtop)
     *   is for Pass2 and subsequent: refine bins based on result of previous pass
     *
     */
    // NOTE: future improvement: initialize pass1 bins to only cover vec.min() .. vec.max()
    //     for which we would need QuantilesTask(double xmin, double xmax)
    //     For now we assume xmin = Double.NEGATIVE_INFINITY, xmax = Double.POSITIVE_INFINITY

    QuantilesTask() {

      _bins = new double[131072]; // 1Mb. Could parameterize size. The values below only occupy 13534 entries
      Arrays.fill(_bins, Double.POSITIVE_INFINITY);

      _bfirst = 0;

      _bins[0] = Double.NEGATIVE_INFINITY;
      _bins[1] = -Double.MAX_VALUE; // -1.7e308

      // Coarse-grained buckets between -Double.MAX_VALUE < x < -Long.MAX_VALUE
      int b, e;
      double[] negativeDoubleVals = new double[]{-6., -4., -2.5, -1.5, -1.};
      for (b=2, e=+307; e>=+19; e--)
        for (double val: negativeDoubleVals)
          _bins[b++] = val * Math.pow(10.,e);

      // NOTE: important reason why we use foreach on lists of literals below, instead
      // of a simple for-loop; it's to guarantee an exact floating-point match on integer values,
      // and thus get early termination with binarySearchInexact().

      // Finer-grained bins for -Long.MAX_VALUE,~9.22e18 < x <= -1e-18
      double[] fineNegativeDoubleVals = new double[]{-9.9,-9.8,-9.7,-9.6,-9.5,-9.4,-9.3,-9.2,-9.1,-9.0,
        -8.9,-8.8,-8.7,-8.6,-8.5,-8.4,-8.3,-8.2,-8.1,-8., -7.9,-7.8,-7.7,-7.6,-7.5,-7.4,-7.3,-7.2,-7.1,-7.,
        -6.9,-6.8,-6.7,-6.6,-6.5,-6.4,-6.3,-6.2,-6.1,-6., -5.9,-5.8,-5.7,-5.6,-5.5,-5.4,-5.3,-5.2,-5.1,-5.,
        -4.9,-4.8,-4.7,-4.6,-4.5,-4.4,-4.3,-4.2,-4.1,-4., -3.9,-3.8,-3.7,-3.6,-3.5,-3.4,-3.3,-3.2,-3.1,-3.,
        -2.9,-2.8,-2.7,-2.6,-2.5,-2.4,-2.3,-2.2,-2.1,-2.,
        -1.95,-1.9,-1.85,-1.8,-1.75,-1.7,-1.65,-1.6,-1.55,-1.5,
        -1.45,-1.4,-1.35,-1.3,-1.25,-1.2,-1.15,-1.1,-1.05,-1.0 };
      for (e=+18 ; e>=-18 ; e--)
        for (double val: fineNegativeDoubleVals)
          _bins[b++] = val * Math.pow(10.,e);

      // Coarse bins for -1e-18 < x < -Double.MIN_VALUE
      for (e=-19; e>=-323; e--)
        for (double val: negativeDoubleVals)
          _bins[b++] = val * Math.pow(10.,e);

      // Zero
      _bins[b++] = 0.0;

      // Coarse bins for +Double.MIN_VALUE < x < +1e-18
      double[] positiveDoubleVals = new double[]{1., 1.5, 2.5, 4., 6.};
      for (e=-323; e<=-19; e++)
        for (double val: positiveDoubleVals)
          _bins[b++] = val * Math.pow(10.,e);

      // Fine bins for +1e-18 <= x <= +Long.MAX_VALUE,~9.22e18
      double[] finePositiveDoubleVals = new double[]{1.,1.05,1.1,1.15,1.2,1.25,1.3,1.35,1.4,1.45,
          1.5,1.55,1.6,1.65,1.7,1.75,1.8,1.85,1.9,1.95,
          2.0,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9, 3.0,3.1,3.2,3.3,3.4,3.5,3.6,3.7,3.8,3.9,
          4.0,4.1,4.2,4.3,4.4,4.5,4.6,4.7,4.8,4.9, 5.0,5.1,5.2,5.3,5.4,5.5,5.6,5.7,5.8,5.9,
          6.0,6.1,6.2,6.3,6.4,6.5,6.6,6.7,6.8,6.9, 7.0,7.1,7.2,7.3,7.4,7.5,7.6,7.7,7.8,7.9,
          8.0,8.1,8.2,8.3,8.4,8.5,8.6,8.7,8.8,8.9, 9.0,9.1,9.2,9.3,9.4,9.5,9.6,9.7,9.8,9.9 };
      for (e=-18 ; e<=+18 ; e++)
        for (double val: finePositiveDoubleVals)
        _bins[b++] = val * Math.pow(10.,e);

      // Coarse-grained buckets between +Long.MAX_VALUE < x < +Double.MAX_VALUE
      for (e=+19; e<=+307; e++)
        for (double val: positiveDoubleVals)
          _bins[b++] = val * Math.pow(10.,e);

      _bins[b++] = +Double.MAX_VALUE; // -1.7e308

      _blast = b; // store the index of the highest bin in current use

      // Pad any remaining elements in array with +INF
      Arrays.fill(_bins, b, _bins.length-1, Double.POSITIVE_INFINITY);

      _counts = new long[_bins.length]; // all initialized to zero
    }


    /**
     * Pass2 and subsequent: refine _bins based on result of previous pass:
     *  (qbot[q], qtop[q])
     *
     * For each of the NQ=6 quantiles, create NBINS=10000 buckets
     */
    QuantilesTask(double[] qbot, double[] qtop) {

      assert qbot.length>0;
      System.out.println("Pass2 bins:");
      System.out.println("Quantile A: " + qbot[0] + ".." + qtop[0]);
      System.out.println("Quantile B: " + qbot[1] + ".." + qtop[1]);
      System.out.println("Quantile C: " + qbot[2] + ".." + qtop[2]);
      System.out.println("Quantile D: " + qbot[3] + ".." + qtop[3]);
      System.out.println("Quantile E: " + qbot[4] + ".." + qtop[4]);
      System.out.println("Quantile F: " + qbot[5] + ".." + qtop[5]);

      _bins = new double[NQ*(NBINS+1)+2];
      Arrays.fill(_bins, Double.POSITIVE_INFINITY);

      _bfirst = 0;
      int b=0;
      _bins[b++] = Double.NEGATIVE_INFINITY;

      for (int q=0; q<NQ; q++) {

        if (qbot[q]==_bins[b-1]) b--; // handle overlapping bins by stepping back one bin

        double bval = qbot[q];
        double bstep = (qtop[q]-qbot[q]) / (double)NBINS;
        for (int i=0; i<NBINS; i++, bval+=bstep) {
          _bins[b++] = bval;
        }
        _bins[b++] = qtop[q]; // include exact (presumably near-integer) top-of-bin
      }

      _blast = b;
      _bins[b++] = Double.POSITIVE_INFINITY;

    }


    /**
     * Map: for each value, increment corresponding bin-count
     */
    @Override public void map(Chunk xs) {

      _counts = new long[_bins.length];

      double value;  // don't use MemoryManager.malloc8d(1)
      for (int i=0; i<xs._len; i++) {
        value = xs.at0(i);
        if (!Double.isNaN(value)) {
          int b = binarySearchInexact(_bins, _bfirst, _blast, value);
          _counts[b]++;
        }
      }

    }

    /**
     * Reduce: merge bin-counts
     */
    @Override public void reduce( QuantilesTask other ) {
      for (int b=_bfirst; b<_blast; b++)
        this._counts[b] += other._counts[b];
    }

  }

  /**
   *  Inexact comparison: guaranteed to return bin b for the interval
   *  [ _bins[b], bins[b+1] ) which contains val
   */
  private static int binarySearchInexact(double[] _bins, int bfirst, int blast, double val) {

    int bmin = bfirst;
    int bmax = blast;
    int bmid = (bmin+bmax)/2;
    int deltab = bmax-bmin;
    while((bmin < bmax) && deltab>1) {
      deltab = bmax - bmin;
      bmid = (bmin+bmax)/2;
      if (val == _bins[bmid]) { // Early-termination on exact match
        return bmid;
      } else if (val < _bins[bmid]) {
        if (bmin == bmax-1) return bmin;
        bmax = bmid;
      } else if (val > _bins[bmid]) {
        bmin = bmid;
      }
    }
    return bmin;
  }

  private static void PrintChunk(String msg, Chunk xs) {
    System.out.print("\n" + msg);
    for (int i=0; i<xs._len; i++) {
      System.out.print(xs.at0(i) + ",");
    }
    System.out.println();
  }

}

