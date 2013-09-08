package hex;

import java.util.Arrays;

import water.*;
import water.fvec.*;

/**
 * @author smcinerney
 *
 * Quantiles by exact multipass bucket-based method, similar to Summary.
 *
 * Reference:
 * The 9 methods used in R stats::quantile() are defined in:
 * "Sample quantiles in statistical packages" - Hyndman & Fan - ASA 1996
 *
 * TODO: could preinitialize the bins to cover col (min,max)
 */

public class Quantiles extends Iced {

  public final int NQ=6; // no of quantiles
  public int[] qbin; // index of bin containing quantile
  public double[] qval; // quantile results
  public double[] qbot, qtop; // lower- and upper-bounds on each quantile, from bin
  double xmin = Double.NEGATIVE_INFINITY;
  double xmax = Double.POSITIVE_INFINITY;

  // Could switch directly on (sign,exponent) components of double:
  //  Sign bit: 1 bit.
  //  Binary exponent, biased by 1023: 11 bits.
  //  long exponent = ((bits & 0x7ff0000000000000L) >> 52) - 1023; // check this is correct
  //  Mantissa: 53 bits.
  //  long mantissa = (bits & 0x000fffffffffffffL) | 0x0010000000000000L;

  public Quantiles(Vec vec, double quantile_a, double quantile_b,
      double quantile_c, double quantile_d, double quantile_e, double quantile_f) {

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

    // If vec is available, look up its min, max
    assert vec != null;
    if (vec==null) {
      System.out.println("Vec" + vec + "unavailable");
    } else {
      if (Double.isNaN(vec.min())) {
        System.out.println("Vec" + vec + "exists but min,max unavailable)");
      } else {
        System.out.println("Vec" + vec + "has min:" + vec.min() + ", max:" + vec.max());
      }
    }


    // Pass 1
    long start = System.currentTimeMillis();
    QuantilesTask qt1 = new QuantilesTask().doAll(vec);
    calculateResults(qt1, quantile);
    long pass1 = System.currentTimeMillis();
    //pass1time = pass1 - start;
    System.out.println("pass1 quantiles: " + Arrays.toString(qval));


    // Pass 2
    start = System.currentTimeMillis();
    QuantilesTask qt2 = new QuantilesTask(qbot, qtop).doAll(vec);
    calculateResults(qt2, quantile);
    long pass2 = System.currentTimeMillis();
    //pass2time = pass1 - start;
  }

  protected void calculateResults(QuantilesTask t, double[] quantile) {

    // Get count of non-NA values
    long _n = 0;
    for (int b=0; b<t._bins.length; b++) {
      _n += t._counts[b];
    }
    System.out.println("Total bincount _n=" + _n);

    qbin[0] = locateQuantile(t._counts, t._bins, _n, quantile[0], 0);
    qbin[1] = locateQuantile(t._counts, t._bins, _n, quantile[1], 1);
    qbin[2] = locateQuantile(t._counts, t._bins, _n, quantile[2], 2);
    qbin[3] = locateQuantile(t._counts, t._bins, _n, quantile[3], 3);
    qbin[4] = locateQuantile(t._counts, t._bins, _n, quantile[4], 4);
    qbin[5] = locateQuantile(t._counts, t._bins, _n, quantile[5], 5);

    // TODO: 'correct' result is to take(/estimate) sample at corresponding indices phi*N
    // Optionally use one of the 9 averaging schemes as per R/ Hyndman & Fan
    //double[] res = new double[quantiles.length];
    for (int i=0; i<NQ; i++) {
      qbot[i] = t._bins[qbin[i]];
      qtop[i] = t._bins[qbin[i]+1];
      qval[i] = (qbot[i]+qtop[i])/2.; // NOTE: if we just took qbot[i], we could guarantee integer results
    }
  }

  protected int locateQuantile(long[] counts, double[] bins, long n, double quantile, /*UNNEEDED*/ int qi) {
    // bin-index (b) of this quantile

    // TODO rare special-case where quantile straddles 2 or more bins
    double phi_times_n = quantile * n; // (n-1)?
    long phi_times_n_ = (int) Math.round(phi_times_n); // TODO slightly crude round-to-nearest!

    // Locate the bin where this quantile occurs
    int b = 0;
    long tot = 0;
    while (tot<phi_times_n_) {
      tot += counts[b++];
    }
    b--; // step back one bin

    // Note: can't meaningfully do interpolation within a bin (or its neighbors), so we just return the midpoint
    //return (bins[b] + bins[b+1]) / 2.;

    return b;
  }

  public static class QuantilesTask extends MRTask2<QuantilesTask> {

    public final int NQ=6; // no of quantiles
    public final int NBINS=10000; // no of bins we will use for each quantile

    public long[] _counts;
    public double[] _bins; // don't use MemoryManager.malloc8d(_bins._len);
    public int _bfirst, _blast;

    /*QuantilesTask(double xmin, double xmax) {
      this(...);
    }*/

    // pass1: initialize bins to cover entire range
    QuantilesTask() {
      _bins = new double[131072]; // TBD allow tweaking size. The contents below only occupy 13534 entries
      //System.out.println("Allocated _bins with length: " + _bins.length);
      Arrays.fill(_bins, Double.POSITIVE_INFINITY);

      _bfirst = 0;

      _bins[0] = Double.NEGATIVE_INFINITY;
      _bins[1] = -Double.MAX_VALUE; // -1.7e308

      //
      int b, e;
      for (b=2, e=+307; e>=+19; e--, b+=5) {
        _bins[b]   = -6.  * Math.pow(10.,e);
        _bins[b+1] = -4.  * Math.pow(10.,e);
        _bins[b+2] = -2.5 * Math.pow(10.,e);
        _bins[b+3] = -1.5 * Math.pow(10.,e);
        _bins[b+4] =     - Math.pow(10.,e);
      }
      //System.out.println("\nBin values: ");
      //System.out.print("-Long.MAX_VALUE at b=" + b);
      // more fine bins below (double)Long.MAX_VALUE, ~9.22e18
      for (e=+18 ; e>=-18 ; e--, b+=100) {
        _bins[b]    = -9.9 * Math.pow(10.,e);
        _bins[b+1]  = -9.8 * Math.pow(10.,e);
        _bins[b+2]  = -9.7 * Math.pow(10.,e);
        _bins[b+3]  = -9.6 * Math.pow(10.,e);
        _bins[b+4]  = -9.5 * Math.pow(10.,e);
        _bins[b+5]  = -9.4 * Math.pow(10.,e);
        _bins[b+6]  = -9.3 * Math.pow(10.,e);
        _bins[b+7]  = -9.2 * Math.pow(10.,e);
        _bins[b+8]  = -9.1 * Math.pow(10.,e);
        _bins[b+9]  = -9.  * Math.pow(10.,e);
        _bins[b+10] = -8.9 * Math.pow(10.,e);
        _bins[b+11] = -8.8 * Math.pow(10.,e);
        _bins[b+12] = -8.7 * Math.pow(10.,e);
        _bins[b+13] = -8.6 * Math.pow(10.,e);
        _bins[b+14] = -8.5 * Math.pow(10.,e);
        _bins[b+15] = -8.4 * Math.pow(10.,e);
        _bins[b+16] = -8.3 * Math.pow(10.,e);
        _bins[b+17] = -8.2 * Math.pow(10.,e);
        _bins[b+18] = -8.1 * Math.pow(10.,e);
        _bins[b+19] = -8. * Math.pow(10.,e);
        _bins[b+20] = -7.9 * Math.pow(10.,e);
        _bins[b+21] = -7.8 * Math.pow(10.,e);
        _bins[b+22] = -7.7 * Math.pow(10.,e);
        _bins[b+23] = -7.6 * Math.pow(10.,e);
        _bins[b+24] = -7.5 * Math.pow(10.,e);
        _bins[b+25] = -7.4 * Math.pow(10.,e);
        _bins[b+26] = -7.3 * Math.pow(10.,e);
        _bins[b+27] = -7.2 * Math.pow(10.,e);
        _bins[b+28] = -7.1 * Math.pow(10.,e);
        _bins[b+29] = -7.  * Math.pow(10.,e);
        _bins[b+30] = -6.9 * Math.pow(10.,e);
        _bins[b+31] = -6.8 * Math.pow(10.,e);
        _bins[b+32] = -6.7 * Math.pow(10.,e);
        _bins[b+33] = -6.6 * Math.pow(10.,e);
        _bins[b+34] = -6.5 * Math.pow(10.,e);
        _bins[b+35] = -6.4 * Math.pow(10.,e);
        _bins[b+36] = -6.3 * Math.pow(10.,e);
        _bins[b+37] = -6.2 * Math.pow(10.,e);
        _bins[b+38] = -6.1 * Math.pow(10.,e);
        _bins[b+39] = -6.  * Math.pow(10.,e);
        _bins[b+40] = -5.9 * Math.pow(10.,e);
        _bins[b+41] = -5.8 * Math.pow(10.,e);
        _bins[b+42] = -5.7 * Math.pow(10.,e);
        _bins[b+43] = -5.6 * Math.pow(10.,e);
        _bins[b+44] = -5.5 * Math.pow(10.,e);
        _bins[b+45] = -5.4 * Math.pow(10.,e);
        _bins[b+46] = -5.3 * Math.pow(10.,e);
        _bins[b+47] = -5.2 * Math.pow(10.,e);
        _bins[b+48] = -5.1 * Math.pow(10.,e);
        _bins[b+49] = -5.  * Math.pow(10.,e);
        _bins[b+50] = -4.9 * Math.pow(10.,e);
        _bins[b+51] = -4.8 * Math.pow(10.,e);
        _bins[b+52] = -4.7 * Math.pow(10.,e);
        _bins[b+53] = -4.6 * Math.pow(10.,e);
        _bins[b+54] = -4.5 * Math.pow(10.,e);
        _bins[b+55] = -4.4 * Math.pow(10.,e);
        _bins[b+56] = -4.3 * Math.pow(10.,e);
        _bins[b+57] = -4.2 * Math.pow(10.,e);
        _bins[b+58] = -4.1 * Math.pow(10.,e);
        _bins[b+59] = -4.  * Math.pow(10.,e);
        _bins[b+60] = -3.9 * Math.pow(10.,e);
        _bins[b+61] = -3.8 * Math.pow(10.,e);
        _bins[b+62] = -3.7 * Math.pow(10.,e);
        _bins[b+63] = -3.6 * Math.pow(10.,e);
        _bins[b+64] = -3.5 * Math.pow(10.,e);
        _bins[b+65] = -3.4 * Math.pow(10.,e);
        _bins[b+66] = -3.3 * Math.pow(10.,e);
        _bins[b+67] = -3.2 * Math.pow(10.,e);
        _bins[b+68] = -3.1 * Math.pow(10.,e);
        _bins[b+69] = -3.  * Math.pow(10.,e);
        _bins[b+70] = -2.9 * Math.pow(10.,e);
        _bins[b+71] = -2.8 * Math.pow(10.,e);
        _bins[b+72] = -2.7 * Math.pow(10.,e);
        _bins[b+73] = -2.6 * Math.pow(10.,e);
        _bins[b+74] = -2.5 * Math.pow(10.,e);
        _bins[b+75] = -2.4 * Math.pow(10.,e);
        _bins[b+76] = -2.3 * Math.pow(10.,e);
        _bins[b+77] = -2.2 * Math.pow(10.,e);
        _bins[b+78] = -2.1 * Math.pow(10.,e);
        _bins[b+79] = -2.  * Math.pow(10.,e);
        _bins[b+80] = -1.95 * Math.pow(10.,e);
        _bins[b+81] = -1.9  * Math.pow(10.,e);
        _bins[b+82] = -1.85 * Math.pow(10.,e);
        _bins[b+83] = -1.8  * Math.pow(10.,e);
        _bins[b+84] = -1.75 * Math.pow(10.,e);
        _bins[b+85] = -1.7  * Math.pow(10.,e);
        _bins[b+86] = -1.65 * Math.pow(10.,e);
        _bins[b+87] = -1.6  * Math.pow(10.,e);
        _bins[b+88] = -1.55 * Math.pow(10.,e);
        _bins[b+89] = -1.5  * Math.pow(10.,e);
        _bins[b+90] = -1.45 * Math.pow(10.,e);
        _bins[b+91] = -1.4  * Math.pow(10.,e);
        _bins[b+92] = -1.35 * Math.pow(10.,e);
        _bins[b+93] = -1.3  * Math.pow(10.,e);
        _bins[b+94] = -1.25 * Math.pow(10.,e);
        _bins[b+95] = -1.2  * Math.pow(10.,e);
        _bins[b+96] = -1.15 * Math.pow(10.,e);
        _bins[b+97] = -1.1  * Math.pow(10.,e);
        _bins[b+98] = -1.05 * Math.pow(10.,e);
        _bins[b+99] = -1.   * Math.pow(10.,e);
      }
      //System.out.print(" ... small -ve Doubles at b=" + b);

      for (e=+19; e>=-323; e--, b+=5) {
        _bins[b]   = -6.  * Math.pow(10.,e);
        _bins[b+1] = -4.  * Math.pow(10.,e);
        _bins[b+2] = -2.5 * Math.pow(10.,e);
        _bins[b+3] = -1.5 * Math.pow(10.,e);
        _bins[b+4] = -      Math.pow(10.,e);
      }
      //System.out.print(" ... ~ -Double.MIN_VALUE at b=" + b);

      _bins[b++] = 0.0;
      //System.out.print(" ...ZERO at b="+b);

      for (e=-323; e<=-19; e++, b+=5) {
        _bins[b]   = +      Math.pow(10.,e);
        _bins[b+1] = +1.5 * Math.pow(10.,e);
        _bins[b+2] = +2.5 * Math.pow(10.,e);
        _bins[b+3] = +4.  * Math.pow(10.,e);
        _bins[b+4] = +6.  * Math.pow(10.,e);
      }
      //System.out.print(" ... +ve small Doubles at b=" + b);

      for (e=-18 ; e<=+18 ; e++, b+=100) { // fine-grained buckets over the range of Long
        _bins[b]    = +1.   * Math.pow(10.,e);
        _bins[b+1]  = +1.05 * Math.pow(10.,e);
        _bins[b+2]  = +1.1  * Math.pow(10.,e);
        _bins[b+3]  = +1.15 * Math.pow(10.,e);
        _bins[b+4]  = +1.2  * Math.pow(10.,e);
        _bins[b+5]  = +1.25 * Math.pow(10.,e);
        _bins[b+6]  = +1.3  * Math.pow(10.,e);
        _bins[b+7]  = +1.35 * Math.pow(10.,e);
        _bins[b+8]  = +1.4  * Math.pow(10.,e);
        _bins[b+9]  = +1.45 * Math.pow(10.,e);
        _bins[b+10] = +1.5  * Math.pow(10.,e);
        _bins[b+11] = +1.55 * Math.pow(10.,e);
        _bins[b+12] = +1.6  * Math.pow(10.,e);
        _bins[b+13] = +1.65 * Math.pow(10.,e);
        _bins[b+14] = +1.7  * Math.pow(10.,e);
        _bins[b+15] = +1.75 * Math.pow(10.,e);
        _bins[b+16] = +1.8  * Math.pow(10.,e);
        _bins[b+17] = +1.85 * Math.pow(10.,e);
        _bins[b+18] = +1.9  * Math.pow(10.,e);
        _bins[b+19] = +1.95 * Math.pow(10.,e);
        _bins[b+20] = +2.   * Math.pow(10.,e);
        _bins[b+21] = +2.1  * Math.pow(10.,e);
        _bins[b+22] = +2.2  * Math.pow(10.,e);
        _bins[b+23] = +2.3  * Math.pow(10.,e);
        _bins[b+24] = +2.4  * Math.pow(10.,e);
        _bins[b+25] = +2.5  * Math.pow(10.,e);
        _bins[b+26] = +2.6  * Math.pow(10.,e);
        _bins[b+27] = +2.7  * Math.pow(10.,e);
        _bins[b+28] = +2.8  * Math.pow(10.,e);
        _bins[b+29] = +2.9  * Math.pow(10.,e);
        _bins[b+30] = +3.   * Math.pow(10.,e);
        _bins[b+31] = +3.1  * Math.pow(10.,e);
        _bins[b+32] = +3.2  * Math.pow(10.,e);
        _bins[b+33] = +3.3  * Math.pow(10.,e);
        _bins[b+34] = +3.4  * Math.pow(10.,e);
        _bins[b+35] = +3.5  * Math.pow(10.,e);
        _bins[b+36] = +3.6  * Math.pow(10.,e);
        _bins[b+37] = +3.7  * Math.pow(10.,e);
        _bins[b+38] = +3.8  * Math.pow(10.,e);
        _bins[b+39] = +3.9  * Math.pow(10.,e);
        _bins[b+40] = +4.   * Math.pow(10.,e);
        _bins[b+41] = +4.1  * Math.pow(10.,e);
        _bins[b+42] = +4.2  * Math.pow(10.,e);
        _bins[b+43] = +4.3  * Math.pow(10.,e);
        _bins[b+44] = +4.4  * Math.pow(10.,e);
        _bins[b+45] = +4.5  * Math.pow(10.,e);
        _bins[b+46] = +4.6  * Math.pow(10.,e);
        _bins[b+47] = +4.7  * Math.pow(10.,e);
        _bins[b+48] = +4.8  * Math.pow(10.,e);
        _bins[b+49] = +4.9  * Math.pow(10.,e);
        _bins[b+50] = +5.   * Math.pow(10.,e);
        _bins[b+51] = +5.1  * Math.pow(10.,e);
        _bins[b+52] = +5.2  * Math.pow(10.,e);
        _bins[b+53] = +5.3  * Math.pow(10.,e);
        _bins[b+54] = +5.4  * Math.pow(10.,e);
        _bins[b+55] = +5.5  * Math.pow(10.,e);
        _bins[b+56] = +5.6  * Math.pow(10.,e);
        _bins[b+57] = +5.7  * Math.pow(10.,e);
        _bins[b+58] = +5.8  * Math.pow(10.,e);
        _bins[b+59] = +5.9  * Math.pow(10.,e);
        _bins[b+60] = +6.   * Math.pow(10.,e);
        _bins[b+61] = +6.1  * Math.pow(10.,e);
        _bins[b+62] = +6.2  * Math.pow(10.,e);
        _bins[b+63] = +6.3  * Math.pow(10.,e);
        _bins[b+64] = +6.4  * Math.pow(10.,e);
        _bins[b+65] = +6.5  * Math.pow(10.,e);
        _bins[b+66] = +6.6  * Math.pow(10.,e);
        _bins[b+67] = +6.7  * Math.pow(10.,e);
        _bins[b+68] = +6.8  * Math.pow(10.,e);
        _bins[b+69] = +6.9  * Math.pow(10.,e);
        _bins[b+70] = +7.   * Math.pow(10.,e);
        _bins[b+71] = +7.1  * Math.pow(10.,e);
        _bins[b+72] = +7.2  * Math.pow(10.,e);
        _bins[b+73] = +7.3  * Math.pow(10.,e);
        _bins[b+74] = +7.4  * Math.pow(10.,e);
        _bins[b+75] = +7.5  * Math.pow(10.,e);
        _bins[b+76] = +7.6  * Math.pow(10.,e);
        _bins[b+77] = +7.7  * Math.pow(10.,e);
        _bins[b+78] = +7.8  * Math.pow(10.,e);
        _bins[b+79] = +7.9  * Math.pow(10.,e);
        _bins[b+80] = +8.   * Math.pow(10.,e);
        _bins[b+81] = +8.1  * Math.pow(10.,e);
        _bins[b+82] = +8.2  * Math.pow(10.,e);
        _bins[b+83] = +8.3  * Math.pow(10.,e);
        _bins[b+84] = +8.4  * Math.pow(10.,e);
        _bins[b+85] = +8.5  * Math.pow(10.,e);
        _bins[b+86] = +8.6  * Math.pow(10.,e);
        _bins[b+87] = +8.7  * Math.pow(10.,e);
        _bins[b+88] = +8.8  * Math.pow(10.,e);
        _bins[b+89] = +8.9  * Math.pow(10.,e);
        _bins[b+90] = +9.   * Math.pow(10.,e);
        _bins[b+91] = +9.1  * Math.pow(10.,e);
        _bins[b+92] = +9.2  * Math.pow(10.,e);
        _bins[b+93] = +9.3  * Math.pow(10.,e);
        _bins[b+94] = +9.4  * Math.pow(10.,e);
        _bins[b+95] = +9.5  * Math.pow(10.,e);
        _bins[b+96] = +9.6  * Math.pow(10.,e);
        _bins[b+97] = +9.7  * Math.pow(10.,e);
        _bins[b+98] = +9.8  * Math.pow(10.,e);
        _bins[b+99] = +9.9  * Math.pow(10.,e);
      }
      //System.out.print(" +Long.MAX_VALUE at b=" + b);

      for (e=+19; e<=+307; e++, b+=5) {
        _bins[b]   = +      Math.pow(10.,e);
        _bins[b+1] = +1.5 * Math.pow(10.,e);
        _bins[b+2] = +2.5 * Math.pow(10.,e);
        _bins[b+3] = +4.  * Math.pow(10.,e);
        _bins[b+4] = +6.  * Math.pow(10.,e);
      }
      //System.out.print(" ~ +Double.MAX_VALUE at b="+b);
      _bins[b++] = +Double.MAX_VALUE; // -1.7e308

      _blast = b; // store the index of the highest bin in current use
      Arrays.fill(_bins, b, _bins.length-1, Double.POSITIVE_INFINITY);
      //System.out.println(" ...padded with +INF to b="+b);

      _counts = new long[_bins.length];

    }

    // pass2 and subsequent: refine _bins based on previous result (qbot[q], qtop[q])
    QuantilesTask(double[] qbot, double[] qtop) {
      assert qbot.length>0;
      System.out.println("TBD pass2 bins:");
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

        if (qbot[q]==_bins[b-1]) b--; // handle overlapping bins

        double bval = qbot[q];
        double bstep = (qtop[q]-qbot[q]) / (double)NBINS;
        for (int i=0; i<NBINS; i++, bval+=bstep) {
          _bins[b++] = bval;
        }
        _bins[b++] = qtop[q]; // include exact (presumably near-integer) top-of-bin
      }

      _blast = b;
      _bins[b++] = Double.POSITIVE_INFINITY;

      System.out.println("Pass2 bins:" + Arrays.toString(_bins));
    }

    @Override public void map(Chunk xs) {
      System.out.println("Chunk 0x" + xs +" of length=" + xs._len); // printing will distort runtime
      //PrintChunk("", xs);

      // Output
      _counts = new long[_bins.length];

      double value;  // don't use = MemoryManager.malloc8d(1)
      for (int i=0; i<xs._len; i++) {
        value = xs.at0(i);

        if (Double.isNaN(value)) continue;

        int b = binarySearchInexact(_bins, _bfirst, _blast, value);
        System.out.println(value + "-> _bin:" + _bins[b] +  " index:" + b);
        _counts[b]++;
      }

      // DEBUG: Print _bins,_counts
      for (int b=_bfirst; b<_blast; b++) {
        if (_bins[b]>0.1 && _bins[b]<=3000) // DEBUG on airline.csv
          System.out.println(_counts[b] + "\t" + _bins[b]);
          //System.out.println(_counts[b] + "\t" + String.format("%+6.5g",_bins[b]));
      }
    }

    // reduce: accumulate bin counts
    @Override public void reduce( QuantilesTask other ) {
      for (int b=_bfirst; b<_blast; b++) {
        this._counts[b] += other._counts[b];
      }
    }
  }

  private static void PrintChunk(String msg, Chunk xs) {
    System.out.print("\n" + msg);
    for (int i=0; i<xs._len; i++) {
      System.out.print(xs.at0(i) + ",");
    }
    System.out.println();
  }

  private static int binarySearchInexact(double[] _bins, int bfirst, int blast, double val) {
    // Inexact comparison: bin b contains the interval [ bins(b), bins(b+1) )
    int bmin = bfirst;
    int bmax = blast;
    int bmid = (bmin+bmax)/2;
    int deltab = bmax-bmin;
    while((bmin < bmax) && deltab>1) {
      deltab = bmax - bmin;
      bmid = (bmin+bmax)/2;
      if (val == _bins[bmid]) {
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

}
