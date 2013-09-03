package hex;

import water.*;
import water.fvec.*;

/**
 * @author smcinerney
 *
 * Quantiles by exact multipass bucket-based method, similar to Summary.
 * If Col summary was available for FluidVec, we could preinitialize the bins to cover col (min,max)
 *
 * Reference:
 * The 9 methods used in R stats::quantile() are defined in:
 * "Sample quantiles in statistical packages" - Hyndman & Fan - ASA 1996
 */

    // Pass 1: ...
    //long start = System.currentTimeMillis();
    //nrows = vec.length();
    /* Chunk dataset out to fixed-size subtrees */
    //MergeSortTask ms1 = new MergeSortTask().doAll(vec);
    //long pass1 = System.currentTimeMillis();
    //pass1time = pass1 - start;

    // Split the column chunks(?) and buffers out to nodes... make sure the buffers are local


public class Quantiles extends Iced {

  // Initialize bucket sizes to cover entire range of double
  //Vec _bins, _counts;
  byte[] _initchunk;
  double _binSizeRatio;
  //whether to implement nDefLess, nDefGrtr, nUnseen counters
  // Is bin-merging worth it? (messes up add()?) assume not.

  // Could switch directly on (sign,exponent) components of double:
  //  Sign bit: 1 bit.
  //  Binary exponent, biased by 1023: 11 bits.
  //  long exponent = ((bits & 0x7ff0000000000000L) >> 52) - 1023; // check this is correct
  //  Mantissa: 53 bits.
  //  long mantissa = (bits & 0x000fffffffffffffL) | 0x0010000000000000L;


  public Quantiles(Frame frm, Vec vec, double[] quantiles) {
    assert vec != null;
    // from vec, could look up Column c to get its min, max
    System.err.println("Vec" + vec + "has min:" + vec.min() + ", max:" + vec.max());

    // Allocate and add our temporary Vecs/Chunks to frame
    // Note: must allocate Vec, can't directly allocate Chunk / C8DChunk/ C8Chunk

    // Allocate Chunk (1M) for bins+counters. Should handle at least 16K? levels: 8K positive, 8K negative
    // Can't see how to get key for given frame?
    //Key countkey = frm...

    //long[] _initchunk = new long[2 << (Vec.LOG_CHK)];
    //Vec _counts = new Vec(countkey, _initchunk, false, 0); // ERROR: Vec(...) ctor not visible
    //frm.add("_counts", _counts);

    // TODO: bin min,max determination go here

    // TODO: get result by taking bins at corresponding indices phi*N
    // Optionally use one of the 9 averaging schemes as per R/ Hyndman & Fan
    double[] res = new double[quantiles.length];
    // TODO

  }


  public static class QuantilesTask extends MRTask2<QuantilesTask> {

    long[] _counts;

    @Override public void map(Chunk xs, Chunk bins) {
      System.err.println("Chunk 0x" + xs +" of length=" + xs._len); // printing will distort runtime
      PrintChunk("", xs);

      //double[] _bins = MemoryManager.malloc8d(bins._len);
      //for (int i=0; i<bins._len; i++) _bins[i] = bins.at0(i);

      // Output
      _counts = new long[bins._len];
      //Arrays.fill(_counts, 0);

      //double value = MemoryManager.malloc8d(1);
      double value;
      for (int i=0; i<xs._len; i++) {
        value = xs.at0(i); // cannot be isNan()

        // binary-search bins for value, inexact comparison, bin b contains the interval [ bins(b), bins(b+1) )
        int b = bins._len/2;
        int oldB = b;
        int deltaB = Math.max((bins._len/2 +1)/2, 1);
        int oldDeltaB = deltaB;
        while(b!=oldB && oldDeltaB!=1) { // TODO: test termination condition corner-cases
          oldB = b;

          // TODO: Double.MINUS_INFINITY might not match for equality
          if (value == bins.at0(b)) {
            break;
          } else if (value < bins.at0(b)) {
            b -= deltaB;
          } else if (value > bins.at0(b)) {
            b += deltaB;
          }

          oldDeltaB = deltaB;
          deltaB = Math.min((deltaB+1)/2, 1); // maybe suspect
        }
        _counts[b]++;
      }

      // DEBUG: Print counts
    }

    // reduce: accumulate bin counts
    @Override public void reduce( QuantilesTask other ) {
      for (int b=0; b<_counts.length; b++) {
        this._counts[b] += other._counts[b];
      }
    }
  }

  private static void PrintChunk(String msg, Chunk xs) {
    System.err.print("\n" + msg);
    for (int i=0; i<xs._len; i++) {
      System.err.print(xs.at0(i) + ",");
    }
    System.err.println();
  }

}
