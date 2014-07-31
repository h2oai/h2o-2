package hex.singlenoderf;

import water.*;
import water.util.Utils;

//import hex.singlenoderf.TreeP;

import java.util.ArrayList;
import java.util.Arrays;

public abstract class Sampling {


  /** Available sampling strategies. */
  public enum Strategy {
    RANDOM(0); //,
    int _id; // redundant id
    private Strategy(int id) { _id = id; }
  }

  abstract Data sample(final Data data, long seed, Key modelKey, boolean local_mode);

  /** Deterministically sample the Data at the bagSizePct.  Toss out
   invalid rows (as-if not sampled), but maintain the sampling rate. */
  final static class Random extends Sampling {
    final double _bagSizePct;
    final int[]  _rowsPerChunks;

    public Random(double bagSizePct, int[] rowsPerChunks) { _bagSizePct = bagSizePct; _rowsPerChunks = rowsPerChunks; }

    @Override Data sample(final Data data, long seed, Key modelKey, boolean local_mode) {
      SpeeDRFModel m = UKV.get(modelKey);
      int [] sample;
      sample = sampleFair(data,seed,_rowsPerChunks);
      // add the remaining rows
      Arrays.sort(sample); // we want an ordered sample
      return new Subset(data, sample, 0, sample.length);
    }

    /** Roll a fair die for sampling, resetting the random die every numrows. */
    private int[] sampleFair(final Data data, long seed, int[] rowsPerChunks ) {
      // init
      java.util.Random rand = null;
      int   rows   = data.rows();
      int   size   = bagSize(rows,_bagSizePct);
      int[] sample = MemoryManager.malloc4((int) (size * 1.10));
      float f      = (float) _bagSizePct;
      int   cnt    = 0;  // Counter for resetting Random
      int   j      = 0;  // Number of selected samples
      int   cidx   = 0;  // Chunks counter
      // compute
      for( int i=0; i<rows; i++ ) {
        if( cnt--==0 ) {
          /* NOTE: Before changing used generator think about which kind of random generator you need:
           * if always deterministic or non-deterministic version - see hex.singlenoderf.Utils.get{Deter}RNG */
          long chunkSamplingSeed = chunkSampleSeed(seed, i);
          // DEBUG: System.err.println(seed + " : " + i + " (sampling)");
          rand = Utils.getDeterRNG(chunkSamplingSeed);
          cnt  = rowsPerChunks[cidx++]-1;
        }
        float randFloat = rand.nextFloat();
        if( randFloat < f ) {
          if( j == sample.length ) sample = Arrays.copyOfRange(sample,0,(int)(1 + sample.length*1.2));
          sample[j++] = i;
        }
      }
      return Arrays.copyOf(sample,j); // Trim out bad rows
    }
  }

  /**
   * ! CRITICAL code !
   * This method returns the correct seed based on initial seed and row index.
   *  WARNING : this method is crucial for correct replay of sampling.
   */
  static final long chunkSampleSeed(long seed, int rowIdx) { return seed + ((long)rowIdx<<16); }

  static final int bagSize( int rows, double bagSizePct ) {
    int size = (int)(rows * bagSizePct);
    return (size>0 || rows==0) ? size : 1;
  }
}
