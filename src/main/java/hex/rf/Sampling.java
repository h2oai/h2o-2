package hex.rf;

import java.util.Arrays;

import water.MemoryManager;
import water.util.Utils;

public abstract class Sampling {
  /** Available sampling strategies. */
  public enum Strategy {
    RANDOM(0),
    STRATIFIED_LOCAL(1);
//    STRATIFIED_DISTRIBUTED(2);
    int _id; // redundant id
    private Strategy(int id) { _id = id; }
  }

  abstract Data sample(final Data data, long seed);

  /** Deterministically sample the Data at the bagSizePct.  Toss out
      invalid rows (as-if not sampled), but maintain the sampling rate. */
  final static class Random extends Sampling {
    final double _bagSizePct;
    final int    _rowsPerChunk;

    public Random(double bagSizePct, int rowsPerChunk) { _bagSizePct = bagSizePct; _rowsPerChunk = rowsPerChunk; }

    @Override Data sample(final Data data, long seed) {
      int [] sample;
      sample = sampleFair(data,seed,_rowsPerChunk);
      // add the remaining rows
      Arrays.sort(sample); // we want an ordered sample
      return new Subset(data, sample, 0, sample.length);
      }

    /** Roll a fair die for sampling, resetting the random die every numrows. */
    private int[] sampleFair(final Data data, long seed, int rowsPerChunk ) {
      // preconditions
      assert rowsPerChunk != 0 : "RowsPerChunk contains 0! Not able to assure deterministic sampling!";
      // init
      java.util.Random rand = null;
      int   rows   = data.rows();
      int   size   = bagSize(rows,_bagSizePct);
      int[] sample = MemoryManager.malloc4((int)(size*1.10));
      float f      = (float) _bagSizePct;
      int   cnt    = 0;  // Counter for resetting Random
      int   j      = 0;  // Number of selected samples
      // compute
      for( int i=0; i<rows; i++ ) {
        if( cnt--==0 ) {
          /* NOTE: Before changing used generator think about which kind of random generator you need:
           * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
          long chunkSamplingSeed = chunkSampleSeed(seed, i);
          rand = Utils.getDeterRNG(chunkSamplingSeed);
          cnt  = rowsPerChunk-1;
          if( i+2*rowsPerChunk > rows ) cnt = rows; // Last chunk is big
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

  /** Strata is a dataset group which corresponds to a class.
   * Sampling is specified per strata group.
   *
   * Stratified sampling look only at local data stored on the node.
   */
  final static class StratifiedLocal extends Sampling {
    final private float[] _strataSamples;
    final private int     _rowsPerChunk;

    public StratifiedLocal(float[] strataSamples, int rowsPerChunk) {
      _strataSamples = strataSamples; _rowsPerChunk = rowsPerChunk;
    }

    @Override final Data sample(final Data data, long seed) {
        int sample[] = sampleLocalStratified(data, seed, _rowsPerChunk);
        Arrays.sort(sample);
        return new Subset(data, sample, 0, sample.length);
    }

    private int[] sampleLocalStratified(final Data data, long seed, int rowsPerChunk) {
      // preconditions
      assert _strataSamples.length == data._dapt.classes() : "There is not enought number of samples for individual stratas!";
      // precomputing - find the largest sample and compute the bag size for it
      float largestSample = 0.0f;
      for (float sample : _strataSamples) if (sample > largestSample) largestSample = sample;
      // compute
      java.util.Random rand   = null;
      int    rows   = data.rows();
      int[]  sample = new int[(int) (largestSample*rows)]; // be little bit more pessimistic
      int    j      = 0;
      int    cnt    = 0;
      // collect samples per strata
      for (int row=0; row<rows; row++) {
        if( cnt--==0 ) {
          long chunkSamplingSeed = chunkSampleSeed(seed, row);
          rand = Utils.getDeterRNG(chunkSamplingSeed);
          cnt  = rowsPerChunk-1;
          if( row+2*rowsPerChunk > rows ) cnt = rows; // Last chunk is big
        }
        float randFloat = rand.nextFloat();
        if (!data._dapt.hasBadValue(row, data._dapt.classColIdx())) {
          int strata = data._dapt.classOf(row); // strata groups are represented by response classes
          if (randFloat < _strataSamples[strata]) {
            if( j == sample.length ) sample = Arrays.copyOfRange(sample,0,(int)(1+sample.length*1.2));
            sample[j++] = row;
          }
        }
      }
      return Arrays.copyOf(sample,j);
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
