package hex;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;

public class GBM extends Job {
  public static final String KEY_PREFIX = "__GBMModel_";
  public static final int HISTOGRAM_BINS=256;

  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }
  private GBM(Key dest, Frame fr) { super("GBM "+fr, dest); }
  // Called from a non-FJ thread; makea a GBM and hands it over to FJ threads
  public static GBM start(Key dest, final Frame fr) {
    final GBM job = new GBM(dest, fr);
    H2O.submitTask(job.start(new H2OCountedCompleter() {
        @Override public void compute2() { job.run(fr); tryComplete(); }
      })); 
    return job;
  }

  // ==========================================================================

  // Compute a GBM tree.  

  // Start by splitting all the data according to some criteria (minimize
  // variance at the leaves).  Record on each row which split it goes to, and
  // assign a split number to it (for next pass).  On *this* pass, use the
  // split-number to build a per-split histogram, with a per-histogram-bucket
  // variance.

  int _splitLevel;              // Tree split level.
  
  // Number of active splits at this level
  int _numSplits;

  // Total number of rows in each active split partition
  long _numRowsPerSplit[];

  // A histogram; one per split and each one has HISTOGRAM_BINS bins.
  long _histogram[][];

  // Compute a single GBM tree
  private void run(Frame fr) {
    // Initially setup as-if an empty-split had just happened
    _numSplits = 1;
    _numRowsPerSplit = new long[]{fr._vec[0].length()};

    while( true ) {

      // Build an array of histograms, one histogram per split.
      // The histogram array is "ragged" - we use smaller arrays
      // 
      _histogram = new long[_numSplits][];

      
    }
  }
}

