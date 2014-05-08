package hex;

import java.util.Arrays;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Utils;

/**
 * Frame splitter function to divide given frame into
 * multiple partitions based on given ratios.
 *
 * <p>The task creates <code>ratios.length+1</code> output frame each containing a
 * demanded fraction of rows from source dataset</p>
 *
 * <p>The task internally processes each chunk of each column of source dataset
 * and extract i-th fraction of chunk rows into output chunk. The task does not
 * shuffle rows inside the tasks, nevertheless it would be possible.</p>
 *
 * <p>Assumptions and invariants</p>
 * <ul>
 * <li>number of demanding split parts is reasonable number, i.e., &lt;10. The task is not designed to split into many small parts.</li>
 * <li>the worker preserves distribution of new chunks over the cloud according to source dataset chunks.</li>
 * <li>rows inside one chunk are not shuffled, they are extracted deterministically in the same order based on output part index.</li>
 * </ul>
 */
public class FrameSplitter extends H2OCountedCompleter {
  /** Dataset to split */
  final Frame   dataset;
  /** Split ratios - resulting number of split is ratios.length+1 */
  final float[] ratios;
  /** Destination keys for each output frame split. */
  final Key[]   destKeys;
  /** Optional job key */
  final Key     jobKey;
  /** Random seed */
  final long    seed;

  /** Output frames for each output split part */
  private Frame[] splits;
  /** Temporary variable holding exceptions of workers */
  private Throwable[] workersExceptions;

  public FrameSplitter(Frame dataset, float[] ratios) { this(dataset, ratios, 43); }
  public FrameSplitter(Frame dataset, float[] ratios, long seed) {
    this(dataset, ratios, null, null, seed);
  }
  public FrameSplitter(Frame dataset, float[] ratios, Key[] destKeys, Key jobKey, long seed) {
    assert ratios.length > 0 : "No ratio specified!";
    assert ratios.length < 100 : "Too many frame splits demanded!";
    this.dataset  = dataset;
    this.ratios    = ratios;
    this.destKeys = destKeys!=null ? destKeys : Utils.generateNumKeys(dataset._key, ratios.length+1);
    assert this.destKeys.length == this.ratios.length+1 : "Unexpected number of destination keys.";
    this.jobKey   = jobKey;
    this.seed     = seed;
  }

  @Override public void compute2() {
    // Lock all possible data
    dataset.read_lock(jobKey);
    // Create a template vector for each segment
    final Vec[][] templates = makeTemplates(dataset, ratios);
    final int nsplits = templates.length;
    assert nsplits == ratios.length+1 : "Unexpected number of split templates!";
    // Launch number of distributed FJ for each split part
    final Vec[] datasetVecs = dataset.vecs();
    splits = new Frame[nsplits];
    for (int s=0; s<nsplits; s++) {
      Frame split = new Frame(destKeys[s], dataset.names(), templates[s] );
      split.delete_and_lock(jobKey);
      splits[s] = split;
    }
    setPendingCount(1);
    H2O.submitTask(new H2OCountedCompleter(FrameSplitter.this) {
      @Override public void compute2() {
        setPendingCount(nsplits);
        for (int s=0; s<nsplits; s++) {
          new FrameSplitTask(new H2OCountedCompleter(this) { // Completer for this task
            @Override public void compute2() { }
            @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
              synchronized( FrameSplitter.this ) { // synchronized on this since can be accessed from different workers
                workersExceptions = workersExceptions!=null ? Arrays.copyOf(workersExceptions, workersExceptions.length+1) : new Throwable[1];
                workersExceptions[workersExceptions.length-1] = ex;
              }
              tryComplete(); // we handle the exception so wait perform normal completion
              return false;
            }
          }, datasetVecs, ratios, s).asyncExec(splits[s]);
        }
        tryComplete(); // complete the computation of nsplits-tasks
      }
    });
    tryComplete(); // complete the computation of thrown tasks
  }

  /** Blocking call to obtain a result of computation. */
  public Frame[] getResult() {
    join();
    if (workersExceptions!=null) throw new RuntimeException(workersExceptions[0]);
    return splits;
  }

  @Override public void onCompletion(CountedCompleter caller) {
    boolean exceptional = workersExceptions!=null;
    dataset.unlock(jobKey);
    if (splits!=null) {
      for (Frame s : splits) {
        if (s!=null) {
          if (!exceptional) {
            s.update(jobKey);
            s.unlock(jobKey);
          } else { // Have to unlock and delete here
            s.unlock(jobKey);
            s.delete(jobKey, 3.14f); // delete all splits
          }
        }
      }
    }
  }

  // Make vector templates for all output frame vectors
  private Vec[][] makeTemplates(Frame dataset, float[] ratios) {
    final long[][] espcPerSplit = computeEspcPerSplit(dataset.anyVec(), ratios);
    final int num = dataset.numCols(); // number of columns in input frame
    final int nsplits = espcPerSplit.length; // number of splits
    final String[][] domains = dataset.domains(); // domains
    Vec[][] t = new Vec[nsplits][/*num*/]; // resulting vectors for all
    for (int i=0; i<nsplits; i++) {
      // vectors for j-th split
      t[i] = new Vec(Vec.newKey(),espcPerSplit[i/*-th split*/]).makeZeros(num, domains);
    }
    return t;
  }

  private long[/*nsplits*/][/*nchunks*/] computeEspcPerSplit(Vec v, float[] ratios) {
    long[] espc = v._espc;
    long[][] r = new long[ratios.length+1][espc.length];
    for (int i=0; i<espc.length-1; i++) {
      int nrows = (int) (espc[i+1]-espc[i]);
      int[] splits = Utils.partitione(nrows, ratios, (byte) (i%2));
      assert splits.length == ratios.length+1 : "Unexpected number of splits";
      for (int j=0; j<splits.length; j++) {
        r[j][i+1] = r[j][i] + splits[j]; // previous + current number of rows
      }
    }
    return r;
  }

  /** MR task extract specified part of <code>_srcVecs</code>
   * into output chunk.*/
  private static class FrameSplitTask extends MRTask2<FrameSplitTask> {
    final Vec  [] _srcVecs;
    final float[] _ratios;
    final int     _partIdx;
    public FrameSplitTask(H2OCountedCompleter completer, Vec[] srcVecs, float[] ratios, int partIdx) {
      super(completer);
      _srcVecs = srcVecs;
      _ratios  = ratios;
      _partIdx = partIdx;
    }
    @Override public void map(Chunk[] cs) {
      // Get corresponding input chunk for this chunk and its length
      // NOTE: expecting the same distribution of input chunks as output chanks
      int cidx = cs[0].cidx();
      int len = _srcVecs[0].chunkLen(cidx); // get length of original chunk
      int[] splits = Utils.partitione(len, _ratios, (byte)(cidx%2)); // compute partitions for different parts
      int startRow = 0;
      for (int i=0; i<_partIdx; i++) startRow += splits[i];
      // For each output chunk extract appropriate rows for partIdx-th part
      for (int i=0; i<cs.length; i++) {
        // Extract correct rows of _partIdx-th split from i-th input vector into the i-th chunk
        assert cs[i]._len == splits[_partIdx]; // Be sure that we correctly prepared vector template
        // NOTE: we preserve co-location of cs[i] chunks with _srcVecs[i] chunks so it is local load of chunk
        ChunkSplitter.extractChunkPart(_srcVecs[i].chunkForChunkIdx(cidx), cs[i], startRow, splits[_partIdx], _fs);
      }
    }
  }
}
