package hex;

import java.util.Arrays;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Utils;

/**
 * Support class for extracting things from frame.
 **/
public abstract class FrameExtractor extends H2OCountedCompleter {
  /** Dataset to split */
  final Frame   dataset;
  /** Destination keys for each output frame split. */
  final Key[]   destKeys;
  /** Optional job key */
  final Key     jobKey;

  /** Output frames for each output split part */
  private Frame[] splits;
  /** Temporary variable holding exceptions of workers */
  private Throwable[] workersExceptions;

  public FrameExtractor(Frame dataset, Key[] destKeys, Key jobKey) {
    this.dataset  = dataset;
    this.jobKey   = jobKey;
    this.destKeys = destKeys!=null ? destKeys : generateDestKeys(dataset!=null?dataset._key:null, numOfOutputs());
  }

  @Override public void compute2() {
    // Lock all possible data
    dataset.read_lock(jobKey);
    // Create a template vector for each segment
    final Vec[][] templates = makeTemplates();
    final int nsplits = templates.length;
    assert templates.length == numOfOutputs() : "Number of outputs and number of created templates differ!";
    final Vec[] datasetVecs = dataset.vecs();
    // Create output frames
    splits = new Frame[nsplits];
    for (int s=0; s<nsplits; s++) {
      Frame split = new Frame(destKeys[s], dataset.names(), templates[s] );
      split.delete_and_lock(jobKey);
      splits[s] = split;
    }
    // Launch number of distributed FJ for each split part
    setPendingCount(1);
    H2O.submitTask(new H2OCountedCompleter(FrameExtractor.this) {
      @Override public void compute2() {
        setPendingCount(nsplits);
        for (int s=0; s<nsplits; s++) {
          MRTask2 mrt = createNewWorker(new H2OCountedCompleter(this) { // Completer for this task
            @Override public void compute2() { }
            @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
              synchronized( FrameExtractor.this ) { // synchronized on this since can be accessed from different workers
                workersExceptions = workersExceptions!=null ? Arrays.copyOf(workersExceptions, workersExceptions.length+1) : new Throwable[1];
                workersExceptions[workersExceptions.length-1] = ex;
              }
              tryComplete(); // we handle the exception so wait perform normal completion
              return false;
            }
          }, datasetVecs, s);
          assert mrt.getCompleter() != null : "The `createNewWorker` method violates API contract and forgets to setup given counted completer!";
          mrt.asyncExec(splits[s]);
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

  /** Create a new worker which has to setup given completer. */
  protected abstract MRTask2 createNewWorker(H2OCountedCompleter completer, Vec[] inputVecs, int split) ;

  /** Create a templates for vector composing output frame */
  protected Vec[][] makeTemplates() {
    Vec anyVec = dataset.anyVec();
    final long[][] espcPerSplit = computeEspcPerSplit(anyVec._espc, anyVec.length());
    final int num = dataset.numCols(); // number of columns in input frame
    final int nsplits = espcPerSplit.length; // number of splits
    final String[][] domains = dataset.domains(); // domains
    final boolean[] uuids = dataset.uuids();
    final byte[] times = dataset.times();
    Vec[][] t = new Vec[nsplits][/*num*/]; // resulting vectors for all
    for (int i=0; i<nsplits; i++) {
      // vectors for j-th split
      t[i] = new Vec(Vec.newKey(),espcPerSplit[i/*-th split*/]).makeZeros(num, domains, uuids, times);
    }
    return t;
  }

  /**
   * Compute espc for output vectors for each split.
   * @param espc input vector espc
   * @param nrows total number of rows in input vector
   * @return espc for each partition
   */
  protected abstract long[][] computeEspcPerSplit(long[] espc, long nrows) ;
  /**
   * Generates default names for destination keys.
   *
   * @param masterKey
   *          key for input dataset
   * @param numberOfKeys
   *          number of keys to generate
   * @return return an array of keys.
   */
  protected Key[] generateDestKeys(Key masterKey, int numberOfKeys) {
    return Utils.generateNumKeys(masterKey, numberOfKeys);
  }

  /** Return a number of resulting frame which this task produces. */
  protected abstract int numOfOutputs();

}
