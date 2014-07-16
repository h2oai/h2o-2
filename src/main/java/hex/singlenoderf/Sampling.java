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

  abstract Data sample(final Data data, long seed, Key key, boolean local_mode);

  /** Deterministically sample the Data at the bagSizePct.  Toss out
   invalid rows (as-if not sampled), but maintain the sampling rate. */
  final static class Random extends Sampling {
    final double _bagSizePct;
    final int[]  _rowsPerChunks;

    public Random(double bagSizePct, int[] rowsPerChunks) { _bagSizePct = bagSizePct; _rowsPerChunks = rowsPerChunks; }
    private TreeP[] getLocalTrees(SpeeDRFModel m) {
      ArrayList<TreeP> trees = new ArrayList<TreeP>();
      TreeP[] all_trees = m.tree_pojos;
      for (int i = 0; i < all_trees.length; ++i) {
        TreeP t = all_trees[i];
        if (t == null) return null;
        if (t._tk.home()) {
          t._tree_id = i;
          t._numErrs = m.errorsPerTree[i];
          trees.add(t);
        }
      }
      TreeP[] res = new TreeP[trees.size()];
      for (int i = 0; i < res.length; ++i) res[i] = trees.get(i);
      return res;
    }

    @Override Data sample(final Data data, long seed, Key modelKey, boolean local_mode) {
      SpeeDRFModel m = UKV.get(modelKey);
      TreeP[] localTrees = getLocalTrees(m);
      int [] sample;
      sample = localTrees != null && local_mode ? biteDIVotes(data, seed, localTrees, m) : sampleFair(data,seed,_rowsPerChunks);
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

    /** Sample with replacement using the IVotes approach
     *
     *  Build a Bite using the k current classifiers:
     *
     *  Select an observation(x,y) and add to new Bite according to:
     *    E_k(x) == y
     *        ? ( runif(1) > (E_k.err() / (1 - E_k.err()))
     *            ? Bite.add(x,y)
     *            : doNothing())
     *        : Bite.add(x,y);
     *
     *  Continue to sample until Bite.size == sample.length, return the bite.
     */
    private int[] biteDIVotes(final Data data, long seed, TreeP[] localTrees, SpeeDRFModel m) {
      java.util.Random rand = null;
      int   rows   = data.rows();
      int   size   = bagSize(rows,_bagSizePct);
      ArrayList<Integer> sample = new ArrayList<Integer>();
      rand = Utils.getDeterRNG(seed);
      // compute
      while (sample.size() < size) {
        int row_idx = rand.nextInt(rows);
        Data.Row r = data.at(row_idx);
        int cls = r.classOf();
        if (isCorrect(localTrees, cls, r, data._dapt.classes(), m, data)) {
          if (rand.nextFloat() < probability(localTrees)) continue;
          sample.add(row_idx);
        } else {
          sample.add(row_idx);
        }
      }
      assert sample.size() == size;
      int[] bite = new int[sample.size()];
      for (int i = 0; i < bite.length; ++i) bite[i] = sample.get(i);
      return bite;
    }

    private boolean isCorrect(TreeP[] localTrees, int ref, Data.Row r, int num_classes, SpeeDRFModel m, Data d) {
      int[] votes = new int[num_classes + 1];
      for (TreeP t: localTrees) {
        if (!t.isOOB(r)) continue;
        votes[classify(t, r, m, d)]++;
      }
      votes[0] = Utils.maxIndex(votes);
      return votes[0] == ref;
    }

    private int classify(TreeP t, Data.Row r, SpeeDRFModel m, Data d) {
//      return (int) m.tree_pojos[t._tree_id].classify(r);
      return (int) Tree.classify(new AutoBuffer(m.tree(t._tree_id)), d.unpackRow(r), d.classes(), false);
    }

    private float error_rate(TreeP[] localTrees) {
      int total_errrs = 0;
      int total_train = 0;
      for (TreeP t : localTrees) {
        total_errrs += t.get_numErrs();
        total_train += t.get_trainSize();
      }
      return (float) total_errrs / (float) total_train;
    }

    private float probability(TreeP[] localTrees) {
      float err = error_rate(localTrees);
      return err / (1 - err);
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
