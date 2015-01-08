package water.util;

import static water.util.Utils.getDeterRNG;
import water.*;
import water.fvec.*;

import java.util.Random;

public class MRUtils {

  /**
   * Sample rows from a frame.
   * Can be unlucky for small sampling fractions - will continue calling itself until at least 1 row is returned.
   * @param fr Input frame
   * @param rows Approximate number of rows to sample (across all chunks)
   * @param seed Seed for RNG
   * @return Sampled frame
   */
  public static Frame sampleFrame(Frame fr, final long rows, final long seed) {
    if (fr == null) return null;
    final float fraction = rows > 0 ? (float)rows / fr.numRows() : 1.f;
    if (fraction >= 1.f) return fr;
    Frame r = new MRTask2() {
      @Override
      public void map(Chunk[] cs, NewChunk[] ncs) {
        final Random rng = getDeterRNG(seed + cs[0].cidx());
        int count = 0;
        for (int r = 0; r < cs[0]._len; r++)
          if (rng.nextFloat() < fraction || (count == 0 && r == cs[0]._len-1) ) {
            count++;
            for (int i = 0; i < ncs.length; i++) {
              ncs[i].addNum(cs[i].at0(r));
            }
          }
      }
    }.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());
    if (r.numRows() == 0) {
      Log.warn("You asked for " + rows + " rows (out of " + fr.numRows() + "), but you got none (seed=" + seed + ").");
      Log.warn("Let's try again. You've gotta ask yourself a question: \"Do I feel lucky?\"");
      return sampleFrame(fr, rows, seed+1);
    }
    return r;
  }

  /**
   * Row-wise shuffle of a frame (only shuffles rows inside of each chunk)
   * @param fr Input frame
   * @return Shuffled frame
   */
  public static Frame shuffleFramePerChunk(Frame fr, final long seed) {
    return shuffleFramePerChunk(null, fr, seed);
  }
  public static Frame shuffleFramePerChunk(Key outputFrameKey, Frame fr, final long seed) {
    Frame r = new MRTask2() {
      @Override
      public void map(Chunk[] cs, NewChunk[] ncs) {
        long[] idx = new long[cs[0]._len];
        for (int r=0; r<idx.length; ++r) idx[r] = r;
        Utils.shuffleArray(idx, seed);
        for (int r=0; r<idx.length; ++r) {
          for (int i = 0; i < ncs.length; i++) {
            ncs[i].addNum(cs[i].at0((int)idx[r]));
          }
        }
      }
    }.doAll(fr.numCols(), fr).outputFrame(outputFrameKey, fr.names(), fr.domains());
    return r;
  }

  /**
   * Global redistribution of a Frame (balancing of chunks), done by calling process (all-to-one + one-to-all)
   * @param fr Input frame
   * @param seed RNG seed
   * @param shuffle whether to shuffle the data globally
   * @return Shuffled frame
   */
  public static Frame shuffleAndBalance(final Frame fr, int splits, long seed, final boolean local, final boolean shuffle) {
    if( (fr.vecs()[0].nChunks() < splits || shuffle) && fr.numRows() > splits) {
      Vec[] vecs = fr.vecs().clone();
      Log.info("Load balancing dataset, splitting it into up to " + splits + " chunks.");
      long[] idx = null;
      if (shuffle) {
        idx = new long[splits];
        for (int r=0; r<idx.length; ++r) idx[r] = r;
        Utils.shuffleArray(idx, seed);
      }
      Key keys[] = new Vec.VectorGroup().addVecs(vecs.length);
      final long rows_per_new_chunk = (long)(Math.ceil((double)fr.numRows()/splits));
      //loop over cols (same indexing for each column)
      Futures fs = new Futures();
      for(int col=0; col<vecs.length; col++) {
        AppendableVec vec = new AppendableVec(keys[col]);
        // create outgoing chunks for this col
        NewChunk[] outCkg = new NewChunk[splits];
        for(int i=0; i<splits; ++i)
          outCkg[i] = new NewChunk(vec, i);
        //loop over all incoming chunks
        for( int ckg = 0; ckg < vecs[col].nChunks(); ckg++ ) {
          final Chunk inCkg = vecs[col].chunkForChunkIdx(ckg);
          // loop over local rows of incoming chunks (fast path)
          for (int row = 0; row < inCkg._len; ++row) {
            int outCkgIdx = (int)((inCkg._start + row) / rows_per_new_chunk); // destination chunk idx
            if (shuffle) outCkgIdx = (int)(idx[outCkgIdx]); //shuffle: choose a different output chunk
            assert(outCkgIdx >= 0 && outCkgIdx < splits);
            outCkg[outCkgIdx].addNum(inCkg.at0(row));
          }
        }
        for(int i=0; i<outCkg.length; ++i)
          outCkg[i].close(i, fs);
        Vec t = vec.close(fs);
        t._domain = vecs[col]._domain;
        vecs[col] = t;
      }
      fs.blockForPending();
      Log.info("Load balancing done.");
      return new Frame(fr.names(), vecs);
    }
    return fr;
  }

  /**
   * Compute the class distribution from a class label vector
   * (not counting missing values)
   *
   * Usage 1: Label vector is categorical
   * ------------------------------------
   * Vec label = ...;
   * assert(label.isEnum());
   * long[] dist = new ClassDist(label).doAll(label).dist();
   *
   * Usage 2: Label vector is numerical
   * ----------------------------------
   * Vec label = ...;
   * int num_classes = ...;
   * assert(label.isInt());
   * long[] dist = new ClassDist(num_classes).doAll(label).dist();
   *
   */
  public static class ClassDist extends ClassDistHelper {
    public ClassDist(final Vec label) { super(label.domain().length); }
    public ClassDist(int n) { super(n); }
    public final long[] dist() { return _ys; }
    public final float[] rel_dist() {
      float[] rel = new float[_ys.length];
      for (int i=0; i<_ys.length; ++i) rel[i] = (float)_ys[i];
      final float sum = Utils.sum(rel);
      assert(sum != 0.);
      Utils.div(rel, sum);
      return rel;
    }
  }
  private static class ClassDistHelper extends MRTask2<ClassDist> {
    private ClassDistHelper(int nclass) { _nclass = nclass; }
    final int _nclass;
    protected long[] _ys;
    @Override public void map(Chunk ys) {
      _ys = new long[_nclass];
      for( int i=0; i<ys._len; i++ )
        if( !ys.isNA0(i) )
          _ys[(int)ys.at80(i)]++;
    }
    @Override public void reduce( ClassDist that ) { Utils.add(_ys,that._ys); }
  }


  /**
   * Stratified sampling for classifiers
   * @param fr Input frame
   * @param label Label vector (must be enum)
   * @param sampling_ratios Optional: array containing the requested sampling ratios per class (in order of domains), will be overwritten if it contains all 0s
   * @param maxrows Maximum number of rows in the returned frame
   * @param seed RNG seed for sampling
   * @param allowOversampling Allow oversampling of minority classes
   * @param verbose Whether to print verbose info
   * @return Sampled frame, with approximately the same number of samples from each class (or given by the requested sampling ratios)
   */
  public static Frame sampleFrameStratified(final Frame fr, Vec label, float[] sampling_ratios, long maxrows, final long seed, final boolean allowOversampling, final boolean verbose) {
    if (fr == null) return null;
    assert(label.isEnum());
    assert(maxrows >= label.domain().length);

    long[] dist = new ClassDist(label).doAll(label).dist();
    assert(dist.length > 0);
    Log.info("Doing stratified sampling for data set containing " + fr.numRows() + " rows from " + dist.length + " classes. Oversampling: " + (allowOversampling ? "on" : "off"));
    if (verbose) {
      for (int i=0; i<dist.length;++i) {
        Log.info("Class " + label.domain(i) + ": count: " + dist[i] + " prior: " + (float)dist[i]/fr.numRows());
      }
    }

    // create sampling_ratios for class balance with max. maxrows rows (fill existing array if not null)
    if (sampling_ratios == null || (Utils.minValue(sampling_ratios) == 0 && Utils.maxValue(sampling_ratios) == 0)) {
      // compute sampling ratios to achieve class balance
      if (sampling_ratios == null) {
        sampling_ratios = new float[dist.length];
      }
      assert(sampling_ratios.length == dist.length);
      for (int i=0; i<dist.length;++i) {
        if (dist[i] == 0) {
          Log.warn("No rows of class " + label.domain()[i] + " found.");
        }
        sampling_ratios[i] = dist[i] == 0 ? 1 // don't sample if there's no rows of a certain class (avoid division by 0)
                : ((float)fr.numRows() / label.domain().length) / dist[i]; // prior^-1 / num_classes
        assert(sampling_ratios[i] >= 0);
      }
      final float inv_scale = Utils.minValue(sampling_ratios); //majority class has lowest required oversampling factor to achieve balance
      if (!Float.isNaN(inv_scale) && !Float.isInfinite(inv_scale))
        Utils.div(sampling_ratios, inv_scale); //want sampling_ratio 1.0 for majority class (no downsampling)
    }
    for (float s : sampling_ratios) assert(!Float.isNaN(s) && !Float.isInfinite(s));

    if (!allowOversampling) {
      for (int i=0; i<sampling_ratios.length; ++i) {
        sampling_ratios[i] = Math.min(1.0f, sampling_ratios[i]);
      }
    }
    for (float s : sampling_ratios) assert(!Float.isNaN(s) && !Float.isInfinite(s));

    // given these sampling ratios, and the original class distribution, this is the expected number of resulting rows
    float numrows = 0;
    for (int i=0; i<sampling_ratios.length; ++i) {
      numrows += sampling_ratios[i] * dist[i];
    }
    final long actualnumrows = Math.min(maxrows, Math.round(numrows)); //cap #rows at maxrows
    assert(actualnumrows >= 0); //can have no matching rows in case of sparse data where we had to fill in a makeZero() vector
    Log.info("Stratified sampling to a total of " + String.format("%,d", actualnumrows) + " rows" + (actualnumrows < numrows ? " (limited by max_after_balance_size).":"."));

    if (actualnumrows != numrows) {
      assert(numrows > 0);
      Utils.mult(sampling_ratios, (float)actualnumrows/numrows); //adjust the sampling_ratios by the global rescaling factor
      if (verbose)
        Log.info("Downsampling majority class by " + (float)actualnumrows/numrows
                + " to limit number of rows to " + String.format("%,d", maxrows));
    }
    if (Utils.minIndex(sampling_ratios) == Utils.maxIndex(sampling_ratios)) {
      Log.info("All classes are sampled with sampling ratio: " + Utils.minValue(sampling_ratios));
    } else {
      for (int i=0;i<label.domain().length;++i) {
        Log.info("Class '" + label.domain()[i].toString()
                + "' sampling ratio: " + sampling_ratios[i]);
      }
    }

    return sampleFrameStratified(fr, label, sampling_ratios, seed, verbose);
  }

  /**
   * Stratified sampling
   * @param fr Input frame
   * @param label Label vector (from the input frame)
   * @param sampling_ratios Given sampling ratios for each class, in order of domains
   * @param seed RNG seed
   * @param debug Whether to print debug info
   * @return Stratified frame
   */
  public static Frame sampleFrameStratified(final Frame fr, Vec label, final float[] sampling_ratios, final long seed, final boolean debug) {
    return sampleFrameStratified(fr, label, sampling_ratios, seed, debug, 0);
  }

  // internal version with repeat counter
  // currently hardcoded to do up to 10 tries to get a row from each class, which can be impossible for certain wrong sampling ratios
  private static Frame sampleFrameStratified(final Frame fr, Vec label, final float[] sampling_ratios, final long seed, final boolean debug, int count) {
    if (fr == null) return null;
    assert(label.isEnum());
    assert(sampling_ratios != null && sampling_ratios.length == label.domain().length);
    for (float s : sampling_ratios) assert(!Float.isNaN(s));
    final int labelidx = fr.find(label); //which column is the label?
    assert(labelidx >= 0);

    final boolean poisson = false; //beta feature

    Frame r = new MRTask2() {
      @Override
      public void map(Chunk[] cs, NewChunk[] ncs) {
        final Random rng = getDeterRNG(seed + cs[0].cidx());
        for (int r = 0; r < cs[0]._len; r++) {
          if (cs[labelidx].isNA0(r)) continue; //skip missing labels
          final int label = (int)cs[labelidx].at80(r);
          assert(sampling_ratios.length > label && label >= 0);
          int sampling_reps;
          if (poisson) {
            sampling_reps = Utils.getPoisson(sampling_ratios[label], rng);
          } else {
            final float remainder = sampling_ratios[label] - (int)sampling_ratios[label];
            sampling_reps = (int)sampling_ratios[label] + (rng.nextFloat() < remainder ? 1 : 0);
          }
          for (int i = 0; i < ncs.length; i++) {
            for (int j = 0; j < sampling_reps; ++j) {
              ncs[i].addNum(cs[i].at0(r));
            }
          }
        }
      }
    }.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());
    assert(r.numCols() == fr.numCols());

    // Confirm the validity of the distribution
    long[] dist = new ClassDist(r.vecs()[labelidx]).doAll(r.vecs()[labelidx]).dist();

    // if there are no training labels in the test set, then there is no point in sampling the test set
    if (dist == null) {
      r.delete();
      return fr;
    }

    if (debug) {
      long sumdist = Utils.sum(dist);
      Log.info("After stratified sampling: " + sumdist + " rows.");
      for (int i=0; i<dist.length;++i) {
        Log.info("Class " + r.vecs()[labelidx].domain(i) + ": count: " + dist[i]
                + " sampling ratio: " + sampling_ratios[i] + " actual relative frequency: " + (float)dist[i] / sumdist * dist.length);
      }
    }

    // Re-try if we didn't get at least one example from each class
    if (Utils.minValue(dist) == 0 && count < 10) {
      Log.info("Re-doing stratified sampling because not all classes were represented (unlucky draw).");
      r.delete();
      return sampleFrameStratified(fr, label, sampling_ratios, seed+1, debug, ++count);
    }

//    // shuffle intra-chunk
//    Frame shuffled = shuffleFramePerChunk(r, seed + 0x580FF13);
//    r.delete();
//    return shuffled;
    return r;
  }

  /**
   * Compute the L2 norm for each row of the frame
   * @param fr Input frame
   * @return Vec containing L2 values for each row, is in K-V store
   */
  public static Vec getL2(final Frame fr, final double[] scale) {
    // add workspace vec at end
    final int idx = fr.numCols();
    assert(scale.length == idx) : "Mismatch for number of columns";
    fr.add("L2", fr.anyVec().makeZero());
    Vec res;
    try {
      new MRTask2() {
        @Override
        public void map(Chunk[] cs) {
          for (int r = 0; r < cs[0]._len; r++) {
            double norm2 = 0;
            for (int i = 0; i < idx; i++)
              norm2 += Math.pow(cs[i].at0(r) * scale[i], 2);
            cs[idx].set0(r, Math.sqrt(norm2));
          }
        }
      }.doAll(fr);
    } finally {
      res = fr.remove(idx);
    }
    res.rollupStats();
    return res;
  }
}
