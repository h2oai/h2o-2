package water.util;

import water.H2O;
import water.H2ONode;
import water.Key;
import water.MRTask2;
import water.fvec.*;

import java.util.Random;

import static water.util.Utils.getDeterRNG;

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
        for (int r = 0; r < cs[0]._len; r++)
          if (rng.nextFloat() < fraction) {
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
    }.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());
    return r;
  }

  /**
   * Global redistribution of a Frame (balancing of chunks), done my calling process (all-to-one + one-to-all)
   * @param fr Input frame
   * @param seed RNG seed
   * @param shuffle whether to shuffle the data globally
   * @return Shuffled frame
   */
  public static Frame shuffleAndBalance(final Frame fr, long seed, final boolean shuffle) {
    int cores = 0;
    for( H2ONode node : H2O.CLOUD._memary )
      cores += node._heartbeat._num_cpus;
    final int splits = cores;

    Vec[] vecs = fr.vecs();
    if( vecs[0].nChunks() < splits || shuffle ) {
      long[] idx = null;
      if (shuffle) {
        idx = new long[(int)fr.numRows()]; //HACK: should be a long
        for (int r=0; r<idx.length; ++r) idx[r] = r;
        Utils.shuffleArray(idx, seed);
      }
      Key keys[] = new Vec.VectorGroup().addVecs(vecs.length);
      for( int v = 0; v < vecs.length; v++ ) {
        AppendableVec vec = new AppendableVec(keys[v]);
        final long rows = fr.numRows();
        for( int split = 0; split < splits; split++ ) {
          long off = rows * split / splits;
          long lim = rows * (split + 1) / splits;
          assert(lim <= Integer.MAX_VALUE);
          NewChunk chunk = new NewChunk(vec, split);
          for( long r = off; r < lim; r++ ) {
            if (shuffle) chunk.addNum(fr.vecs()[v].at(idx[(int)r]));
            else chunk.addNum(fr.vecs()[v].at(r));
          }
          chunk.close(split, null);
        }
        Vec t = vec.close(null);
        t._domain = vecs[v]._domain;
        vecs[v] = t;
      }
    }
    fr.reloadVecs();
    return new Frame(fr.names(), vecs);
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
   * @param minrows Minimum number of rows in the returned frame, should be (much) greater than the number of classes
   * @param maxrows Maximum number of rows in the returned frame, must be > minrows
   * @param seed RNG seed for sampling
   * @param sampling_ratios Optional: array containing the requested sampling ratios per class (in order of domains), will be overwritten if it contains all 0s
   * @return Sampled frame, with approximately the same number of samples from each class (or given by the requested sampling ratios)
   */
  public static Frame sampleFrameStratified(final Frame fr, Vec label, float[] sampling_ratios, long minrows, long maxrows, final long seed, final boolean debug) {
    if (fr == null) return null;
    assert(label.isEnum());
    assert(maxrows >= minrows);
    assert(maxrows >= fr.numRows());

    // create sampling_ratios (fill existing array if not null)
    if (sampling_ratios == null || (Utils.minValue(sampling_ratios) == 0 && Utils.maxValue(sampling_ratios) == 0)) {
      long[] dist = new ClassDist(label).doAll(label).dist();
      assert(dist.length > 0);
      assert(minrows >= dist.length);
      if (debug) {
        Log.info("Before stratified sampling: " + fr.numRows() + " rows.");
        for (int i=0; i<dist.length;++i) {
          Log.info("Class " + label.domain(i) + ": count: " + dist[i] + " prior: " + (float)dist[i]/fr.numRows());
        }
      }

      // compute sampling ratios to achieve class balance
      if (sampling_ratios == null) {
        sampling_ratios = new float[dist.length];
      }
      assert(sampling_ratios.length == dist.length);
      for (int i=0; i<dist.length;++i) {
        sampling_ratios[i] = ((float)fr.numRows() / label.domain().length) / dist[i] ;
      }

      float inv_scale = Utils.minValue(sampling_ratios); //minority class determines upscaling factor
      long numrows = (long)((float)fr.numRows() / inv_scale + 0.5f); //number of total rows if full upsampling is done
      if (debug) Log.info("Full class balance via oversampling requires approx. " + numrows + " rows.");
      numrows = Math.min(maxrows, numrows); // at most maxrows
      numrows = Math.max(minrows, numrows); // at least minrows
      Utils.mult(sampling_ratios, (float)numrows/fr.numRows()); //adjust the sampling_ratios by the global rescaling factor
      Log.info("Doing stratified sampling to obtain class balance with " + numrows + " total rows.");
    }

    return sampleFrameStratified(fr, label, sampling_ratios, seed, debug);
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
    if (fr == null) return null;
    assert(label.isEnum());
    assert(sampling_ratios != null && sampling_ratios.length == label.domain().length);
    final int labelidx = fr.find(label); //which column is the label?
    assert(labelidx >= 0);

    Frame r = new MRTask2() {
      @Override
      public void map(Chunk[] cs, NewChunk[] ncs) {
        final Random rng = getDeterRNG(seed + cs[0].cidx());
        for (int r = 0; r < cs[0]._len; r++) {
          int label = (int)cs[labelidx].at80(r);
          assert(sampling_ratios.length > label && label >= 0);
          final int sampling_reps = Utils.getPoisson(sampling_ratios[label], rng);
          for (int i = 0; i < ncs.length; i++) {
            for (int j = 0; j < sampling_reps; ++j) {
              ncs[i].addNum(cs[i].at0(r));
            }
          }
        }
      }
    }.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());

    // Confirm the validity of the distribution
    long[] dist = new ClassDist(r.vecs()[labelidx]).doAll(r.vecs()[labelidx]).dist();

    if (debug) {
      long sumdist = Utils.sum(dist);
      Log.info("After stratified sampling: " + sumdist + " rows.");
      for (int i=0; i<dist.length;++i) {
        Log.info("Class " + r.vecs()[labelidx].domain(i) + ": count: " + dist[i]
                + " sampling ratio: " + sampling_ratios[i] + " actual relative frequency: " + (float)dist[i] / sumdist * dist.length);
      }
    }

    // Re-try if we didn't get at least one example from each class
    if (Utils.minValue(dist) == 0) {
      Log.info("Re-doing stratified sampling because not all classes were represented (unlucky draw).");
      return sampleFrameStratified(fr, label, sampling_ratios, seed+1, debug);
    }

    return r;
  }
}
