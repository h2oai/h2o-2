package hex;

import hex.NewRowVecTask.DataFrame;

import java.util.Arrays;

import junit.framework.Assert;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.fvec.*;
import water.fvec.Vec.VectorGroup;

public abstract class PCAScoreTask {
  public static class PCAScoreJob extends ChunkProgressJob {
    public PCAScoreJob(Frame data, Key dataKey, Key destKey, boolean standardize) {
      super("PCAScore(" + dataKey.toString() + ")", destKey, standardize ? 2*data.anyVec().nChunks() : data.anyVec().nChunks());
    }

    public boolean isDone() {
      return DKV.get(self()) == null;
    }

    @Override public float progress() {
      ChunkProgress progress = UKV.get(progressKey());
      return (progress != null ? progress.progress() : 0);
    }
  }

  public static class StandardizeTask extends MRTask2<StandardizeTask> {
    final PCAScoreJob _job;
    final double[] _normSub;
    final double[] _normMul;

    public StandardizeTask(PCAScoreJob job, double[] normSub, double[] normMul) {
      _job = job;
      _normSub = normSub;
      _normMul = normMul;
    }

    @Override public void map(Chunk [] chunks) {
      int ncol = _normSub.length;
      int rows = chunks[0]._len;
      for(int c = 0; c < ncol; c++) {
        for(int r = 0; r < rows; r++) {
          double x = chunks[c].at0(r);
          x -= _normSub[c];
          x *= _normMul[c];
          chunks[ncol+c].set(r,x);
        }
      }
      if(_job != null) _job.updateProgress(1);
    }
  }

  public static Frame standardize(Frame data, double[] normSub, double[] normMul) {
    int ncol = normSub.length;
    Vec [] vecs = Arrays.copyOf(data.vecs(), 2*ncol);
    for(int i = 0; i < ncol; i++)
      vecs[ncol+i] = vecs[0].makeZero();
    StandardizeTask tsk = new StandardizeTask(null, normSub, normMul).doAll(vecs);
    Vec [] outputVecs = Arrays.copyOfRange(tsk._fr.vecs(), ncol, 2*ncol);
    return new Frame(data.names(), outputVecs);
  }

  public static Frame standardize(final DataFrame data) {
    // Extract only the columns in the associated model
    Frame subset = data.modelAsFrame();
    Assert.assertEquals(subset.numCols(), data._normSub.length);
    return standardize(subset, data._normSub, data._normMul);
  }

  public static class ScoreTask extends MRTask2<ScoreTask> {
    final PCAScoreJob _job;
    final int _nfeat;           // number of cols of the input dataset
    final int _ncomp;           // number of cols of the output dataset
    final double[][] _smatrix;  // small matrix in multiplication

    public ScoreTask(PCAScoreJob job, int nfeat, int ncomp, double[][] smatrix) {
      _job = job;
      _nfeat = nfeat;
      _ncomp = ncomp;
      _smatrix = smatrix;
    }

    // Matrix multiplication A * B, where A is a skinny matrix (# rows >> # cols) and B is a
    // small matrix that fitting on a single node. For PCA scoring, the cols of A (rows of B) are
    // the features of the input dataset, while the cols of B are the principal components.
    @Override public void map(Chunk [] chunks) {
      int rows = chunks[0]._len;
      for(int r = 0; r < rows; r++) {
        for(int c = 0; c < _ncomp; c++) {
         double x = 0;
         for(int d = 0; d < _nfeat; d++)
           x += chunks[d].at0(r)*_smatrix[d][c];
         chunks[_nfeat+c].set(r,x);
        }
      }
      _job.updateProgress(1);
    }
  }

  public static Job mult(final Frame lmatrix, final double[][] smatrix, int nrow, int ncol, final Key dataKey, final Key destKey) {
    if(smatrix.length != lmatrix.numCols())
      throw new RuntimeException("Mismatched dimensions! Left matrix has " + lmatrix.numCols() + " columns, while right matrix has " + smatrix.length + " rows");

    final int ncomp = Math.min(ncol, smatrix[0].length);
    final int nfeat = Math.min(nrow, lmatrix.numCols());

    final PCAScoreJob job = new PCAScoreJob(lmatrix, dataKey, destKey, false);
    final H2OCountedCompleter fjtask = new H2OCountedCompleter() {
      @Override public void compute2() {
        Vec [] vecs = Arrays.copyOf(lmatrix.vecs(), nfeat + ncomp);
        for(int i = 0; i < ncomp; i++)
          vecs[nfeat+i] = vecs[0].makeZero();

        ScoreTask tsk = new ScoreTask(job, nfeat, ncomp, smatrix).doAll(vecs);
        Vec [] outputVecs = Arrays.copyOfRange(tsk._fr.vecs(), nfeat, nfeat + ncomp);
        String [] names = new String[ncomp];
        for(int i = 0; i < ncomp; i++) names[i] = "PC" + i;
        Frame f = new Frame(names, outputVecs);
        DKV.put(destKey, f);
        job.remove();
      }
    };
    H2O.submitTask(job.start(fjtask));
    return job;
  }

  public static Job mult(Frame lmatrix, double[][] smatrix, Key dataKey, Key destKey) {
    return mult(lmatrix, smatrix, smatrix.length, smatrix[0].length, dataKey, destKey);
  }

  public static Job mult(Frame lmatrix, double[][] smatrix, int ncol, Key dataKey, Key destKey) {
    return mult(lmatrix, smatrix, smatrix.length, ncol, dataKey, destKey);
  }

  public static Job mult(final DataFrame lmatrix, double[][] smatrix, Key destKey) {
    return mult(lmatrix._ary.asFrame(), smatrix, lmatrix._ary._key, destKey);
  }

  public static Job mult(final DataFrame lmatrix, double[][] smatrix, int ncol, Key destKey) {
    return mult(lmatrix._ary.asFrame(), smatrix, ncol, lmatrix._ary._key, destKey);
  }

  public static Job score(final DataFrame data, final double[][] eigvec, final int nrow, final int ncol, Key dataKey, final Key destKey, final boolean standardize) {
    if(data._modelDataMap.length != eigvec.length)
      throw new RuntimeException("Mismatched dimensions! Model matrix has " + data._modelDataMap.length + " features, while eigenvector matrix has " + eigvec.length + " features");

    final Frame origModel = data.modelAsFrame();
    final PCAScoreJob job = new PCAScoreJob(origModel, dataKey, destKey, standardize);

    final H2OCountedCompleter fjtask = new H2OCountedCompleter() {
      @Override public void compute2() {
        Frame lmatrix = origModel;

        // Note: Standardize automatically removes columns not in modelDataMap
        if(standardize) {
          // lmatrix = standardize(origModel, data._normSub, data._normMul);
          int ncol = origModel.numCols();
          Vec [] vecs = Arrays.copyOf(origModel.vecs(), 2*ncol);
          for(int i = 0; i < ncol; i++)
            vecs[ncol+i] = vecs[0].makeZero();
          StandardizeTask tsk = new StandardizeTask(job, data._normSub, data._normMul).doAll(vecs);
          Vec [] outputVecs = Arrays.copyOfRange(tsk._fr.vecs(), ncol, 2*ncol);
          lmatrix = new Frame(origModel.names(), outputVecs);
        }

        final int ncomp = Math.min(ncol, eigvec[0].length);
        final int nfeat = Math.min(nrow, lmatrix.numCols());

        Vec [] vecs = Arrays.copyOf(lmatrix.vecs(), nfeat + ncomp);
        VectorGroup vg = lmatrix.vecs()[0].group();
        Key [] keys = vg.addVecs(ncomp);
        for(int i = 0; i < ncomp; i++) {
          vecs[nfeat+i] = vecs[0].makeZero();
        }

        ScoreTask tsk = new ScoreTask(job, nfeat, ncomp, eigvec).doAll(vecs);
        Vec [] outputVecs = Arrays.copyOfRange(tsk._fr.vecs(), nfeat, nfeat + ncomp);
        String [] names = new String[ncomp];
        for(int i = 0; i < ncomp; i++) names[i] = "PC" + i;
        Frame f = new Frame(names, outputVecs);
        DKV.put(destKey, f);
        job.remove();
      }
    };
    H2O.submitTask(job.start(fjtask));
    return job;
  }

  public static Job score(final DataFrame original, final double[][] smatrix, final int ncol, Key dataKey, final Key destKey, final boolean standardize) {
    return score(original, smatrix, smatrix.length, ncol, dataKey, destKey, standardize);
  }
}
