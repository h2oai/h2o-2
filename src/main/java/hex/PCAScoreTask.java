package hex;

import hex.NewRowVecTask.DataFrame;

import java.util.Arrays;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.Job.ChunkProgressJob;
import water.fvec.*;
import water.fvec.Vec.VectorGroup;

public class PCAScoreTask extends MRTask2<PCAScoreTask> {
  final PCAScoreJob _job;
  final int _nfeat;           // number of cols of the input dataset
  final int _ncomp;           // number of cols of the output dataset
  final double[][] _smatrix;  // small matrix in multiplication

  public PCAScoreTask(PCAScoreJob job, int nfeat, int ncomp, double[][] smatrix) {
    _job = job;
    _nfeat = nfeat;
    _ncomp = ncomp;
    _smatrix = smatrix;
  }

  // Matrix multiplication A * B, where A is a skinny matrix (# rows >> # cols) and B is a
  // small matrix that fitting on a single node. For PCA scoring, the cols of A (rows of B) are
  // the features of the input dataset, while the cols of B are the principal components.
  @Override public void map(Chunk [] chunks) {
    Chunk [] inputs = Arrays.copyOf(chunks, _nfeat);
    NewChunk [] outputs = new NewChunk[_ncomp];

    for(int i = _nfeat; i < chunks.length; ++i) {
      outputs[i-_nfeat] = (NewChunk)chunks[i];
    }

    int rows = inputs[0]._len;
    for(int r = 0; r < rows; r++) {
      for(int c = 0; c < _ncomp; c++) {
       double x = 0;
       for(int d = 0; d < _nfeat; d++)
         x += inputs[d].at0(r)*_smatrix[d][c];
       outputs[c].addNum(x);
      }
    }
    _job.updateProgress(1);
  }

  public static class PCAScoreJob extends ChunkProgressJob {
    public PCAScoreJob(Frame data, Key dataKey, Key destKey) {
      super("PCA Score(" + dataKey.toString() + ")", destKey, data.anyVec().nChunks());
    }

    public boolean isDone() {
      return DKV.get(self()) == null;
    }

    @Override public float progress() {
      ChunkProgress progress = UKV.get(progressKey());
      return (progress != null ? progress.progress() : 0);
    }
  }

  public static Job score(final Frame lmatrix, final double[][] smatrix, int nrow, int ncol, final Key dataKey, final Key destKey) {
    if(smatrix.length != lmatrix._vecs.length)
      throw new RuntimeException("Mismatched dimensions! Left matrix has " + lmatrix._vecs.length + " columns, while right matrix has " + smatrix.length + " rows");

    final int ncomp = Math.min(ncol, smatrix[0].length);
    final int nfeat = Math.min(nrow, lmatrix._vecs.length);

    final PCAScoreJob job = new PCAScoreJob(lmatrix, dataKey, destKey);
    final H2OCountedCompleter fjtask = new H2OCountedCompleter() {
      @Override public void compute2() {
        Vec [] vecs = Arrays.copyOf(lmatrix._vecs, nfeat + ncomp);
        VectorGroup vg = lmatrix._vecs[0].group();
        Key [] keys = vg.addVecs(ncomp);
        for(int i = 0; i < ncomp; i++) {
          vecs[nfeat+i] = new AppendableVec(keys[i]);
        }
        PCAScoreTask tsk = new PCAScoreTask(job, nfeat, ncomp, smatrix).doAll(vecs);
        Vec [] outputVecs = Arrays.copyOfRange(tsk._fr._vecs, nfeat, nfeat + ncomp);
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

  public static Job score(Frame lmatrix, double[][] smatrix, Key dataKey, Key destKey) {
    return score(lmatrix, smatrix, smatrix.length, smatrix[0].length, dataKey, destKey);
  }

  public static Job score(Frame lmatrix, double[][] smatrix, int ncol, Key dataKey, Key destKey) {
    return score(lmatrix, smatrix, smatrix.length, ncol, dataKey, destKey);
  }

  public static Job score(final DataFrame lmatrix, double[][] smatrix, Key destKey) {
    return score(lmatrix._ary.asFrame(), smatrix, lmatrix._ary._key, destKey);
  }

  public static Job score(final DataFrame lmatrix, double[][] smatrix, int ncol, Key destKey) {
    return score(lmatrix._ary.asFrame(), smatrix, ncol, lmatrix._ary._key, destKey);
  }
}
