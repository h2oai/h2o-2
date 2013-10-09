package hex.pca;

import java.util.Arrays;

import water.Job.FrameJob;
import water.*;
import water.api.DocGen;
import water.fvec.*;

public class PCAScore2 extends FrameJob {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "pca_score";

  @API(help = "PCA model to use for scoring")
  PCAModel model;

  @API(help = "Number of principal components", filter = Default.class)
  int num_pc;

  @Override protected void exec() {
    Frame fr = source;
    // TODO: Extract only feature columns in source that match data used to build model!
    int nfeat = model.eigVec.length;
    PCAScoreTask tsk = new PCAScoreTask(this, nfeat, num_pc, model.eigVec, model.params.standardize);
    tsk.doAll(fr);
    Vec [] outputVecs = Arrays.copyOfRange(tsk._fr.vecs(), nfeat, nfeat + num_pc);
    String [] names = new String[num_pc];
    for(int i = 0; i < num_pc; i++) names[i] = "PC" + i;
    Frame f = new Frame(names, outputVecs);
    DKV.put(destination_key, f);
  }

  /*@Override public float progress() {
    ChunkProgress progress = UKV.get(progressKey());
    return (progress != null ? progress.progress() : 0);
  }*/

  public static class PCAScoreTask extends MRTask2<PCAScoreTask> {
    final Job _job;
    double[] _normSub;
    double[] _normMul;
    final boolean _standardize;
    final int _nfeat;           // number of cols of the input dataset
    final int _ncomp;           // number of cols of the output dataset
    final double[][] _smatrix;  // small matrix in multiplication

    public PCAScoreTask(Job job, int nfeat, int ncomp, double[][] smatrix, boolean standardize) {
      _job = job;
      _normSub = null;
      _normMul = null;
      _standardize = standardize;
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
           // x += chunks[d].at0(r)*_smatrix[d][c];
           x += (chunks[d].at0(r) - _normSub[d])*_normMul[d]*_smatrix[d][c];
         chunks[_nfeat+c].set0(r,x);
        }
      }
      //_job.updateProgress(1);
    }

    @Override public PCAScoreTask dfork(Frame fr) {
        final Vec [] vecs = fr.vecs();
        _normSub = MemoryManager.malloc8d(_nfeat);
        _normMul = MemoryManager.malloc8d(_nfeat); Arrays.fill(_normMul, 1);

        if(_standardize) {
          for(int i = 0; i < _nfeat; ++i) {
          _normSub[i] = vecs[i].mean();
          _normMul[i] = 1.0/vecs[i].sigma();
          }
        }
      return super.dfork(fr);
    }
  }
 }
