package hex;

import hex.NewRowVecTask.DataFrame;

import java.util.Arrays;

import Jama.Matrix;
import water.*;
import water.fvec.*;
import water.fvec.Vec.VectorGroup;

public class PCAScoreTask extends MRTask2<PCAScoreTask> {
  final int _nfeat;           // number of cols of the input dataset
  final int _ncomp;           // number of cols of the output dataset
  final double[][] _smatrix;  // small matrix in multiplication

  public PCAScoreTask(int nfeat, int ncomp, double[][] smatrix) {
    _nfeat = nfeat;
    _ncomp = ncomp;
    _smatrix = smatrix;
  }

  // Matrix multiplication A * B, where A is a skinny matrix (# rows >> # cols) and B is a
  // small matrix that fitting on a single node. For PCA scoring, the cols of A (rows of B) are
  // the features of the input dataset, while the cols of B are the principal components.
  @Override public void map(Chunk ... chunks) {
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
         x += inputs[d].at(r)*_smatrix[d][c];
       outputs[c].addNum(x);
     }
    }
  }

  public static Frame score(Frame lmatrix, double[][] smatrix, int nrow, int ncol, Key destKey) {
    if(smatrix.length != lmatrix._vecs.length)
      throw new RuntimeException("Mismatched dimensions! Left matrix has " + lmatrix._vecs.length + " columns, while right matrix has " + smatrix.length + " rows");

    int ncomp = Math.min(ncol, smatrix[0].length);
    int nfeat = Math.min(nrow, lmatrix._vecs.length);

    Vec [] vecs = Arrays.copyOf(lmatrix._vecs, nfeat + ncomp);
    VectorGroup vg = lmatrix._vecs[0].group();
    Key [] keys = vg.addVecs(ncomp);
    for(int i = 0; i < ncomp; i++) {
      vecs[nfeat+i] = new AppendableVec(keys[i]);
    }
    PCAScoreTask tsk = new PCAScoreTask(nfeat, ncomp, smatrix).doAll(vecs);
    Vec [] outputVecs = Arrays.copyOfRange(tsk._fr._vecs, nfeat, nfeat + ncomp);

    String [] names = new String[ncomp];
    for(int i = 0; i < ncomp; i++) names[i] = "PC" + i;
    Frame f = new Frame(names, outputVecs);
    DKV.put(destKey, f);
    return f;
  }

  public static Frame score(Frame lmatrix, double[][] smatrix, int ncol, Key destKey) {
    return score(lmatrix, smatrix, smatrix.length, ncol, destKey);
  }

  public static Frame score(Frame lmatrix, double[][] smatrix, Key destKey) {
    return score(lmatrix, smatrix, smatrix.length, smatrix[0].length, destKey);
  }

  public static Frame score(final DataFrame lmatrix, double[][] smatrix, Key destKey) {
    return score(lmatrix._ary.asFrame(), smatrix, destKey);
  }
}
