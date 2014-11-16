package hex.la;

import water.*;
import water.fvec.*;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.NewChunk.Value;

import java.util.Arrays;
import java.util.Iterator;

/**
* Created by tomasnykodym on 11/13/14.
*/
public class DMatrix  {

  public static Frame transpose(Frame src){
    int nchunks = (int)Math.min(src.numCols(),4*H2O.NUMCPUS*H2O.CLOUD.size());
    long [] espc = new long[nchunks+1];
    int rpc = (int)(src.numCols() / nchunks);
    int rem = (int)(src.numCols() % nchunks);
    Arrays.fill(espc, rpc);
    for (int i = 0; i < rem; ++i) ++espc[i];
    long sum = 0;
    for (int i = 0; i < espc.length; ++i) {
      long s = espc[i];
      espc[i] = sum;
      sum += s;
    }
    return transpose(src, new Frame(new Vec(Vec.newKey(),espc).makeZeros((int)src.numRows())));
  }

  public static Frame transpose(Frame src, Frame tgt){
    if(src.numRows() != tgt.numCols() || src.numCols() != tgt.numRows())
      throw new IllegalArgumentException("dimension do not match!");
    for(Vec v:src.vecs()) {
      if (v.isEnum())
        throw new IllegalArgumentException("transpose can only be applied to all-numeric frames (representing a matrix)");
      if(v.length() > 1000000)
        throw new IllegalArgumentException("too many rows, transpose only works for frames with < 1M rows.");
    }
    new TransposeTsk(tgt).doAll(src);
    return tgt;
  }

  public static Frame mmul(Frame x, Frame y) {
    if(x.numCols() != y.numRows())
      throw new IllegalArgumentException("dimensions do not match! x.numcols = " + x.numCols() + ", y.numRows = " + y.numRows());
    Frame z = new Frame(y.anyVec().makeZeros(y.numCols()));
    transpose(x,z);
    z.reloadVecs();

    return z;
  }
  private static class TransposeTsk extends MRTask2<TransposeTsk> {
    final Frame _tgt;

    public TransposeTsk(Frame tgt){ _tgt = tgt;}
    public void map(Chunk [] chks) {
      final Frame tgt = _tgt;
      long [] espc = tgt.anyVec()._espc;
      NewChunk [] tgtChunks = new NewChunk[chks[0]._len];
      int colStart = (int)chks[0]._start;
      for(int i = 0; i < espc.length-1; ++i) {
        for(int j = 0; j < tgtChunks.length; ++j)
          tgtChunks[j] = new NewChunk(tgt.vec(j + colStart), i);
        for (int c = ((int) espc[i]); c < (int) espc[i + 1]; ++c) {
          NewChunk nc = chks[c].inflate();
          Iterator<Value> it = nc.values();
          while (it.hasNext()) {
            Value v = it.next();
            NewChunk t  = tgtChunks[v.rowId0()];
            t.addZeros(c-(int)espc[i] - t.len());
            v.add2Chunk(t);
          }
        }
        for(NewChunk t:tgtChunks) {
          t.addZeros((int)(espc[i+1] - espc[i]) - t.len());
          t.close(_fs);
        }
      }
    }
  }
}
