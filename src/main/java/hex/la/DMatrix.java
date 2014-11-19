package hex.la;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCallback;
import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.fvec.*;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.NewChunk.Value;
import water.util.Utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
* Created by tomasnykodym on 11/13/14.
*/
public class DMatrix  {

  public static Frame transpose(Frame src){
    int nchunks = Math.min(src.numCols(),4*H2O.NUMCPUS*H2O.CLOUD.size());
    long [] espc = new long[nchunks+1];
    int rpc = (src.numCols() / nchunks);
    int rem = (src.numCols() % nchunks);
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

  public static class TransposeTsk extends MRTask2<TransposeTsk> {
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

  public static class MatrixMulStats extends Iced {
    public final Key jobKey;
    public final long chunksTotal;
    public final long _startTime;
    public long lastUpdateAt;
    public long chunksDone;
    public long size;
    public int [] chunkTypes = new int[0];
    public long [] chunkCnts = new long[0];

    public MatrixMulStats(long n, Key jobKey){chunksTotal = n; _startTime = System.currentTimeMillis(); this.jobKey = jobKey;}

    public float progress(){ return (float)((double)chunksDone/chunksTotal);}
  }

  /**
   * Compute x %*% y for two matrices x,y. Assuming reasonable dimensions/sparsity
   *

   */
  public static class MatrixMulJob extends Job {
    final Key _dstKey;
    Frame _x;
    Frame _y;
    Frame _z; // result
    public MatrixMulJob(Key jobKey, Key dstKey, Frame x, Frame y){
      super(jobKey,dstKey);
      if(x.numCols() != y.numRows())
        throw new IllegalArgumentException("dimensions do not match! x.numcols = " + x.numCols() + ", y.numRows = " + y.numRows());
      _dstKey = dstKey;
      _x = x;
      _y = y;
      description = (_x._key != null && _y._key != null)?(_x._key + " %*% " + _y._key):"matrix multiplication";
    }
    @Override public float progress(){
      MatrixMulStats p = DKV.get(_dstKey).get();
      return p.progress();
    }

    @Override public MatrixMulJob fork(){
      Futures fs = new Futures();
      DKV.put(_dstKey, new MatrixMulStats(_x.anyVec().nChunks()* _y.numCols(),self()),fs);
      fs.blockForPending();
      start(new H2OEmptyCompleter());
      H2O.submitTask(new MatrixMulTsk(new H2OCallback<MatrixMulTsk>(_fjtask) { // cleanup
        @Override
        public void callback(MatrixMulTsk mmt) {
          _z = mmt._z;
          DKV.put(_dstKey,_z);
          remove();
        }
        @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
          MatrixMulJob.this.cancel(ex);
          return true;
        }
      },_dstKey, _x,_y));
      return this;
    }
  }
  public static class MatrixMulTsk extends H2OCountedCompleter {
    final Frame _x;
    Frame _y;
    Frame _z;
    final Key _progressKey;
    AtomicInteger _cntr;
    public MatrixMulTsk(H2OCountedCompleter cmp, Key progressKey, Frame x, Frame y) {
      super(cmp);
      if(x.numCols() != y.numRows())
        throw new IllegalArgumentException("dimensions do not match! x.numcols = " + x.numCols() + ", y.numRows = " + y.numRows());
      _x = x;
      _y = y;
      _progressKey = progressKey;
    }

    private class Callback extends H2OCallback{
      public Callback(){super(MatrixMulTsk.this);}
      @Override
      public void callback(H2OCountedCompleter h2OCountedCompleter) {
        int i = _cntr.incrementAndGet();
        if(i < _y.numCols())
          new VecTsk(new Callback(), _progressKey, _x, _y.vec(i), _z.vec(i)).fork();
      }
    }

    @Override
    public void compute2() {
      _z = new Frame(_x.anyVec().makeZeros(_y.numCols()));
      // first rebalance y to single chunk per column
      final Key k = Key.makeSystem(Key.make().toString());
      // todo, should really be minimal size instead of 1 here, i.e. it should be max(size(vec)/max_chunk_size) over all vecs
      new RebalanceDataSet(_y,k,1).fork().join();
      _y = DKV.get(k).get();
      int maxP = 100; // todo: some heuristic to compute better maxP
      _cntr = new AtomicInteger(maxP-1);
      addToPendingCount(_y.numCols()-1);
      for(int i = 0; i < Math.min(_y.numCols(),maxP); ++i)
        new VecTsk(new Callback(), _progressKey, _x, _y.vec(i), _z.vec(i)).fork();
    }
    @Override public void onCompletion(CountedCompleter caller) { _y.delete();}
  }
  // to be invoked from R expression
  public static Frame mmul(Frame x, Frame y) {
    MatrixMulJob mmj = new MatrixMulJob(Key.make("mmul"),Key.make("mmulProgress"),x,y);
    mmj.fork()._fjtask.join();
    DKV.remove(mmj._dstKey); // do not leave garbage in KV
    return mmj._z;
  }

  private static class GetNonZerosTsk extends MRTask2<GetNonZerosTsk>{
    final int _maxsz;
    int [] _res;
    public GetNonZerosTsk(H2OCountedCompleter cmp){super(cmp);_maxsz = 100000;}
    public GetNonZerosTsk(H2OCountedCompleter cmp, int maxsz){super(cmp); _maxsz = maxsz;}
    @Override public void map(Chunk c){
      _res = c.nonzeros();
      if(_res.length > _maxsz)
        throw new RuntimeException("too many nonzeros! found at least " + _res.length + " nonzeros.");
    }
    @Override public void reduce(GetNonZerosTsk gnz){
      if(_res.length + gnz._res.length > _maxsz)
        throw new RuntimeException("too many nonzeros! found at least " + (_res.length + gnz._res.length > _maxsz) + " nonzeros.");
      _res = Utils.mergeSort(_res, gnz._res);
    }

  }
  // Compute one vec of the output
  private static class VecTsk extends H2OCountedCompleter {
    final Key _progressKey;
    final Frame _x;
    final Vec _yVec;
    final Vec _zVec;
    public VecTsk(H2OCountedCompleter cmp, Key progressKey, Frame x, Vec yVec, Vec zVec){
      super(cmp);
      _progressKey = progressKey;
      _x = x;
      _yVec = yVec;
      _zVec = zVec;
    }

    @Override
    public void compute2() {
      addToPendingCount(1);
      // first get nonzeros of y (only vecs we will care about)
      new GetNonZerosTsk(new H2OCallback<GetNonZerosTsk>(this) {
        @Override
        public void callback(GetNonZerosTsk gnz) {
          new MatrixMulTsk3(VecTsk.this,_progressKey, gnz._res,_yVec).asyncExec(Utils.append(_x.vecs(gnz._res),_zVec));
        }
      }).asyncExec(_yVec);
    }
  }

  private static class MatrixMulTsk3 extends MRTask2<MatrixMulTsk3> {
    final int [] _nzs;
    final Vec _y;
    final Key _progressKey;
    public MatrixMulTsk3(H2OCountedCompleter cmp, Key progressKey, int [] nzs, Vec y){
      super(cmp);
      _progressKey = progressKey;
      _nzs = nzs;
      _y = y;
    }
    @Override public void map(Chunk [] chks) {
      Chunk yChunk = _y.chunkForChunkIdx(0);
      Chunk zChunk = chks[chks.length-1];
      double [] res = MemoryManager.malloc8d(chks[0]._len);
      for(int i = 0; i < _nzs.length; ++i) {
        final double yVal = yChunk.at0(_nzs[i]);
        final Chunk xChunk = chks[i];
        for(int k = xChunk.nextNZ(-1); k < res.length; ++k)
          res[k] += yVal*xChunk.at0(k);
      }
      zChunk.setAll(res);
      new UpdateProgress(zChunk.modifiedChunk()._mem.length,zChunk.modifiedChunk().frozenType()).fork(_progressKey);
    }
  }

  private static class UpdateProgress extends TAtomic<MatrixMulStats> {

    final int _chunkSz;
    final int _chunkType;

    public UpdateProgress(int sz, int type) {
      _chunkSz = sz;
      _chunkType = type;
    }

    @Override
    public MatrixMulStats atomic(MatrixMulStats old) {
      old.chunkCnts = old.chunkCnts.clone();
      int j = -1;
      for(int i = 0; i < old.chunkTypes.length; ++i) {
        if(_chunkType == old.chunkTypes[i]) {
          j = i;
          break;
        }
      }
      if(j == -1) {
        old.chunkTypes = Arrays.copyOf(old.chunkTypes,old.chunkTypes.length+1);
        old.chunkCnts = Arrays.copyOf(old.chunkCnts,old.chunkCnts.length+1);
        old.chunkTypes[old.chunkTypes.length-1] = _chunkType;
        j = old.chunkTypes.length-1;
      }
      old.chunksDone++;
      old.chunkCnts[j]++;
      old.lastUpdateAt = System.currentTimeMillis();
      old.size += _chunkSz;
      return old;
    }
  }
}
