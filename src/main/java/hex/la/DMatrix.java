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
 *
 * Distributed matrix operations such as (sparse) multiplication and transpose.
*/
public class DMatrix  {

  /**
   * Transpose the Frame as if it was a matrix (i.e. rows become coumns).
   * Must be all numeric, currently will fail if there are too many rows ( >= ~.5M).
   * Result will be put into a new Vectro Group and will be balanced so that each vec will have
   * (4*num cpus in the cluster) chunks.
   *
   * @param src
   * @return
   */
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

  /**
   * Transpose the Frame as if it was a matrix (rows <-> columns).
   * Must be all numeric, will fail if there are too many rows ( >= ~.5M).
   *
   * Result is made to be compatible (i.e. the same vector group and chunking) with the target frame.
   *
   * @param src
   * @return
   */
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

  /**
   * (MR)Task performing the matrix transpose.
   * It is to be applied to the source frame.
   * Target frame must be created up front (e.g. via Vec.makeZeros() call)
   * and passed in as an argument.
   *
   * Task will utilize sparsity and will preserve compression if possible
   * (compression may differ because of switching from column compressed to row-compressed form)
   */
  public static class TransposeTsk extends MRTask2<TransposeTsk> {
    final Frame _tgt; // Target dataset, should be created up front, e.g. via Vec.makeZeros(n) call.
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
        for(NewChunk t:tgtChunks) { // finalize the target chunks and close them
          t.addZeros((int)(espc[i+1] - espc[i]) - t.len());
          t.close(_fs);
        }
      }
    }
  }


  /**
   * Info about matrix multiplication currently in progress.
   *
   * Contains runtime and (already computed)chunks stats
   *
   */
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
   * Respects sparsity and wil preserve integer compression (all floats expanded to doubles though).
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
    final transient Frame _x;
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

    @Override
    public void compute2() {
      _z = new Frame(_x.anyVec().makeZeros(_y.numCols()));
      int total_cores = H2O.CLOUD.size()*H2O.NUMCPUS;
      int chunksPerCol = _y.anyVec().nChunks();
      int maxP = 8*total_cores/chunksPerCol;
      _cntr = new AtomicInteger(maxP-1);
      addToPendingCount(2*_y.numCols()-1);
      for(int i = 0; i < Math.min(_y.numCols(),maxP); ++i)
       forkVecTask(i);
    }

    private void forkVecTask(final int i) {
      new GetNonZerosTsk(new H2OCallback<GetNonZerosTsk>(this) {
        @Override
        public void callback(GetNonZerosTsk gnz) {
          new VecTsk(new Callback(), _progressKey, gnz._vals).asyncExec(Utils.append(_x.vecs(gnz._idxs), _z.vec(i)));
        }
      }).asyncExec(_y.vec(i));
    }
    private class Callback extends H2OCallback{
      public Callback(){super(MatrixMulTsk.this);}
      @Override
      public void callback(H2OCountedCompleter h2OCountedCompleter) {
        int i = _cntr.incrementAndGet();
        if(i < _y.numCols())
          forkVecTask(i);
      }
    }
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
    int     [] _idxs;
    double  [] _vals;
    public GetNonZerosTsk(H2OCountedCompleter cmp){super(cmp);_maxsz = 100000;}
    public GetNonZerosTsk(H2OCountedCompleter cmp, int maxsz){super(cmp); _maxsz = maxsz;}
    @Override public void map(Chunk c){
      int istart = (int)c._start;
      assert (c._start + c._len) == (istart + c._len);
      final int n = c.sparseLen();
      _idxs = MemoryManager.malloc4(n);
      _vals = MemoryManager.malloc8d(n);
      int j = 0;
      for(int i = c.nextNZ(-1); i < c._len; i = c.nextNZ(i),++j) {
        _idxs[j] = i + istart;
        _vals[j] = c.at0(i);
      }
      assert j == n;
      if(_idxs.length > _maxsz)
        throw new RuntimeException("too many nonzeros! found at least " + _idxs.length + " nonzeros.");
    }
    @Override public void reduce(GetNonZerosTsk gnz){
      if(_idxs.length + gnz._idxs.length > _maxsz)
        throw new RuntimeException("too many nonzeros! found at least " + (_idxs.length + gnz._idxs.length > _maxsz) + " nonzeros.");
      int [] idxs = MemoryManager.malloc4(_idxs.length + gnz._idxs.length);
      double [] vals = MemoryManager.malloc8d(_vals.length + gnz._vals.length);
      Utils.sortedMerge(_idxs,_vals,gnz._idxs,gnz._vals,idxs,vals);
      _idxs = idxs;
      _vals = vals;
    }
  }
  // compute single vec of the output in matrix multiply
  private static class VecTsk extends MRTask2<VecTsk> {
    double [] _y;
    Key _progressKey;
    public VecTsk(H2OCountedCompleter cmp, Key progressKey, double [] y){
      super(cmp);
      _progressKey = progressKey;
      _y = y;
    }

    @Override public void setupLocal(){_fr.lastVec().preWriting();}
    @Override public void map(Chunk [] chks) {
      Chunk zChunk = chks[chks.length-1];
      double [] res = MemoryManager.malloc8d(chks[0]._len);
      for(int i = 0; i < _y.length; ++i) {
        final double yVal = _y[i];
        final Chunk xChunk = chks[i];
        for (int k = xChunk.nextNZ(-1); k < res.length; k = xChunk.nextNZ(k))
          res[k] += yVal * xChunk.at0(k);
      }
      int [] nzs = MemoryManager.malloc4(res.length+1);
      int j = 0;
      for(int i = 0; i < res.length; ++i)
        if(res[i] != 0)
          nzs[j++] = i;
      // NOTE: not using NewChunk.compress here as it was 1) too slow 2) result was too big, we want to treat chunks as sparse with much smaller sparse-ratio here
      // (maybe update NewChunk to have different min-sparsity for double chunks)?
      Chunk modChunk = (j < (res.length >> 1))?new CXDChunk(zChunk._start,res,nzs,j):new C8DChunk(zChunk._start,res);
      new UpdateProgress(modChunk._mem.length,modChunk.frozenType()).fork(_progressKey);
      DKV.put(zChunk._vec.chunkKey(zChunk.cidx()),modChunk,_fs);
    }
    @Override public void closeLocal(){
      _y = null; // drop inputs 
      _progressKey = null;
    }

    @Override public void postGlobal(){
      _fr.lastVec().postWrite();
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
