package hex.la;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.H2O.H2OEmptyCompleter;
import water.fvec.*;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.NewChunk.Value;
import water.util.Log;
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

  public static class MatrixMulStats extends Iced {
    public final long chunksTotal;
    public final long _startTime;
    public long lastUpdateAt;
    public long chunksDone;
    public long size;
    public int [] chunkTypes = new int[0];
    public long [] chunkCnts = new long[0];

    public MatrixMulStats(long n){chunksTotal = n; _startTime = System.currentTimeMillis();}

    public float progress(){ return (float)((double)chunksDone/chunksTotal);}

  }
  public static Frame mmul(Frame x, Frame y) {
    final Key progressKey = Key.make();
    int minChunks = Math.max(H2O.CLOUD.size(),H2O.NUMCPUS);
    int maxChunks = 2*minChunks;
    int optChunks = (maxChunks + minChunks) >> 1;
    if(y.anyVec().nChunks() > maxChunks || y.anyVec().nChunks() < minChunks) {
      Log.info("rebalancing frame from " + y.anyVec().nChunks() + " chunks to " + optChunks);
      y.replaceVecs(RebalanceDataSet.rebalanceAndReplace(optChunks, 100, y.vecs()));
    }
    Frame z = new Frame(x.anyVec().makeZeros(y.numCols()));
    DKV.put(progressKey, new MatrixMulStats(z.vecs().length*z.anyVec().nChunks()));

    Job j = new Job(Key.make(),progressKey){
      @Override public float progress(){
        MatrixMulStats p = DKV.get(progressKey).get();
        return p.progress();
      }
    };
    j.description = (x._key != null && y._key != null)?(x._key + " %*% " + y._key):"matrix multiplication";
    if(x.numCols() != y.numRows())
      throw new IllegalArgumentException("dimensions do not match! x.numcols = " + x.numCols() + ", y.numRows = " + y.numRows());
    j.start(new H2OEmptyCompleter());
    // make the target frame which is compatible with left hand side (same number of rows -> keep it's vector group and espc)
    // make transpose co-located with y
    x = transpose(x,new Frame(y.anyVec().makeZeros((int)x.numRows())));
    x.reloadVecs();
    new MatrixMulTsk2(x,y,progressKey).doAll(z);
    z.reloadVecs();
    x.delete();
    DKV.remove(j.dest());
    j.remove();
    return z;
  }

  // second version of matrix multiply -> no global transpose, just locally transpose chunks (rows compressed instead of column compressed)
  public static Frame mmul2(Frame x, Frame y) {
    if(x.numCols() != y.numRows())
      throw new IllegalArgumentException("dimensions do not match! x.numcols = " + x.numCols() + ", y.numRows = " + y.numRows());
    // make the target frame which is compatible with left hand side (same number of rows -> keep it's vector group and espc)
    Frame z = new Frame(x.anyVec().makeZeros(y.numCols()));
    new MatrixMulTsk(y).doAll(Utils.append(x.vecs(),z.vecs()));
    z.reloadVecs();
    return z;
  }

  /**
   * Matrix multiplication task which takes input of two matrices, X and Y and produces matrix X %*% Y.
   */
  public static class MatrixMulTsk extends MRTask2<MatrixMulTsk> {
    private final Frame  _Y;

    public MatrixMulTsk(Frame Y) {
      _Y = Y;
    }

    public void map(Chunk [] chks) {
      // split to input/output (can't use NewChunks/outputFrame here, writing to chunks in forked off task)
      Chunk [] ncs = Arrays.copyOfRange(chks,chks.length-_Y.numCols(),chks.length);
      chks = Arrays.copyOf(chks,chks.length-_Y.numCols());
      NewChunk [] urows = new NewChunk[chks[0]._len]; // uncompressed rows
      for(int i = 0; i < urows.length; ++i)
        urows[i] = new NewChunk(null,-1,0);
      for(int i = 0; i < chks.length; ++i) {
        Chunk c = chks[i];
        NewChunk nc = c.inflate();
        Iterator<Value> it = nc.values();
        while(it.hasNext()) {
          Value v = it.next();
          int ri = v.rowId0();
          urows[ri].addZeros(i - urows[ri]._len);
          v.add2Chunk(urows[ri]);
        }
      }
      Chunk [] crows = new Chunk[urows.length];
      for(int i = 0; i < urows.length; ++i) {
        urows[i].addZeros(chks.length - urows[i]._len);
        crows[i] = urows[i].compress();
        urows[i] = null;
      }
      // got transposed chunks...now do the multiply over y
      addToPendingCount(1);
      new ChunkMulTsk(this,crows,ncs,_fs).asyncExec(_Y);
    }
  }


  private static class Rows extends Iced {
    Chunk [] _rows; // compressed rows of a chunk of X
    Rows(Chunk [] chks){_rows = chks;}

    @Override
    public AutoBuffer write(AutoBuffer ab) {
      ab.put4(_rows.length);
      for(int i = 0; i < _rows.length; ++i) {
        ab.put4(_rows[i].frozenType());
        ab.putA1(_rows[i]._mem);
      }
      return ab;
    }

    @Override public Rows read(AutoBuffer ab){
      _rows = new Chunk[ab.get4()];
      for(int i = 0; i < _rows.length; ++i)
        (_rows[i] = (Chunk)TypeMap.newFreezable(ab.get4())).read(new AutoBuffer(ab.getA1()));
      return this;
    }
  }

  private static class ChunkMulTsk extends MRTask2<ChunkMulTsk> {
    final transient Chunk [] _ncs;
    final transient Futures _fs;
    Rows _rows;

    public ChunkMulTsk(H2OCountedCompleter cmp, Chunk[] rows, Chunk[] ncs, Futures fs) {
      super(cmp);
      _fs = fs;
      _rows = new Rows(rows); _ncs = ncs;
    }

    double [][] _res;

    @Override public void map(Chunk [] chks){
      final Chunk [] rows = _rows._rows;
      _res = new double [rows.length][];
      for(int i = 0; i < _res.length; ++i)
        _res[i] = MemoryManager.malloc8d(chks.length);
      final int off = (int)chks[0]._start;
      assert off == chks[0]._start;
      for(int i = 0; i < rows.length; ++i) {
        final Chunk rc = rows[i];
        for(int j = 0; j < chks.length; ++j) {
          final Chunk cc = chks[j];
          for(int k = 0; k < chks[j]._len; k = chks[j].nextNZ(k)) {
            _res[i][j] += rc.at0(k+off) * cc.at0(k);
          }
        }
      }
    }
    @Override protected void closeLocal(){ _rows = null;}
    @Override public void reduce(ChunkMulTsk m) {
      for (int i = 0; i < _res.length; ++i)
        Utils.add(_res[i], m._res[i]);
    }

    @Override public void postGlobal(){
      for(int i = 0; i < _res.length; ++i)
        for(int j = 0; j < _res[i].length; ++j)
          _ncs[j].set0(i,_res[i][j]);
      for(Chunk c:_ncs)
        c.close(c.cidx(),_fs);

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
  /**
   * Matrix multiplication task which takes input of two matrices, t(X) and Y and produces matrix X %*% Y.
   */
  public static class MatrixMulTsk2 extends MRTask2<MatrixMulTsk> {
    private final Frame _X, _Y;
    final Key _progressKey;
    public MatrixMulTsk2(Frame X, Frame Y, Key progressKey) {
      _X = X;
      _Y = Y;
      _progressKey = progressKey;
    }
    private static final int MAX_P = 10;

    private transient AtomicInteger _j;
    private class Cmp extends H2OCountedCompleter {
      final int _i;
      final Chunk [] chks;
      final Vec [] xs;
      public Cmp(int i, Chunk [] chks, Vec [] xs){
        super(MatrixMulTsk2.this);
        _i = i;
        this.chks = chks;
        this.xs = xs;
      }

      @Override
      public void compute2() {throw H2O.fail("do not call!");}


      @Override public void onCompletion(CountedCompleter caller){
        VecMulTsk tsk = (VecMulTsk)caller;
        Chunk c = new NewChunk(tsk._res).compress();
        DKV.put(tsk._k, c,_fs, false);
        new UpdateProgress(c._mem.length,c.frozenType()).fork(_progressKey);
        int j = _j.incrementAndGet();
        if(j < chks.length)
          new VecMulTsk(new Cmp(j,chks,xs), chks[j], _fs).asyncExec(Utils.append(new Vec[]{_Y.vec(j)}, xs));
      }
    }
    public void map(Chunk [] chks) {
      _j = new AtomicInteger(MAX_P);
      addToPendingCount(chks.length);
      int iStart = (int)chks[0]._start;
      int iEnd = iStart + chks[0]._len;
      Vec[] xs = Arrays.copyOfRange(_X.vecs(), iStart, iEnd);
      for(int i = 0; i < Math.min(MAX_P,chks.length); ++i)
        new VecMulTsk(new Cmp(i,chks,xs), chks[i], _fs).asyncExec(Utils.append(new Vec[]{_Y.vec(i)}, xs));
    }
  }

  private static final class VecMulTsk extends MRTask2<VecMulTsk> {
    final transient Key _k;
    final transient Futures _fs;
    double [] _res;
    public VecMulTsk(H2OCountedCompleter cmp, Chunk c, Futures fs) {
      super(cmp);
      _k = c._vec.chunkKey(c.cidx()); _fs = fs;
    }
    @Override
    public void map(Chunk [] chks) {
      _res = MemoryManager.malloc8d(chks.length-1);
      int [] pos = MemoryManager.malloc4(chks.length-1);
      for(int i = 0; i < pos.length; ++i)
        pos[i] = chks[i+1].nextNZ(-1);
      for(int j = 1; j < chks.length; ++j) {
        for(int i = 0; i < chks[0]._len; i = chks[0].nextNZ(i)) {
          final double y = chks[0].at0(i);
          if(chks[j] instanceof CXIChunk)
            while(pos[j-1] < chks[j]._len && pos[j-1] < i)
              pos[j-1] = chks[j].nextNZ(pos[j-1]);
          else pos[j-1] = i;
          if(pos[j-1] == i)
            _res[j - 1] += y * chks[j].at0(i);
        }
      }
    }
    @Override
    public void reduce(VecMulTsk v) { Utils.add(_res,v._res);}
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
}
