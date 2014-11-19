package water.fvec;

import jsr166y.CountedCompleter;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.util.Log;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tomasnykodym on 3/28/14.
 *
 * Utility to rebalance dataset so that it has requested number of chunks and each chunk has the same number of rows +/-1.
 *
 * It *does not* guarantee even chunk-node placement.
 * (This can not currently be done in H2O, since the placement of chunks is governed only by key-hash /vector group/ for Vecs)
 */
public class RebalanceDataSet extends H2O.H2OCountedCompleter {
  final Frame _in;

  Key _okey;
  Frame _out;
  final Key _jobKey;
  final transient Vec.VectorGroup _vg;
  final transient long [] _espc;

  /**
   * Constructor for make-compatible task.
   *
   * To be used to make frame compatible with other frame (i.e. make all vecs compatible with other vector group and rows-per-chunk).
   */
  public RebalanceDataSet(Frame modelFrame, Frame srcFrame, Key dstKey) {this(modelFrame,srcFrame,dstKey,null,null);}
  public RebalanceDataSet(Frame modelFrame, Frame srcFrame, Key dstKey, H2O.H2OCountedCompleter cmp, Key jobKey) {
    super(cmp);
    _in = srcFrame;
    _jobKey = jobKey;
    _okey = dstKey;
    _espc = modelFrame.anyVec()._espc;
    _vg = modelFrame.anyVec().group();
  }

  /**
   * Constructor for re-balancing the dataset (e.g. for performance reasons).
   * Resulting dataset will have requested number of chunks and rows will be uniformly distributed with the
   * same rows-per chunk count in all chunk (+/- 1).
   */
  public RebalanceDataSet(Frame srcFrame, Key dstKey, int nchunks) { this(srcFrame,dstKey,nchunks,null,null);}
  public RebalanceDataSet(Frame srcFrame, Key dstKey, int nchunks, H2O.H2OCountedCompleter cmp, Key jobKey) {
    super(cmp);
    // simply create a bogus new vector (don't even put it into KV) with appropriate number of lines per chunk and then use it as a source to do multiple makeZero calls
    // to create empty vecs and than call RebalanceTask on each one of them.
    // RebalanceTask will fetch the appropriate src chunks and fetch the data from them.
    int rpc = (int)(srcFrame.numRows() / nchunks);
    int rem = (int)(srcFrame.numRows() % nchunks);
    final long [] espc;
    _espc = new long[nchunks + 1];
    Arrays.fill(_espc, rpc);
    for (int i = 0; i < rem; ++i) ++_espc[i];
    long sum = 0;
    for (int i = 0; i < _espc.length; ++i) {
      long s = _espc[i];
      _espc[i] = sum;
      sum += s;
    }
    assert _espc[_espc.length - 1] == srcFrame.numRows() : "unexpected number of rows, expected " + srcFrame.numRows() + ", got " + _espc[_espc.length - 1];
    _in = srcFrame;
    _jobKey = jobKey;
    _okey = dstKey;
    _vg = Vec.VectorGroup.newVectorGroup();
  }

  public Frame getResult(){join(); return _out;}

  boolean unlock;
  @Override
  public void compute2() {
    final Vec [] srcVecs = _in.vecs();
    _out = new Frame(_okey,_in.names(), new Vec(_vg.addVec(),_espc).makeZeros(srcVecs.length,_in.domains(),_in.uuids(),_in.times()));
    _out.delete_and_lock(_jobKey);
    new RebalanceTask(this,srcVecs).asyncExec(_out);
  }

  @Override public void onCompletion(CountedCompleter caller){
    assert _out.numRows() == _in.numRows();
    _out.update(_jobKey);
    _out.unlock(_jobKey);
  }
  @Override public boolean onExceptionalCompletion(Throwable t, CountedCompleter caller){
    if(_out != null)_out.delete(_jobKey,0.0f);
    return true;
  }
  public static class RebalanceAndReplaceDriver extends H2OCountedCompleter {
    final AtomicInteger _cntr;
    final int _maxP;
    final Vec [] _vecs;
    Vec [] _newVecs;
    final int _nChunks;
    public RebalanceAndReplaceDriver(int nChunks, int maxP, Vec... vecs){
      _cntr = new AtomicInteger(maxP);
      _maxP = maxP;
      _vecs = vecs;
      _nChunks = nChunks;
    }

    @Override
    public void compute2() {
      long [] espc = MemoryManager.malloc8(_nChunks+1);
      int rpc = (int)(_vecs[0].length() / _nChunks);
      int rem = (int)(_vecs[0].length() % _nChunks);;
      Arrays.fill(espc, rpc);
      for (int i = 0; i < rem; ++i) ++espc[i];
      long sum = 0;
      for (int i = 0; i < espc.length; ++i) {
        long s = espc[i];
        espc[i] = sum;
        sum += s;
      }
      _newVecs = new Vec(Vec.newKey(),espc).makeZeros(_vecs.length);
      setPendingCount(_vecs.length-1);
      for(int i = 0; i < Math.min(_vecs.length,_maxP); ++i) {
        new RebalanceTask(new Cmp(), _vecs[i]).asyncExec(_newVecs[i]);
      }
    }

    private class Cmp extends H2OCountedCompleter {
      Cmp(){super(RebalanceAndReplaceDriver.this);}
      @Override
      public void compute2() { throw H2O.fail("do not call!");}

      @Override
      public void onCompletion(CountedCompleter caller) {
        int i = _cntr.incrementAndGet();
        RebalanceTask rbt = (RebalanceTask)caller;
        Futures fs = new Futures();
        for(Vec v:rbt._srcVecs)
          v.remove(fs);
        fs.blockForPending();;
        if(i < _newVecs.length) {
          new RebalanceTask(new Cmp(), _vecs[i]).asyncExec(_newVecs[i]);
        }
      }
    }
  }

  public static Vec[] rebalanceAndReplace(int nchunks, int maxP, Vec... vecs) {
    RebalanceAndReplaceDriver rbt = new RebalanceAndReplaceDriver(nchunks, maxP, vecs);
    H2O.submitTask(rbt).join();
    return rbt._newVecs;
  }
  public static class RebalanceTask extends MRTask2<RebalanceTask> {
    final Vec [] _srcVecs;
    public RebalanceTask(H2O.H2OCountedCompleter cmp, Vec... srcVecs){super(cmp);_srcVecs = srcVecs;}

    @Override public boolean logVerbose() { return false; }

    private void rebalanceChunk(Vec srcVec, Chunk chk){
      Chunk srcRaw = null;
      try {
        NewChunk dst = new NewChunk(chk);
        dst._len = dst._sparseLen = 0;
        int rem = chk._len;
        while (rem > 0 && dst._len < chk._len) {
          srcRaw = srcVec.chunkForRow(chk._start + dst._len);
          NewChunk src = new NewChunk((srcRaw));
          src = srcRaw.inflate_impl(src);
          assert src._len == srcRaw._len;
          int srcFrom = (int) (chk._start + dst._len - src._start);
          // check if the result is sparse (not exact since we only take subset of src in general)
          if ((src.sparse() && dst.sparse()) || ((src.sparseLen() + dst.sparseLen()) * NewChunk.MIN_SPARSE_RATIO < (src.len() + dst.len()))) {
            src.set_sparse(src.sparseLen());
            dst.set_sparse(dst.sparseLen());
          }
          final int srcTo = srcFrom + rem;
          int off = srcFrom - 1;
          Iterator<NewChunk.Value> it = src.values(Math.max(0, srcFrom), srcTo);
          while (it.hasNext()) {
            NewChunk.Value v = it.next();
            final int rid = v.rowId0();
            assert rid < srcTo;
            int add = rid - off;
            off = rid;
            dst.addZeros(add - 1);
            v.add2Chunk(dst);
            rem -= add;
            assert rem >= 0;
          }
          int trailingZeros = Math.min(rem, src._len - off - 1);
          dst.addZeros(trailingZeros);
          rem -= trailingZeros;
        }
        assert rem == 0 : "rem = " + rem;
        assert dst._len == chk._len : "len2 = " + dst._len + ", _len = " + chk._len;
        dst.close(dst.cidx(), _fs);
      } catch(RuntimeException t){
        Log.err("got exception while rebalancing chunk " + srcRaw == null?"null":srcRaw.getClass().getSimpleName());
        throw t;
      }
    }
    @Override public void map(Chunk [] chks){
      for(int i = 0; i < chks.length; ++i)
        rebalanceChunk(_srcVecs[i],chks[i]);
    }
  }
}
