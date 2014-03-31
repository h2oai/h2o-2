package water.fvec;

import jsr166y.CountedCompleter;
import water.*;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by tomasnykodym on 3/28/14.
 *
 * Utility to rebalance dataset so that:
 *   1) Each chunk has the same number of lines (+/-1)
 *   2) There is enough chunks for all cores in case of data parallel processing
 *      tries to get 4x as many chunks aas there are cores in the cloud.
 * It *does not* guarantee even chunk-node placement.
 * (This can not currently be done in H2O, since the placement of chunks is governed only by key-hash /vector group/ for Vecs)
 */
public class RebalanceDataSet {
  /**
   * Rebalance the dataset
   * @param resKey
   * @param f
   * @param minRows
   * @return
   */
  public static Frame rebalanceDataset(final Key resKey, final Frame f, final int minRows){
    H2O.H2OCountedCompleter cmp = new H2O.H2OEmptyCompleter();
    rebalanceDataset(cmp,resKey,f,minRows);
    cmp.join();
    return UKV.get(resKey);
  }


  public static void rebalanceDataset(H2O.H2OCountedCompleter cmp, final Key resKey, final Frame f, final int minRows){
    final Vec [] newVecs = new Vec[f.numCols()];
    f.read_lock(null);
    H2O.submitTask(new H2O.H2OCountedCompleter(cmp) {
      @Override
      public void compute2() {
        // simply create a bogus new vector (don't even put it into KV) with appropriate number of lines per chunk and then use it as a source to do multiple makeZero calls
        // to create empty vecs and than call RebalanceTask on each one of them.
        // RebalanceTask will fetch the appropriate src chunks and fetch the data from them.
        int nchunks = (int)Math.min((f.numRows() / minRows), H2O.CLOUD.size()*H2O.NUMCPUS*4);
        int rpc = (int)(f.numRows() / nchunks);
        int rem = (int)(f.numRows() % nchunks);
        long [] espc = new long[nchunks+1];
        Arrays.fill(espc,rpc);
        for(int i = 0; i < rem; ++i)++espc[i];
        long sum = 0;
        for(int i = 0; i < espc.length; ++i) {
          long  s = espc[i];
          espc[i] = sum;
          sum += s;
        }
        assert espc[espc.length-1] == f.numRows():"unexpected number of rows, expected " + f.numRows() + ", got " + espc[espc.length-1];
        Vec v0 = new Vec(Vec.newKey(),espc);

        final Vec [] srcVecs = f.vecs();
        addToPendingCount(newVecs.length);
        for(int i = 0; i < newVecs.length; ++i)newVecs[i] = v0.makeZero(srcVecs[i].domain());
        new Frame(resKey,f.names(), newVecs).delete_and_lock(null);
        for(int i = 0; i < newVecs.length; ++i)new RebalanceTask(this,srcVecs[i]).asyncExec(newVecs[i]);
        tryComplete();
      }
      @Override public void onCompletion(CountedCompleter caller){
        f.unlock(null);
        Frame res = new Frame(resKey,f.names(),newVecs);
        res.update(null);
        res.unlock(null);
      }
    });
  }

  private static class RebalanceTask extends MRTask2<RebalanceTask> {
    final Vec _srcVec;
    public RebalanceTask(H2O.H2OCountedCompleter cmp, Vec srcVec){super(cmp);_srcVec = srcVec;}
    @Override public void map(Chunk chk){
      final int dstrows = chk._len;
      NewChunk dst = new NewChunk(chk);
      dst._len = dst._len2 = 0;
      int rem = chk._len;
      while(rem > 0 && dst._len2 < chk._len){
        Chunk srcRaw = _srcVec.chunkForRow(chk._start+dst._len2);
        NewChunk src = new NewChunk((srcRaw));
        src = srcRaw.inflate_impl(src);
        assert src._len2 == srcRaw._len;
        int srcFrom = (int)(chk._start+dst._len2 - src._start);
        boolean sparse = false;
        // check if the result is sparse (not exact since we only take subset of src in general)
        if((src.sparse() && dst.sparse()) || (src._len + dst._len < NewChunk.MIN_SPARSE_RATIO*(src._len2 + dst._len2))){
          src.set_sparse(src._len);
          dst.set_sparse(dst._len);
          sparse = true;
        }
        final int srcTo = srcFrom + rem;
        int off = srcFrom-1;
        Iterator<NewChunk.Value> it = src.values(Math.max(0,srcFrom),srcTo);
        while(it.hasNext()){
          NewChunk.Value v = it.next();
          final int rid = v.rowId0();
          assert  rid < srcTo;
          int add = rid - off;
          off = rid;
          dst.addZeros(add-1);
          v.add2Chunk(dst);
          rem -= add;
          assert rem >= 0;
        }
        int trailingZeros = Math.min(rem,src._len2 - off -1);
        dst.addZeros(trailingZeros);
        rem -= trailingZeros;
      }
      assert rem == 0:"rem = " + rem;
      assert dst._len2 == chk._len:"len2 = " + dst._len2 + ", _len = " + chk._len;
      dst.close(dst.cidx(),_fs);
    }
  }
}
