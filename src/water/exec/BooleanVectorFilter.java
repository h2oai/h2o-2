package water.exec;

import water.*;
import java.util.Arrays;

/**
 * @author peta
 */
public class BooleanVectorFilter extends MRTask {
  final Key _dst;               // Output key
  final Key _src;               // Input key; copy matching rows from here
  final Key _bvec;              // Read bools from this VA
  final int _bcol;              // ... and from this column
  long _rpc[];                  // Rows-per-chunk on the output

  public BooleanVectorFilter( Key dst, Key src, Key bvec, int bcol ) {
    // Source & boolean vector must be compatible lengths
    assert ValueArray.value(DKV.get(bvec))._numrows == ValueArray.value(DKV.get(src))._numrows;
    _dst = dst;
    _src = src;
    _bvec= bvec;
    _bcol= bcol;
  }

  @Override public void map(Key key) {
    long srcIdx = ValueArray.getChunkIndex(key);
    // Source metadata
    ValueArray srcAry = ValueArray.value(DKV.get(_src));
    final int rows = srcAry.rpc(srcIdx);
    // Get the raw bits to work on
    AutoBuffer srcBits = srcAry.getChunk(key);
    AutoBuffer dstBits = new AutoBuffer(srcBits.remaining());
    // Boolean iterator
    long srow = srcAry.startRow(srcIdx);
    VAIterator vai = new VAIterator(_bvec,_bcol, srow);
    // Accumulate rows-per-chunk
    _rpc = new long[(int)srcAry.chunks()+1];
    long rpc = 0;
    
    for( int i=0; i<rows; i++ ) {
      vai.next();
      if( vai.data() != 0 ) {
        dstBits.copyArrayFrom(dstBits.position(),srcBits,i*srcAry._rowsize,srcAry._rowsize);
        rpc++;
      }
    }
    _rpc[(int)srcIdx] = rpc;
    byte[] dbits = dstBits.buf();
    Key d = ValueArray.getChunkKey(srcIdx, _dst);
    Value v = new Value(d, dbits);
    DKV.put(d,v);
  }

  @Override public void reduce(DRemoteTask drt) {
    BooleanVectorFilter other = (BooleanVectorFilter) drt;
    if( _rpc == null ) _rpc = other._rpc;
    else 
      for( int i=0; i<_rpc.length; i++ )
        _rpc[i] += other._rpc[i];
  }
}
