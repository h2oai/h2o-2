package water.exec;

import water.fvec.*;
import water.*;

/** A (global) Vec with local lifetime.
 *  @author cliffc@0xdata.com
 */
public class TmpVec extends Vec {
  // Reference counted local vecs.  No expectation of the refcnts being passed
  // around the cloud; local counting only.
  int _refcnt;
  TmpVec( Key key, Vec v ) { super(key,v); }

  // Double your memory costs
  public static Frame deepAllocTmp(Frame fr) {
    Futures fs = new Futures();
    Vec  vecs[] = fr.vecs();
    Vec nvecs[] = new Vec[fr._names.length];
    for( int i=0; i<vecs.length; i++ )
      nvecs[i] =  makeZero(vecs[i],fs);
    fs.blockForPending();
    return new Frame(fr._names,nvecs);
  }


  static TmpVec makeZero( Vec v, Futures fs ) {
    int nchunks = v.nChunks();
    TmpVec v0 = new TmpVec(v.group().addVecs(1)[0],v);
    long row=0;                 // Start row
    for( int i=0; i<nchunks; i++ ) {
      long nrow = v.chunk2StartElem(i+1); // Next row
      DKV.put(v0.chunkKey(i),new C0LChunk(0,(int)(nrow-row)),fs);
      row = nrow;
    }
    DKV.put(v0._key,v0,fs);
    return v0;
  }
}
