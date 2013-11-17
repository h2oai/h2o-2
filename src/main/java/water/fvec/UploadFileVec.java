package water.fvec;

import water.*;

public class UploadFileVec extends FileVec {
  int _nchunks;
  int _last=CHUNK_SZ;
  protected UploadFileVec(Key key) {
    super(key,-1,Value.ICE);
  }

  @Override public boolean writable() { return _len==-1; }

  public void addAndCloseChunk(Chunk c, Futures fs) {
    assert _len==-1;            // Not closed
    assert (c._vec == null);    // Don't try to re-purpose a chunk.
    assert _last==CHUNK_SZ;     // Always CHUNK_SZ except the last one
    _last = c._len;             // Save (possible) last one

    // Attach chunk to this vec.
    c._vec = this;
    DKV.put(chunkKey(_nchunks++),c,fs); // Write updated chunk back into K/V
  }

  public void close() {
    assert _len==-1;            // Not closed
    _len = (_nchunks<<LOG_CHK)+_last;
  }

  @Override public Value chunkIdx( int cidx ) {
    Value val = DKV.get(chunkKey(cidx));
    assert checkMissing(cidx,val);
    return val;
  }
}
