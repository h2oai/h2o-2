package water.fvec;

import water.*;

import java.util.ArrayList;

public class UploadFileVec extends ByteVec {
  private long _espc2[];
  private boolean _closed = false;
  private transient ArrayList<Long> _runningEspc;

  protected UploadFileVec(Key key) {
    super(key,null);
    _runningEspc = new ArrayList<Long>();
    _runningEspc.add(new Long(0));
  }

  @Override public long length() {
    assert _closed;
    return _espc2[_espc2.length-1];
  }

  @Override public int nChunks() {
    assert _closed;
    return _espc2.length-1;
  }

  @Override public boolean writable() { return !_closed; }

  @Override public long[] espc() {
    assert _closed;
    assert _espc2 != null;
    return _espc2;
  }

  int getChunkSz() { return (int)CHUNK_SZ; }

  public void addAndCloseChunk(Chunk c, Futures fs) {
    assert !_closed;
    assert (c._vec == null);    // Don't try to re-purpose a chunk.

    // Attach chunk to this vec.
    c._vec = this;
    int thisChunkNum = _runningEspc.size()-1;
    boolean alwaysPutToDKV = true;
    c.close(thisChunkNum, fs, alwaysPutToDKV);

    // Record number of rows in this chunk to the running total.
    long lastEspc = _runningEspc.get(_runningEspc.size()-1);
    long nextEspc = lastEspc + c._len;
    _runningEspc.add(new Long(nextEspc));
  }

  public void close() {
    assert !_closed;

    // Freeze the running totals into an actual list of Iceable totals.
    _espc2 = new long[_runningEspc.size()];
    for (int i = 0; i < _runningEspc.size(); i++) {
      _espc2[i] = _runningEspc.get(i).longValue();
    }

    // Lock out this vec and clean out transient stuff.
    _runningEspc = null;
    _closed = true;
  }
}
