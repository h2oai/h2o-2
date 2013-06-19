package water.fvec;

import water.*;

// The empty-compression function, where data is in 'int's.
public class C4Chunk extends Chunk {
  C4Chunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>2; }
  @Override public long   get ( int    i ) { return UDP.get4(_mem,i<<2); }
  @Override public double getd( int    i ) { return UDP.get4(_mem,i<<2); }
  @Override void   append2 ( long l, int exp ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.fail(); }
  @Override public C4Chunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>2;
    assert _mem.length == _len<<2;
    return this;
  }
}
