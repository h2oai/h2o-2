package water.fvec;

import water.AutoBuffer;
import water.Freezable;
import water.H2O;

// The empty-compression function, where data is in bytes
public class C0Vec extends CVec {
  long   at_impl ( int i ) { return 0xFF&_mem[i]; }
  double atd_impl( int i ) { throw H2O.unimpl(); }
  @Override public int length() { return _mem.length; }
  @Override public AutoBuffer write(AutoBuffer bb) { throw new RuntimeException("do not call"); }
  @Override public C0Vec read(AutoBuffer bb) { _mem = bb.bufClose(); return this; }
}
