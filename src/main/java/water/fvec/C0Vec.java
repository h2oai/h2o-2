package water.fvec;

import water.*;

// The empty-compression function, where data is in bytes
public class C0Vec extends BigVector {
  long   at_impl ( int i ) { return 0xFF&_mem[i]; }
  double atd_impl( int i ) { throw H2O.unimpl(); }
  @Override public AutoBuffer write(AutoBuffer bb) { 
    throw new RuntimeException("do not call"); 
  }
  @Override public C0Vec read(AutoBuffer bb) { 
    _mem = bb.bufClose(); 
    _start = -1;
    _len = _mem.length;
    return this; 
  }
}
