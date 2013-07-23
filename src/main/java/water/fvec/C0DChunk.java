package water.fvec;

import java.util.Arrays;

import water.AutoBuffer;
import water.UDP;

// The constant column
public class C0DChunk extends Chunk {
  static final int OFF=8+4;
  double _con;
  C0DChunk(double con, int len) { _mem=new byte[OFF]; _start = -1; _len = len;
    _con = con;
    UDP.set8d(_mem,0,con);
    UDP.set4(_mem,8,len);
  }
  @Override protected final long at8_impl( int i ) { return (long)_con; }
  @Override protected final double atd_impl( int i ) {return _con; }
  @Override boolean set8_impl(int idx, long l) { return l==_con; }
  @Override boolean set8_impl(int i, double d) { return d==_con; }
  @Override boolean hasFloat() { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C0DChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _con = UDP.get8d(_mem,0);
    _len = UDP.get4(_mem,8);
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    Arrays.fill(nc._ds,_con);
    return nc;
  }
}