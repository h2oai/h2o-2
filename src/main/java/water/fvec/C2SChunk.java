package water.fvec;

import water.*;

// The scale/bias function, where data is in SIGNED bytes before scaling
public class C2SChunk extends Chunk {
  static private final long _NA = Short.MIN_VALUE;
  static final int OFF=8+4;
  double _scale;
  int _bias;
  C2SChunk( byte[] bs, int bias, double scale ) { _mem=bs; _start = -1; _len = (_mem.length-OFF)>>1;
    _bias = bias; _scale = scale;
    UDP.set8d(_mem,0,scale);
    UDP.set4 (_mem,8,bias );
  }
  @Override protected final long at8_impl( int i ) {
    long res = UDP.get2(_mem,(i<<1)+OFF);
    return (res == _NA)?_vec._iNA:(long)((res + _bias)*_scale);
  }
  @Override protected final double atd_impl( int i ) {
    long res = UDP.get2(_mem,(i<<1)+OFF);
    return (res == _NA)?_vec._fNA:(res + _bias)*_scale;
  }
  @Override boolean set8_impl(int idx, long l) { 
    long res = (long)(l/_scale)-_bias; // Compressed value
    double d = (res+_bias)*_scale;     // Reverse it
    if( (long)d != l ) return false;   // Does not reverse cleanly?
    if( !(Short.MIN_VALUE < res && res <= Short.MAX_VALUE) ) return false; // Out-o-range for a short array
    UDP.set2(_mem,(idx<<1)+OFF,(short)res);
    return true; 
  }
  @Override boolean set8_impl(int i, double d) { 
    throw H2O.unimpl();
    //return false; 
  }
  @Override boolean hasFloat() { return _scale < 1.0; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C2SChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = (_mem.length-OFF)>>1;
    _scale= UDP.get8d(_mem,0);
    _bias = UDP.get4 (_mem,8);
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    throw H2O.unimpl();
  }
}
