package water.fvec;

import water.*;
import water.parser.DParseTask;

/**
 * The scale/bias function, where data is in SIGNED bytes before scaling.
 */
public class C2SChunk extends Chunk {
  static private final long _NA = Short.MIN_VALUE;
  static final int OFF=8+4;
  public double _scale;
  int _bias;
  C2SChunk( byte[] bs, int bias, double scale ) { _mem=bs; _start = -1; _len = (_mem.length-OFF)>>1;
    _bias = bias; _scale = scale;
    UDP.set8d(_mem,0,scale);
    UDP.set4 (_mem,8,bias );
  }
  @Override protected final long at8_impl( int i ) {
    long res = UDP.get2(_mem,(i<<1)+OFF);
    if( res == _NA ) throw new IllegalArgumentException("at8 but value is missing");
    return (long)((res + _bias)*_scale);
  }
  @Override protected final double atd_impl( int i ) {
    long res = UDP.get2(_mem,(i<<1)+OFF);
    return (res == _NA)?Double.NaN:(res + _bias)*_scale;
  }
  @Override protected final boolean isNA_impl( int i ) { return UDP.get2(_mem,(i<<1)+OFF) == _NA; }
  @Override boolean set_impl(int idx, long l) {
    long res = (long)(l/_scale)-_bias; // Compressed value
    double d = (res+_bias)*_scale;     // Reverse it
    if( (long)d != l ) return false;   // Does not reverse cleanly?
    if( !(Short.MIN_VALUE < res && res <= Short.MAX_VALUE) ) return false; // Out-o-range for a short array
    UDP.set2(_mem,(idx<<1)+OFF,(short)res);
    return true;
  }
  @Override boolean set_impl(int i, double d) { throw H2O.unimpl(); }
  @Override boolean set_impl(int i, float f ) { return false; }
  @Override boolean setNA_impl(int idx) { UDP.set2(_mem,(idx<<1)+OFF,(short)_NA); return true; }
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
    double dx = Math.log10(_scale);
    int x = (int)dx;
    if( DParseTask.pow10i(x) != _scale ) throw H2O.unimpl();
    for( int i=0; i<_len; i++ ) {
      long res = UDP.get2(_mem,(i<<1)+OFF);
      if( res == _NA ) nc.setInvalid(i);
      else {
        nc._ls[i] = res+_bias;
        nc._xs[i] = x;
      }
    }
    return nc;
  }
  public int pformat_len0() { return pformat_len0(_scale,5); }
  public String pformat0() { return "% 10.4e"; }
}
