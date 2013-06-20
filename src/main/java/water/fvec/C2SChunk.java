package water.fvec;

import water.*;

// The scale/bias function, where data is in SIGNED bytes before scaling
public class C2SChunk extends Chunk {
  static final int OFF=8+4;
  double _scale;
  int _bias;
  C2SChunk( byte[] bs, int bias, double scale ) {super(Short.MIN_VALUE); _mem=bs; _start = -1; _len = (_mem.length-OFF)>>1;
    _bias = bias; _scale = scale;
    UDP.set8d(_mem,0,scale);
    UDP.set4 (_mem,8,bias );
  }
  @Override public long   get ( int    i ) { return (UDP.get2(_mem,(i<<1)+OFF) == NA())?NA():(long)((UDP.get2(_mem,(i<<1)+OFF)+_bias)*_scale); }
  @Override public double getd( int    i ) { return (UDP.get2(_mem,(i<<1)+OFF) == NA())?Double.NaN:((UDP.get2(_mem,(i<<1)+OFF)+_bias)*_scale); }
  @Override void   append2 ( long l, int exp ) { throw H2O.fail(); }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C2SChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = (_mem.length-OFF)>>1;
    _scale= UDP.get8d(_mem,0);
    _bias = UDP.get4 (_mem,8);
    return this;
  }
}
