package water.fvec;

import water.*;
import water.parser.DParseTask;

public class CNSChunk extends Chunk{
  static final int OFF=8+4+4;
  double _scale;
  int _bias;
  int _bytesz;
  static protected final long [] _NA = new long[]{0,0xFF,Short.MIN_VALUE,0,Integer.MIN_VALUE};
  CNSChunk( byte[] bs, int bytesize, int bias, double scale ) {
    _mem=bs; _start = -1;
    _len = (bs.length-OFF)/(_bytesz=bytesize);
    _bias = bias; _scale = scale;
    UDP.set8d(_mem,0,scale);
    UDP.set4 (_mem,8,bias );
    UDP.set4(_mem,12,_bytesz);
    assert _bytesz == 1 || _bytesz == 2 || _bytesz == 4:"UNEXPECTED BYTE SIZE " + _bytesz;
  }
  @Override protected final long at8_impl( int i ) {
    double res = atd_impl(i);
    if(Double.isNaN(res))throw new IllegalArgumentException("attempting to access missing value via at8 mehtod");
    return (long)res;
  }
  protected long mantissa(int i){
    switch(_bytesz){
      case 1:
        return 0xFF&_mem[i+OFF];
      case 2:
        return UDP.get2(_mem, (i << 1) + OFF);
      case 4:
        return UDP.get4(_mem, (i << 2) + OFF);
      default:
        throw new RuntimeException("unexpected byte size " + _bytesz);
    }
  }

  @Override protected final double atd_impl( int i ) {
    long res = mantissa(i);
    return ( res == _NA[_bytesz] )?Double.NaN:(_bias + res)*_scale;
  }

  @Override public StringBuilder atStr_impl(int i,StringBuilder sb){
    int ndigits =1;
    long mantissa = mantissa(i) + _bias;
    if(mantissa < 0){
      sb.append("-");
      mantissa = -mantissa;
    }
    int dec = -(int)Math.log10(_scale);
    if(mantissa != 0){
      ndigits = (int)Math.log10(mantissa) + 1;
      int x = 10;
      while((mantissa % x) == 0 && (ndigits != 0 && dec != 0)){
        --ndigits;
        --dec;
        x *= 10;
      }
    }
    long div = (long)Math.pow(10, ndigits-1);
    long rem = mantissa;
    for(int j = 0; j < ndigits; ++j){
      if(j == (ndigits - dec))sb.append(".");
      if(div == 0){
        System.out.println("haha");
      }
      sb.append(rem / div);
      rem = rem % div;
      div /= 10;
    }
    return sb;
  }

  @Override protected final boolean isNA_impl( int i ) { return mantissa(i) == _NA[_bytesz]; }
  @Override boolean set_impl(int i, long l) {
    long res = (long)(l/_scale)-_bias; // Compressed value
    double d = (res+_bias)*_scale;     // Reverse it
    if( (long)d != l ) return false;   // Does not reverse cleanly?
    switch(_bytesz) {
      case 1:
        if(255 <= res || res < 0)return false;
        _mem[i+OFF] = (byte)res;
        break;
      case 2:
        if(Short.MAX_VALUE <= res || res < Short.MIN_VALUE)return false;
        UDP.set2(_mem, OFF + (i << 1), (short)res);
        break;
      case 4:
        if(Integer.MAX_VALUE <= res || res < Integer.MIN_VALUE)return false;
        UDP.set4(_mem, OFF + (i << 1), (int)res);
        break;
      default:
        throw H2O.unimpl();
    }
    return true;
  }
  @Override boolean set_impl(int i, double d) { return false; }
  @Override boolean set_impl(int i, float f ) { return false; }
  @Override boolean setNA_impl(int idx) {
    switch(_bytesz){
      case 1: _mem[idx+OFF] = (byte)_NA[1]; return true;
      case 2: UDP.set2(_mem, OFF + (idx << 1), (short)_NA[2]); return true;
      case 4: UDP.set4(_mem, OFF + (idx << 2), (short)_NA[4]); return true;
      default: throw H2O.unimpl();
    }
  }
  @Override public boolean hasFloat() { return _scale < 1.0; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public CNSChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length-OFF;
    _scale= UDP.get8d(_mem,0);
    _bias = UDP.get4 (_mem,8);
    _bytesz = UDP.get4(_mem, 12);
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    double dx = Math.log10(_scale);
    assert DParseTask.fitsIntoInt(dx);
    int x = (int)dx;
    nc._ds = null;
    nc._ls = MemoryManager.malloc8 (_len);
    nc._xs = MemoryManager.malloc4 (_len);
    for( int i=0; i<_len; i++ ) {
      long res = mantissa(i);
      if( res == C1Chunk._NA ) nc.setInvalid(i);
      else {
        nc._ls[i] = res+_bias;
        nc._xs[i] = x;
      }
    }
    return nc;
  }
  public int pformat_len0() { return hasFloat() ? pformat_len0(_scale,3) : super.pformat_len0(); }
  public String  pformat0() { return hasFloat() ? "% 8.2e" : super.pformat0(); }
}
