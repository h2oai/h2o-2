package water.parser;

import java.util.ArrayList;
import water.Iced;

public final class ValueString extends Iced implements Comparable<ValueString> {
  private byte [] _buf;
  private int _off;
  private int _len;

  public ValueString() {}

  public ValueString(byte [] buf, int off, int len){
    _buf = buf;
    _off = off;
    _len = len;
  }
  
  public ValueString(String from) {
    _buf = from.getBytes();
    _off = 0;
    _len = _buf.length;
  }
  
  public ValueString(byte [] buf){
    this(buf,0,buf.length);
  }
  
  void addChar (){_len++;}
  void skipChar(){_len++;}
  
  void addBuff(byte [] bits){
    byte [] buf = new byte[_len];
    int l1 = _buf.length-_off;
    System.arraycopy(_buf, _off, buf, 0, l1);
    System.arraycopy(bits, 0, buf, l1, _len-l1);
    _off = 0;
    _buf = buf;
  }
  
  @Override public int hashCode() {
    int hash = 0;
    int n = _off + _len;
    for (int i = _off; i < n; ++i)
      hash = 31 * hash + _buf[i];
    return hash;
  }

  @Override public boolean equals(Object o) {
    if(!(o instanceof ValueString)) return false;
    ValueString str = (ValueString)o;
    if( str._len != _len ) return false;
    for( int i = 0; i < _len; ++i )
      if( _buf[_off+i] != str._buf[str._off+i]) 
        return false;
    return true;
  }

  @Override public int compareTo( ValueString o ) {
    int len = Math.min(_len,o._len);
    for( int i=0; i<len; i++ ) {
      int x = (0xFF&_buf[_off+i]) - (0xFF&o._buf[o._off+i]);
      if( x != 0 ) return x;
    }
    return _len - o._len;
  }
 
  // WARNING: LOSSY CONVERSION!!!
  // Converting to a String will truncate all bytes with high-order bits set,
  // even if they are otherwise a valid member of the field/ValueString.
  // Converting back to a ValueString will then make something with fewer
  // characters than what you started with, and will fail all equals() tests.a
  @Override public String toString(){
    return new String(_buf,_off,_len);
  }
  public static String[] toString( ValueString vs[] ) {
    if( vs==null ) return null;
    String[] ss = new String[vs.length];
    for( int i=0; i<vs.length; i++ )
      ss[i] = vs[i].toString();
    return ss;
  }

  void set(byte [] buf, int off, int len){
    _buf = buf;
    _off = off;
    _len = len;
  }

  public ValueString setTo(String what) {
    _buf = what.getBytes();
    _off = 0;
    _len = _buf.length;
    return this;
  }

  public final byte [] get_buf() {return _buf;}
  public final int get_off() {return _off;}
  public final int get_length() {return _len;}
}
