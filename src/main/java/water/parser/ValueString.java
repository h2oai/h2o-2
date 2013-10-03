package water.parser;

import java.util.ArrayList;

public final class ValueString {
   byte [] _buf;
   int _off;
   int _length;
   ArrayList<Integer> _skips = new ArrayList<Integer>();

   public ValueString() {}

   public ValueString(byte [] buf, int off, int len){
     _buf = buf;
     _off = off;
     _length = len;
   }

   public ValueString(String from) {
     _buf = from.getBytes();
     _off = 0;
     _length = _buf.length;
   }

   public ValueString(byte [] buf){
     this(buf,0,buf.length);
   }
   @Override
   public int hashCode(){
     int hash = 0;
     if(_skips.isEmpty()){
       int n = _off + _length;
       for (int i = _off; i < n; ++i)
         hash = 31 * hash + _buf[i];
       return hash;
     } else {
       int i = _off;
       for(int n:_skips){
         for(; i < _off+n; ++i)
           hash = 31*hash + _buf[i];
         ++i;
       }
       int n = _off + _length;
       for(; i < n; ++i)
         hash = 31*hash + _buf[i];
       return hash;
     }
   }

   void addChar(){++_length;}
   void skipChar(){_skips.add(_length++);}

   void addBuff(byte [] bits){
     byte [] buf = new byte[_length];
     int l1 = _buf.length-_off;
     System.arraycopy(_buf, _off, buf, 0, l1);
     System.arraycopy(bits, 0, buf, l1, _length-l1);
     _off = 0;
     _buf = buf;
   }

  @Override
  public String toString(){
    if(_skips.isEmpty())return new String(_buf,_off,_length);
    StringBuilder sb = new StringBuilder();
    int off = _off;
    for(int next:_skips){
      int len = _off+next-off;
      sb.append(new String(_buf,off,len));
      off += len + 1;
    }
    sb.append(new String(_buf,off,_length-off+_off));
    return sb.toString();
  }

  void set(byte [] buf, int off, int len){
    _buf = buf;
    _off = off;
    _length = len;
    _skips.clear();
  }

  public ValueString setTo(String what) {
    _buf = what.getBytes();
    _off = 0;
    _length = _buf.length;
    _skips.clear();
    return this;
  }

  @Override public boolean equals(Object o){
    if(!(o instanceof ValueString)) return false;
    ValueString str = (ValueString)o;
    if(_skips.isEmpty() && str._skips.isEmpty()){ // no skipped chars
      if(str._length != _length)return false;
      for(int i = 0; i < _length; ++i)
        if(_buf[_off+i] != str._buf[str._off+i]) return false;
    }else if(str._skips.isEmpty()){ // only this has skipped chars
      if(_length - _skips.size() != str._length)return false;
      int j = 0;
      int i = _off;
      for(int n:_skips){
        for(; i < _off+n; ++i)
          if(_buf[i] != str._buf[j++])return false;
        ++i;
      }
      int n = _off + _length;
      for(; i < n; ++i) if(_buf[i] != str._buf[j++])return false;
    } else return toString().equals(str.toString()); // both strings have skipped chars, unnecessarily complicated so just turn it into a string (which skips the chars), should not happen too often

    return true;
  }
}
