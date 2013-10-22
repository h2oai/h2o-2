package water.parser;

import java.util.ArrayList;

public final class ValueString {
   private byte [] _buf;
   private int _off;
   private int _len;
   ArrayList<Integer> _skips = new ArrayList<Integer>();

   public ValueString() {}

   public ValueString(byte [] buf, int off, int len){
     _buf = buf;
     _off = off;
     _len = len;
   }

   public ValueString(String from) {
     _buf = from.getBytes();
     _off = 0;
     _len = get_buf().length;
   }

   public ValueString(byte [] buf){
     this(buf,0,buf.length);
   }
   @Override
   public int hashCode(){
     int hash = 0;
     if(_skips.isEmpty()){
       int n = get_off() + get_length();
       for (int i = get_off(); i < n; ++i)
         hash = 31 * hash + get_buf()[i];
       return hash;
     } else {
       int i = get_off();
       for(int n:_skips){
         for(; i < get_off()+n; ++i)
           hash = 31*hash + get_buf()[i];
         ++i;
       }
       int n = get_off() + get_length();
       for(; i < n; ++i)
         hash = 31*hash + get_buf()[i];
       return hash;
     }
   }

   void addChar(){_len++;}
   void skipChar(){_skips.add(_len++);}

   void addBuff(byte [] bits){
     byte [] buf = new byte[get_length()];
     int l1 = get_buf().length-get_off();
     System.arraycopy(get_buf(), get_off(), buf, 0, l1);
     System.arraycopy(bits, 0, buf, l1, get_length()-l1);
     _off = 0;
     _buf = buf;
   }

  @Override
  public String toString(){
    if(_skips.isEmpty())return new String(get_buf(),get_off(),get_length());
    StringBuilder sb = new StringBuilder();
    int off = get_off();
    for(int next:_skips){
      int len = get_off()+next-off;
      sb.append(new String(get_buf(),off,len));
      off += len + 1;
    }
    sb.append(new String(get_buf(),off,get_length()-off+get_off()));
    return sb.toString();
  }

  void set(byte [] buf, int off, int len){
    _buf = buf;
    _off = off;
    _len = len;
    _skips.clear();
  }

  public ValueString setTo(String what) {
    _buf = what.getBytes();
    _off = 0;
    _len = _buf.length;
    _skips.clear();
    return this;
  }

  @Override public boolean equals(Object o){
    if(!(o instanceof ValueString)) return false;
    ValueString str = (ValueString)o;
    if(_skips.isEmpty() && str._skips.isEmpty()){ // no skipped chars
      if(str.get_length() != get_length())return false;
      for(int i = 0; i < get_length(); ++i)
        if(get_buf()[get_off()+i] != str.get_buf()[str.get_off()+i]) return false;
    }else if(str._skips.isEmpty()){ // only this has skipped chars
      if(get_length() - _skips.size() != str.get_length())return false;
      int j = 0;
      int i = get_off();
      for(int n:_skips){
        for(; i < get_off()+n; ++i)
          if(get_buf()[i] != str.get_buf()[j++])return false;
        ++i;
      }
      int n = get_off() + get_length();
      for(; i < n; ++i) if(get_buf()[i] != str.get_buf()[j++])return false;
    } else return toString().equals(str.toString()); // both strings have skipped chars, unnecessarily complicated so just turn it into a string (which skips the chars), should not happen too often

    return true;
  }
  public final byte [] get_buf() {return _buf;}
  public final int get_off() {return _off;}
  public final int get_length() {return _len;}
}
