package water.parser;

public final class ValueString {
   byte [] _buf;
   int _off;
   int _length;

   public ValueString(byte [] buf, int off, int len){
     _buf = buf;
     _off = off;
     _length = len;
   }

   public ValueString(byte [] buf){
     this(buf,0,buf.length);
   }
   @Override
   public int hashCode(){
     int hash = 0;
     int n = _off + _length;
     for (int i = _off; i < n; ++i)
       hash = 31 * hash + _buf[i];
     return hash;
   }

   void addChar(){
     ++_length;
   }

   void set(byte [] buf, int off, int len){
     _buf = buf;
     _off = off;
     _length = len;
   }

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
    return new String(_buf,_off,_length);
  }

  public ValueString() { }

  public ValueString(String from) {
    _buf = from.getBytes();
    _off = 0;
    _length = _buf.length;
  }

  public ValueString setTo(String what) {
    _buf = what.getBytes();
    _off = 0;
    _length = _buf.length;
    return this;
  }

  @Override public boolean equals(Object o){
    if(!(o instanceof ValueString)) return false;
    ValueString str = (ValueString)o;
    if(str._length != _length)return false;
    for(int i = 0; i < _length; ++i)
      if(_buf[_off+i] != str._buf[str._off+i]) return false;
    return true;
  }
}
