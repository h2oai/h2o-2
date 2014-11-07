package water.parser;

import water.AutoBuffer;
import water.Freezable;
import water.Iced;
import water.nbhm.NonBlockingHashMap;

import java.util.Map;


public final class Doc extends Iced implements Cloneable, Freezable{
  String _label;
  NonBlockingHashMap<ValueString, Long> _terms;
  long _nElems;

  public Doc(){_terms = new NonBlockingHashMap<ValueString, Long>();}
  private Doc(String label, NonBlockingHashMap<ValueString,Long> map){
    _label = label;
    _terms = map;
    _nElems = map.size();
  }
  public Doc clone() {
    NonBlockingHashMap<ValueString,Long> map = _terms;
    if(map != null)map = (NonBlockingHashMap<ValueString,Long>)map.clone();
    return new Doc(_label,map);
  }
  /**
   * Add key to this map (treated as hash set in this case).
   * All keys are added with value = 1.
   * @param str
   */
  public long addKey(ValueString str) {
    // _terms is shared and be cast to null (if enum is killed) -> grab local copy
    NonBlockingHashMap<ValueString, Long> m = _terms;
    if( m == null ) return Integer.MAX_VALUE;     // Nuked already
    long res = -1;
    if (m.containsKey(str)) res = m.get(str);
    m.put(str, res == -1 ? 1 : res+1);
    _nElems = m.size();
    return res;
  }

  public void put(ValueString str, long res) { _terms.put(str, res); }

  public final boolean containsKey(ValueString key){return _terms.containsKey(key);}
  public void addKey(String str) { addKey(new ValueString(str)); }
  public long getTokenValue(String str) { return getTokenValue(new ValueString(str)); }
  public long addedElems(){return _nElems;}
  public long getTokenValue(ValueString str){ return _terms.get(str); }
  public int size() { return _terms.size(); }
  public void merge(Doc other){
    if( this == other ) return;
    Map<ValueString, Long> myMap = _terms;
    Map<ValueString, Long> otMap = other._terms;
    if( myMap == otMap ) return;
    for( ValueString str : otMap.keySet() ) {
      if (myMap.containsKey(str)) {
        long res = myMap.get(str);
        long ores=otMap.get(str);
        myMap.put(str, res + ores);
//        if (res+ores > 2) myMap.put(str,res+ores);
//        else myMap.remove(str); //trim out the word otherwise
      } else addKey(str);
    }
  }

  // Since this is a *concurrent* hashtable, writing it whilst its being
  // updated is tricky.  If the table is NOT being updated, then all is written
  // as expected.  If the table IS being updated we only promise to write the
  // Keys that existed at the time the table write began.  If elements are
  // being deleted, they may be written anyways.  If the Values are changing, a
  // random Value is written.
  @Override public AutoBuffer write( AutoBuffer ab ) {
    super.write(ab);
    if( _terms == null ) return ab.put4(0);
    ab.put4(_terms.size());
    for( ValueString key : _terms.keySet() )
      ab.put2((char)key.get_length()).putA1(key.get_buf(),key.get_length()).put8(_terms.get(key));
    return ab;
  }

  @Override public Doc read( AutoBuffer ab ) {
    super.read(ab);
    assert _terms == null || _terms.size()==0;
    _terms = null;
    int len = ab.get4();
    if (len == 0) return this;
    _terms = new NonBlockingHashMap<ValueString, Long>();
    for (int i = 0; i < len; ++i) {
      int l = ab.get2();
      _terms.put(new ValueString(ab.getA1(l)), ab.get8());
    }
    return this;
  }
}
