package water.parser;

import java.util.*;
import water.AutoBuffer;
import water.Iced;
import water.nbhm.NonBlockingHashMap;

/**
 * Class for tracking enum columns.
 *
 * Basically a wrapper around non blocking hash map.
 * In the first pass, we just collect set of unique strings per column
 * (if there are less than MAX_ENUM_SIZE unique elements).
 *
 * After pass1, the keys are sorted and indexed alphabetically.
 * In the second pass, map is used only for lookup and never updated.
 *
 * Enum objects are shared among threads on the local nodes!
 *
 * @author tomasnykodym
 *
 */
public final class Enum extends Iced {

  public static final int MAX_ENUM_SIZE = 65535;

  volatile NonBlockingHashMap<ValueString, Integer> _map;

  public Enum(){
    _map = new NonBlockingHashMap<ValueString, Integer>();
  }

  /**
   * Add key to this map (treated as hash set in this case).
   * All keys are added with value = 1.
   * @param str
   */
  void addKey(ValueString str) {
    // _map is shared and be cast to null (if enum is killed) -> grab local copy
    Map<ValueString, Integer> m = _map;
    if( m == null ) return;     // Nuked already
    if( m.get(str) != null ) return; // Recorded already
    assert str._length < 65535;      // Length limit so 65535 can be used as a sentinel
    m.put(new ValueString(Arrays.copyOfRange(str._buf, str._off, str._off + str._length)), 1);
    if(m.size() > MAX_ENUM_SIZE)
      kill();
  }

  public void addKey(String str) {
    addKey(new ValueString(str));
  }

  public int getTokenId(String str) {
    return getTokenId(new ValueString(str));
  }

  int getTokenId(ValueString str){
    assert _map.get(str) != null:"missing value! " + str.toString();
    return _map.get(str);
  }

  public void merge(Enum other){
    if( this == other ) return;
    if( isKilled() ) return;
    if( !other.isKilled() ) {   // do the merge
      Map<ValueString, Integer> myMap = _map;
      Map<ValueString, Integer> otMap = other._map;
      if( myMap == otMap ) return;
      for( ValueString str : otMap.keySet() )
        myMap.put(str, 1);
      if( myMap.size() <= MAX_ENUM_SIZE ) return;
    }
    kill(); // too many values, enum should be killed!
  }
  public int size() { return _map.size(); }
  public boolean isKilled() { return _map == null; }
  public void kill() { _map = null; }

  // assuming single threaded
  public String [] computeColumnDomain(){
    if( isKilled() ) return null;
    String [] res = new String[_map.size()];
    Map<ValueString, Integer> oldMap = _map;
    Iterator<ValueString> it = oldMap.keySet().iterator();
    for( int i = 0; i < res.length; ++i )
      res[i] = it.next().toString();
    Arrays.sort(res);
    NonBlockingHashMap<ValueString, Integer> newMap = new NonBlockingHashMap<ValueString, Integer>();
    for( int j = 0; j < res.length; ++j )
      newMap.put(new ValueString(res[j]), j);
    oldMap.clear();
    _map = newMap;
    return res;
  }

  // Since this is a *concurrent* hashtable, writing it whilst its being
  // updated is tricky.  If the table is NOT being updated, then all is written
  // as expected.  If the table IS being updated we only promise to write the
  // Keys that existed at the time the table write began.  If elements are
  // being deleted, they may be written anyways.  If the Values are changing, a
  // random Value is written.
  public AutoBuffer write( AutoBuffer ab ) {
    if( _map != null )
      for( ValueString key : _map.keySet() )
        ab.put2((char)key._length).putA1(key._buf,key._length).put4(_map.get(key));
    return ab.put2((char)65535); // End of map marker
  }

  public Enum read( AutoBuffer ab ) {
    assert _map == null || _map.size()==0;
    _map = new NonBlockingHashMap<ValueString, Integer>();
    int len = 0;
    while( (len = ab.get2()) != 65535 ) // Read until end-of-map marker
      _map.put(new ValueString(ab.getA1(len)),ab.get4());
    return this;
  }
}
