package water.parser;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

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
public final class Enum extends Iced implements Cloneable{
  public static final int MAX_ENUM_SIZE = 65000;
  AtomicInteger _id = new AtomicInteger();
  int _maxId = -1;
  long _nElems;
  volatile NonBlockingHashMap<ValueString, Integer> _map;
  public Enum(){_map = new NonBlockingHashMap<ValueString, Integer>();}
  private Enum(int id, long nElems, NonBlockingHashMap<ValueString,Integer>map){
    _id = new AtomicInteger(id);
    _nElems = nElems;
    _map = map;
  }
  public Enum clone(){
    NonBlockingHashMap<ValueString,Integer> map = _map;
    if(map != null)map = (NonBlockingHashMap<ValueString,Integer>)map.clone();
    return new Enum(_id.get(),_nElems,map);
  }
  /**
   * Add key to this map (treated as hash set in this case).
   * All keys are added with value = 1.
   * @param str
   */
  public int addKey(ValueString str) {
    // _map is shared and be cast to null (if enum is killed) -> grab local copy
    NonBlockingHashMap<ValueString, Integer> m = _map;
    if( m == null ) return Integer.MAX_VALUE;     // Nuked already
    Integer res = m.get(str);
    if(res != null ) return res; // Recorded already
    assert str._length < 65535;      // Length limit so 65535 can be used as a sentinel
    Integer newVal = new Integer(_id.incrementAndGet());
    res = m.putIfAbsent(new ValueString(Arrays.copyOfRange(str._buf, str._off, str._off + str._length)), newVal);
    if(res != null)return res;
    if(m.size() > MAX_ENUM_SIZE){
      kill();
      return Integer.MAX_VALUE;
    }
    return newVal;
  }
  public final boolean containsKey(Object key){return _map.containsKey(key);}
  public void addKey(String str) {
    addKey(new ValueString(str));
  }

  public int getTokenId(String str) {
    return getTokenId(new ValueString(str));
  }
  public String toString(){
    StringBuilder sb = new StringBuilder("{");
    for(Entry e: _map.entrySet())sb.append(" " + e.getKey().toString() + "->" + e.getValue().toString());
    sb.append(" }");
    return sb.toString();
  }
  public long addedElems(){return _nElems;}

  public int getTokenId(ValueString str){
    Integer I = _map.get(str);
    assert I != null : "missing value! " + str.toString();
    return I;
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
  public int maxId(){return _maxId == -1?_id.get():_maxId;}
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
    if( _map == null ) return ab.put1(1); // Killed map marker
    ab.put1(0);                           // Not killed
    ab.put4(maxId());
    for( ValueString key : _map.keySet() )
      ab.put2((char)key._length).putA1(key._buf,key._length).put4(_map.get(key));
    return ab.put2((char)65535); // End of map marker
  }

  public Enum read( AutoBuffer ab ) {
    assert _map == null || _map.size()==0;
    _map = null;
    if( ab.get1() == 1 ) return this; // Killed?
    _maxId = ab.get4();
    _map = new NonBlockingHashMap<ValueString, Integer>();
    int len = 0;
    while( (len = ab.get2()) != 65535 ) // Read until end-of-map marker
      _map.put(new ValueString(ab.getA1(len)),ab.get4());
    return this;
  }
}
