package water.fvec;

import static org.junit.Assert.*;
import java.io.File;
import java.util.Arrays;
import org.junit.*;
import water.*;
import water.nbhm.NonBlockingHashMap;
import water.util.Log;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class FVecTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  @Test public void testBasicCRUD() {
    // Make and insert a FileVec to the global store
    File file = TestUtil.find_test_file("./smalldata/cars.csv");
    Key key = NFSFileVec.make(file);
    NFSFileVec nfs=DKV.get(key).get();

    int[] x = new ByteHisto().invoke(nfs)._x;
    int sum=0;
    for( int i : x )
      sum += i;
    assertEquals(file.length(),sum);

    UKV.remove(key);
  }

  private static class ByteHisto extends MRTask2<ByteHisto> {
    public int[] _x;
    // Count occurrences of bytes
    @Override public void map( long start, int len, BigVector bv ) {
      _x = new int[256];        // One-time set histogram array
      for( long i=start; i<start+len; i++ )
        _x[(int)bv.at(i)]++;
    }
    // ADD together all results
    @Override public void reduce( ByteHisto bh ) {
      for( int i=0; i<_x.length; i++ )
        _x[i] += bh._x[i];
    }
  }


  @Test public void testWordCount() {
    //File file = TestUtil.find_test_file("./smalldata/cars.csv");
    File file = TestUtil.find_test_file("../wiki/enwiki-latest-pages-articles.xml");
    Key key = NFSFileVec.make(file);
    NFSFileVec nfs=DKV.get(key).get();
    NonBlockingHashMap<VStr,VStr> words = new WordCount().invoke(nfs)._words;
    VStr[] vss = words.keySet().toArray(new VStr[words.size()]);
    Arrays.sort(vss);
    Log.unwrap(System.err,Arrays.toString(vss));
    UKV.remove(key);
  }

  private static class WordCount extends MRTask2<WordCount> {
    public static NonBlockingHashMap<VStr,VStr> WORDS;
    public static AtomicInteger CHUNKS = new AtomicInteger();
    public NonBlockingHashMap<VStr,VStr> _words;
    @Override public void init() { WORDS = new NonBlockingHashMap(); }
    @Override public void map( long start, int len, BigVector bv ) {
      _words = WORDS;

      long i = start;           // Parse point
      // Skip partial words at the start of chunks, assuming they belong to the
      // trailing end of the prior chunk.
      if( start > 0 )           // Not on the 1st chunk...
        while( Character.isLetter((char)bv.at(i)) ) i++; // skip any partial word from prior
      int waste=0, used=256;
      VStr vs = new VStr(new byte[used],(short)0);
      // Loop over the chunk, picking out words
      while( i<start+len || vs._len > 0 ) { // Till we run dry & not in middle of word
        int c = (int)bv.at(i);  // Load a char
        if( Character.isLetter(c) ) { // In a word?
          assert vs._len<256 : "Too long: "+this+" at char "+i;
          int w=vs.append(c);         // Append char
          if( w > 0 ) { used += vs._cs.length+8; waste+=w+8; }
        } else if( vs._len > 0 ) {    // Have a word?
          VStr vs2 = WORDS.putIfAbsent(vs,vs);
          if( vs2 == null ) {   // If actually inserted, need new VStr
            if( (WORDS.size()&65535)==0 )
              Log.unwrap(System.err,PrettyPrint.bytes(WORDS.size())+" "+vs);
            vs = new VStr(vs._cs,(short)(vs._off+vs._len)); 
          } else {
            vs2.inc();        // Inc count on added word, 
            vs._len = 0;      // and re-use VStr
          }
        }
        i++;
      }
      if( vs._off > 0 ) waste += vs._cs.length-vs._off; // keep partial last charspace
      else              used -= (vs._cs.length+8);      // drop unused  last charspace
      CHUNKS.addAndGet(1);
      Log.unwrap(System.err,
                 "total="+PrettyPrint.bytes(((long)CHUNKS.get())<<ValueArray.LOG_CHK)+
                 //", map row="+PrettyPrint.bytes(start)+
                 ", chars pad/tot="+waste+"/"+used);
    }

    @Override public void reduce( WordCount wc ) {
      if( _words != wc._words )
        throw H2O.unimpl();
    }

    @Override public AutoBuffer write(AutoBuffer bb) { 
      throw H2O.unimpl();
    }
    @Override public WordCount read(AutoBuffer bb) { 
      throw H2O.unimpl();
    }
    @Override public void copyOver(DTask wc) { _words = ((WordCount)wc)._words; }    
  }


  // A word, and a count of occurences
  private static class VStr implements Comparable<VStr> {
    byte[] _cs;                 // shared array of chars holding words
    short _off,_len;            // offset & len of this word
    VStr(byte[]cs, short off) { assert off>=0:off; _cs=cs; _off=off; _len=0; _cnt=1; }
    // append a char; return wasted pad space
    public int append( int c ) {
      int waste=0;
      if( _off+_len == _cs.length ) { // no room for word?
        waste=_len;             // will recopy to new buffer, so all is wasted
        byte[] cs = new byte[(_cs.length<<1)<1024?(_cs.length<<1):1024];
        System.arraycopy(_cs,_off,cs,0,_len);
        _off=0;
        _cs = cs;
      }
      _cs[_off+_len++] = (byte)c;
      return waste;
    }
    volatile int _cnt;          // Atomically update
    private static final AtomicIntegerFieldUpdater<VStr> _cntUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VStr.class, "_cnt");
    void inc() {
      int r = _cnt;
      while( !_cntUpdater.compareAndSet(this,r,r+1) )
        r = _cnt;
    }

    public String toString() { return new String(_cs,_off,_len)+"="+_cnt; }

    @Override public int compareTo(VStr vs) {
      int f = vs._cnt - _cnt; // sort by freq
      if( f != 0 ) return f;
      // alpha-sort, after tied on freq
      int len = Math.min(_len,vs._len);
      for(int i = 0; i < len; ++i)
        if(_cs[_off+i] != vs._cs[vs._off+i]) 
          return _cs[_off+i]-vs._cs[vs._off+i];
      return _len - vs._len;
    }
    @Override public boolean equals(Object o){
      if(!(o instanceof VStr)) return false;
      VStr vs = (VStr)o;
      if( vs._len != _len)return false;
      for(int i = 0; i < _len; ++i)
        if(_cs[_off+i] != vs._cs[vs._off+i]) return false;
      return true;
    }
    @Override public int hashCode() {
     int hash = 0;
     for(int i = 0; i < _len; ++i)
       hash = 31 * hash + _cs[_off+i];
     return hash;
    }
  }

}
