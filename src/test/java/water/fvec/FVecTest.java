package water.fvec;

import static org.junit.Assert.*;
import java.io.File;
import java.util.Arrays;
import org.junit.*;
import water.*;
import water.nbhm.NonBlockingHashMap;
import water.util.Log;
import java.util.concurrent.atomic.AtomicInteger;

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
    File file = TestUtil.find_test_file("../wiki/enwiki-latest-pages-articles.xml");
    Key key = NFSFileVec.make(file);
    NFSFileVec nfs=DKV.get(key).get();
    NonBlockingHashMap<VStr,VStr> words = new WordCount().invoke(nfs)._words;
    Log.unwrap(System.err,words.toString());
    UKV.remove(key);
  }

  private static class WordCount extends MRTask2<WordCount> {
    public static NonBlockingHashMap<VStr,VStr> WORDS;
    public NonBlockingHashMap<VStr,VStr> _words;
    @Override public void init() { WORDS = new NonBlockingHashMap(); }
    @Override public void map( long start, int len, BigVector bv ) {
      _words = WORDS;
      long i = start;
      if( start > 0 )           // Not on the 1st chunk...
        while( Character.isLetter((char)bv.at(i)) ) i++; // skip any partial word from prior
      VStr vs = new VStr();
      while( i<start+len ) {    // Till we run dry
        int c = (int)bv.at(i);  // Load a char
        if( Character.isLetter(c) ) { // In a word?
          vs.append(c);         // Append char
        } else {                // Out of a word?
          if( vs._len > 0 ) {   // Have a word?
            VStr vs2 = WORDS.putIfAbsent(vs,vs);
            if( vs2 == null ) {
              vs = new VStr();  // If actually inserted, need new VStr
            } else {
              vs2.inc();        // Inc count on added word, 
              vs._len = 0;      // and re-use VStr
            }
          }
        }
        i++;
      }
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
    @Override public void copyOver(DTask that) { 
      throw H2O.unimpl();
    }    
  }

  // A word, and a count of occurences
  private static class VStr {
    byte[] _mem = new byte[1];
    int _len = 0;
    final AtomicInteger _cnt = new AtomicInteger(1);
    public void append( int c ) {
      if( _len == _mem.length ) 
        _mem = Arrays.copyOf(_mem,_len<<1);
      _mem[_len++] = (byte)c;
    }
    public String toString() { return new String(_mem,0,_len)+"="+_cnt.get(); }
    void inc() { _cnt.addAndGet(1); }
    @Override public boolean equals(Object o){
      if(!(o instanceof VStr)) return false;
      VStr vs = (VStr)o;
      if( vs._len != _len)return false;
      for(int i = 0; i < _len; ++i)
        if(_mem[i] != vs._mem[i]) return false;
      return true;
    }
    @Override public int hashCode() {
     int hash = 0;
     for(int i = 0; i < _len; ++i)
       hash = 31 * hash + _mem[i];
     return hash;
    }
  }

}
