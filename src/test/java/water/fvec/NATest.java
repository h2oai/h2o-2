package water.fvec;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;



public class NATest extends TestUtil {
  static Class [] expectedTypes = new Class[]{C1Chunk.class,C1SChunk.class,C2Chunk.class,C2SChunk.class,C4Chunk.class,C4FChunk.class,C8Chunk.class,C8DChunk.class};
  int h = Integer.MAX_VALUE;
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }
  @Test public void testNAs() {
    File file = find_test_file("./smalldata/test/na_test.zip");
    Key key = NFSFileVec.make(file);
    Key okey = Key.make("na_test.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{key});
    int nlines = (int)fr._vecs[0].length();
    assertTrue(fr._vecs.length == expectedTypes.length);
    for(int i = 0; i < expectedTypes.length; ++i){
      assertTrue(expectedTypes[i].isInstance(fr._vecs[i].elem2BV(0)));
    }
    for(int i = 0; i < nlines-1; ++i)
      for(Vec v:fr._vecs)
        assertTrue(!v.isNA(v.getd(i)) && !v.isNA(v.get(i)));
    for(Vec v:fr._vecs){
      assertTrue(v.isNA(v.getd(nlines-1)) && v.isNA(v.get(nlines-1)));
      v.replaceNAs(1.0, 2);
      assertTrue(!v.isNA(v.getd(nlines-1)) && !v.isNA(v.get(nlines-1)));
      assertTrue(v.getd(nlines-1) == 1.0 && v.get(nlines-1) == 2);
      v.setNAs(3.0, 4);
      assertTrue(v.isNA(v.getd(nlines-1)) && v.isNA(v.get(nlines-1)));
      assertTrue(v.getd(nlines-1) == 3.0 && v.get(nlines-1) == 4);
    }
    UKV.remove(key);
    UKV.remove(okey);
  }
  public static void main(String [] args){
    new NATest().testNAs();
    new NewVectorTest().testCompression();
  }
}