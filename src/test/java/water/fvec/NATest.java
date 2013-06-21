package water.fvec;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;

public class NATest extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }
  @Test public void testNAs() {
    File file = find_test_file("./smalldata/test/na_test.zip");
    Key key = NFSFileVec.make(file);
    Key okey = Key.make("na_test.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{key});
    int nlines = (int)fr._vecs[0].length();
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
