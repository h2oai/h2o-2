package water.fvec;

import static org.junit.Assert.*;
import java.io.File;
import org.junit.*;
import water.*;
import water.util.Log;

public class FVecTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(2); }

  @Test public void testBasicCRUD() {
    // Make and insert a FileVec to the global store
    //File file = TestUtil.find_test_file("./smalldata/cars.csv");
    File file = TestUtil.find_test_file("./target/h2o.jar");
    Key key = NFSFileVec.make(file);
    NFSFileVec nfs=DKV.get(key).get();

    int[] x = new ByteHisto().invoke(nfs)._x;
    int sum=0;
    for( int i : x )
      sum += i;
    assertEquals(file.length(),sum);

    UKV.remove(key);
  }

  public static class ByteHisto extends MRTask2<ByteHisto> {
    public int[] _x;
    // Count occurrences of bytes
    @Override public void map( long start, CVec cvec ) {
      _x = new int[256];        // One-time set histogram array
      int len = cvec.length();
      for( int i=0; i<len; i++ )
        _x[(int)cvec.at(i)]++;
    }
    // ADD together all results
    @Override public void reduce( ByteHisto bh ) {
      for( int i=0; i<_x.length; i++ )
        _x[i] += bh._x[i];
    }
  }
}
