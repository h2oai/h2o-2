package hex;

import static org.junit.Assert.assertEquals;
import hex.gbm.GBM;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.Test;
import water.Key;
import water.TestUtil;
import water.UKV;
import water.fvec.*;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ==========================================================================
  /*@Test*/ public void testBasicGBM() {
    File file = TestUtil.find_test_file("./smalldata/logreg/prostate.csv");
    Key fkey = NFSFileVec.make(file);
    Frame fr = ParseDataset2.parse(Key.make("prostate.hex"),new Key[]{fkey});
    UKV.remove(fkey);
    try {
      assertEquals(380,fr._vecs[0].length());
      
      // Prostate: predict on CAPSULE which is in column #1; move it to last column
      int ncols = fr._names.length;
      String s = fr._names[1];    // capsule
      Vec v    = fr._vecs [1];
      System.arraycopy(fr._names,2,fr._names,1,ncols-2);
      System.arraycopy(fr._vecs ,2,fr._vecs ,1,ncols-2);
      fr._names[ncols-1] = s;
      fr._vecs [ncols-1] = v;
      
      GBM gbm = GBM.start(GBM.makeKey(),fr);
      gbm.get();                  // Block for result
      UKV.remove(gbm._dest);
    } finally {
      UKV.remove(fr._key);
    }
  }
}
