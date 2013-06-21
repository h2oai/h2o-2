package water.fvec;

import hex.GBM;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.Test;
import water.Key;
import water.TestUtil;
import water.UKV;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ==========================================================================
  @Test public void testBasicGBM() {
    File file = TestUtil.find_test_file("../smalldata/logreg/prostate.csv");
    Key fkey = NFSFileVec.make(file);

    Frame fr = ParseDataset2.parse(Key.make("prostate.hex"),new Key[]{fkey});
    UKV.remove(fkey);
    System.out.println("Frame="+fr);
    GBM gbm = GBM.start(GBM.makeKey(),fr);
    gbm.get();                  // Block for result
    UKV.remove(gbm._dest);
    UKV.remove(fr._key);
  }
}
