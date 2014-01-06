package water;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import water.fvec.Frame;

public class BitCmpTest extends TestUtil {
  @Test public void  testBitIdenticalTask(){
    Key k1 = null,k2 = null,k3 = null;
    try {
      Frame fr1 = parseFrame(k1=Key.make("k1"),"smalldata/stego/stego_training.data");
      Frame fr2 = parseFrame(k2=Key.make("k2"),"smalldata/stego/stego_training.data");
      Frame fr3 = parseFrame(k3=Key.make("k3"),"smalldata/stego/stego_training_modified.data");
      assertTrue(fr1.dataEquals(fr2));
      assertTrue(fr2.dataEquals(fr1));
      assertFalse(fr1.dataEquals(fr3));
      assertFalse(fr3.dataEquals(fr2));
    } finally {
      if(k1 != null)UKV.remove(k1);
      if(k2 != null)UKV.remove(k2);
      if(k3 != null)UKV.remove(k3);
    }
  }
}
