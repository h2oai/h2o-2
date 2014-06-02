package hex;

import static org.junit.Assert.assertArrayEquals;
import static water.util.Utils.nfold;

import org.junit.Test;

import water.TestUtil;

public class NFoldFrameExtractorTest extends TestUtil {

  @Test
  public void testNFoldSplitUtility() {
    for (int i=0; i<10; i++) {
      assertArrayEquals(ar(i,12L), nfold(10, 10, i));
    }
  }
}
