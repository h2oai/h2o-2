package hex;

import static org.junit.Assert.assertArrayEquals;
import static water.util.Utils.nfold;

import org.junit.Test;

import water.TestUtil;

public class NFoldFrameExtractorTest extends TestUtil {

  @Test
  public void testNFoldSplitUtility() {
    // 10-fold
    for (int i=0; i<10; i++) {
      assertArrayEquals("10 fold of 10 elements : "+i+"-th failed!", ar(i,1L), nfold(10, 10, i));
    }
    // 10-fold
    for (int i=0; i<9; i++) {
      assertArrayEquals("10 fold of 11 elements : "+i+"-th failed!", ar(i,1L), nfold(11, 10, i));
    }
    assertArrayEquals("10 fold of 11 elements : 9-th failed!", ar(9,2L), nfold(11, 10, 9));
  }

  @Test
  public void testEspcSplit() {
    NFoldFrameExtractor fe = null;
    long [][] espc = null;
    // N-fold split - test on the chunk boundary split - start/end are at chunk boundaries
    for (int i=0; i<3; i++) {
      fe = new NFoldFrameExtractor(null, 3, i, null, null);
      espc = fe.computeEspcPerSplit(ar(0,2,4,6,8,10,12), 12L);
      assertArrayEquals(ar(0L, 2L, 4L, 6L, 8L), espc[0]);
      assertArrayEquals(ar(0L, 2L, 4L), espc[1]);
    }
    // Split inside chunk
    fe = new NFoldFrameExtractor(null, 3, 1, null, null);
    espc = fe.computeEspcPerSplit(ar(0,2,4,6,8,10), 10L);
    assertArrayEquals(ar(0L, 2L, 3L, 5L, 7L), espc[0]);
    assertArrayEquals(ar(0L, 1L, 3L), espc[1]);

    // Split inside chunk
    fe = new NFoldFrameExtractor(null, 3, 0, null, null);
    espc = fe.computeEspcPerSplit(ar(0,3,6), 6L);
    assertArrayEquals(ar(0L, 1L, 4L), espc[0]);
    assertArrayEquals(ar(0L, 2L), espc[1]);
    fe = new NFoldFrameExtractor(null, 3, 1, null, null);
    espc = fe.computeEspcPerSplit(ar(0,3,6), 6L);
    assertArrayEquals(ar(0L, 2L, 4L), espc[0]);
    assertArrayEquals(ar(0L, 1L, 2L), espc[1]);
    fe = new NFoldFrameExtractor(null, 3, 2, null, null);
    espc = fe.computeEspcPerSplit(ar(0,3,6), 6L);
    assertArrayEquals(ar(0L, 3L, 4L), espc[0]);
    assertArrayEquals(ar(0L, 2L), espc[1]);

    // Test scenario that fold split one chunk into 3 parts
    fe = new NFoldFrameExtractor(null, 3, 0 , null, null);
    espc = fe.computeEspcPerSplit(ar(0,6), 6L);
    assertArrayEquals(ar(0L, 4L), espc[0]);
    assertArrayEquals(ar(0L, 2L), espc[1]);
    fe = new NFoldFrameExtractor(null, 3, 1 , null, null);
    espc = fe.computeEspcPerSplit(ar(0,6), 6L);
    assertArrayEquals(ar(0L, 2L, 4L), espc[0]);
    assertArrayEquals(ar(0L, 2L), espc[1]);
    fe = new NFoldFrameExtractor(null, 3, 2 , null, null);
    espc = fe.computeEspcPerSplit(ar(0,6), 6L);
    assertArrayEquals(ar(0L, 4L), espc[0]);
    assertArrayEquals(ar(0L, 2L), espc[1]);

  }
}
