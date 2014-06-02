package hex;

import static org.junit.Assert.assertArrayEquals;
import static water.util.Utils.nfold;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import water.*;
import water.fvec.Frame;
import water.util.Utils;

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

  @Test
  public void testIris() {
    Key key = Key.make("iris.hex");
    Frame fr = parseFrame(key, "./smalldata/iris/iris.csv");
    int[] nfolds = new int[] {2,3,10,11};
    long nrows = fr.numRows();
    try {
      for (int i=0; i<nfolds.length; i++) {
        int n = nfolds[i];
        for (int f=0; f<n; f++) {
          Frame[] splits = null;
          try {
            NFoldFrameExtractor nffe = new NFoldFrameExtractor(fr, n, f, null, null);
            H2O.submitTask(nffe);
            splits = nffe.getResult();
            Arrays.deepToString(splits);
            Assert.assertEquals("N-Fold extract should always produce 2 frames!", 2, splits.length);
            Assert.assertEquals("N-Fold extract should not modify input frame!", nrows, fr.numRows());
            Assert.assertEquals(nrows, splits[0].numRows()+splits[1].numRows());
          } finally {
            if (splits!=null) for (Frame fs: splits) fs.delete();
          }
        }
      }
    } finally {
      if (fr!=null) fr.delete();
    }
  }
}
