package water.fvec;

import org.junit.Assert;
import org.junit.Test;
import water.TestUtil;

import java.util.Arrays;

public class CXIChunkTest extends TestUtil {
  @Test
  public void test_inflate_impl() {
    for (int l=0; l<2; ++l) {
      NewChunk nc = new NewChunk(null, 0);

      int[] vals = new int[]{0, 0, 0, Integer.MAX_VALUE, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, Integer.MIN_VALUE+1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

      if (l==1) nc.addNA();
      for (int v : vals) nc.addNum(v, 0);
      nc.addNA();

      Chunk cc = nc.compress();
      Assert.assertEquals(vals.length + 1 + l, cc.len());
      Assert.assertTrue(cc instanceof CXIChunk);
      if (l==1) Assert.assertTrue(cc.isNA0(0));
      for (int i = 0; i < vals.length; ++i) Assert.assertEquals(vals[i], cc.at80(i+l));
      Assert.assertTrue(cc.isNA0(vals.length+l));

      Chunk cc2 = cc.inflate_impl(new NewChunk(null, 0)).compress();
      Assert.assertEquals(vals.length+1+l, cc.len());
      Assert.assertTrue(cc2 instanceof CXIChunk);
      if (l==1) Assert.assertTrue(cc2.isNA0(0));
      for (int i = 0; i < vals.length; ++i) Assert.assertEquals(vals[i], cc2.at80(i+l));
      Assert.assertTrue(cc2.isNA0(vals.length+l));

      Assert.assertTrue(Arrays.equals(cc._mem, cc2._mem));
    }
  }
}
