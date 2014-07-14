package water.fvec;

import org.junit.Assert;
import org.junit.Test;
import water.TestUtil;

import java.util.Arrays;

public class C4ChunkTest extends TestUtil {
  @Test
  public void test_inflate_impl() {
    for (int l=0; l<2; ++l) {
      NewChunk nc = new NewChunk(null, 0);

      int[] vals = new int[]{-2147483647, 0, 2147483647};
      if (l==1) nc.addNA();
      for (int v : vals) nc.addNum(v, 0);
      nc.addNA(); //-2147483648

      Chunk cc = nc.compress();
      Assert.assertEquals(vals.length + 1 + l, cc.len());
      Assert.assertTrue(cc instanceof C4Chunk);
      for (int i = 0; i < vals.length; ++i) Assert.assertEquals(vals[i], cc.at80(l+i));
      Assert.assertTrue(cc.isNA0(vals.length+l));

      Chunk cc2 = cc.inflate_impl(new NewChunk(null, 0)).compress();
      Assert.assertEquals(vals.length + 1 + l, cc.len());
      Assert.assertTrue(cc2 instanceof C4Chunk);
      for (int i = 0; i < vals.length; ++i) Assert.assertEquals(vals[i], cc2.at80(l+i));
      Assert.assertTrue(cc2.isNA0(vals.length+l));

      Assert.assertTrue(Arrays.equals(cc._mem, cc2._mem));
    }
  }
}
