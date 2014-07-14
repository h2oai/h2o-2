package water.fvec;

import org.junit.Assert;
import org.junit.Test;
import water.TestUtil;

import java.util.Arrays;

public class C1NChunkTest extends TestUtil {
  @Test
  public void test_inflate_impl() {
    NewChunk nc = new NewChunk(null, 0);

    int[] vals = new int[]{0,1,3,254};
    for (int v : vals) nc.addNum(v,0);

    Chunk cc = nc.compress();
    Assert.assertEquals(vals.length, cc.len());
    Assert.assertTrue(cc instanceof C1NChunk);
    for (int i=0;i<vals.length;++i) Assert.assertEquals(vals[i], cc.at80(i));

    Chunk cc2 = cc.inflate_impl(new NewChunk(null, 0)).compress();
    Assert.assertEquals(vals.length, cc.len());
    Assert.assertTrue(cc2 instanceof C1NChunk);
    for (int i=0;i<vals.length;++i) Assert.assertEquals(vals[i], cc2.at80(i));

    Assert.assertTrue(Arrays.equals(cc._mem, cc2._mem));
  }
}
