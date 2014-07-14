package water.fvec;
import org.junit.Assert;
import org.junit.Test;
import water.TestUtil;

import java.util.Arrays;

public class C0LChunkTest extends TestUtil {
  @Test
  public void test_inflate_impl() {
    final int K = 1<<18;
    for (long l : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 23420384l, 0l, -23423423400023l}) {
      NewChunk nc = new NewChunk(null, 0);
      for (int i=0;i<K;++i) nc.addNum(l,0);
      Assert.assertEquals(K, l == 0l ? nc.len2() : nc.len()); //special case for sparse length

      Chunk cc = nc.compress();
      Assert.assertEquals(K, cc.len());
      Assert.assertTrue(cc instanceof C0LChunk);
      for (int i=0;i<K;++i) Assert.assertEquals(l, cc.at80(i));

      Chunk cc2 = cc.inflate_impl(new NewChunk(null, 0)).compress();
      Assert.assertEquals(K, cc2.len());
      Assert.assertTrue(cc2 instanceof C0LChunk);
      for (int i=0;i<K;++i) Assert.assertEquals(l, cc2.at80(i));

      Assert.assertTrue(Arrays.equals(cc._mem, cc2._mem));
    }
  }
}

