package water.fvec;

import org.junit.Assert;
import org.junit.Test;
import water.TestUtil;

import java.util.Arrays;
import java.util.Iterator;

public class CXDChunkTest extends TestUtil {
  @Test
  public void test_inflate_impl() {
    for (int l=0; l<2; ++l) {
      NewChunk nc = new NewChunk(null, 0);

      double[] vals = new double[]{0, 0, 0, Double.MAX_VALUE, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, Double.MIN_VALUE, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

      if (l==1) nc.addNA();
      for (double v : vals) nc.addNum(v);
      nc.addNA();

      Chunk cc = nc.compress();
      Assert.assertEquals(vals.length + 1 + l, cc.len());
      Assert.assertTrue(cc instanceof CXDChunk);
      if (l==1) Assert.assertTrue(cc.isNA0(0));
      for (int i = 0; i < vals.length; ++i) Assert.assertEquals(vals[i], cc.at0(i+l), 0);
      Assert.assertTrue(cc.isNA0(vals.length+l));

      nc = new NewChunk(null, 0);
      cc.inflate_impl(nc);
      Assert.assertEquals(vals.length+l+1, nc.len2());
      Assert.assertEquals(2+1+l, nc.len());
      Iterator<NewChunk.Value> it = nc.values(0, vals.length+l+1);
      if (l==1) Assert.assertTrue(it.next().rowId0() == 0);
      Assert.assertTrue(it.next().rowId0() == 3+l);
      Assert.assertTrue(it.next().rowId0() == 101+l);
      Assert.assertTrue(it.next().rowId0() == vals.length+l);
      Assert.assertTrue(!it.hasNext());
      if (l==1) Assert.assertTrue(nc.isNA0(0));
      for (int i = 0; i < vals.length; ++i) Assert.assertEquals(vals[i], nc.at0(l+i), 0);
      Assert.assertTrue(nc.isNA0(vals.length+l));

      Chunk cc2 = nc.compress();
      Assert.assertEquals(vals.length+1+l, cc.len());
      Assert.assertTrue(cc2 instanceof CXDChunk);
      if (l==1) Assert.assertTrue(cc2.isNA0(0));
      for (int i = 0; i < vals.length; ++i) Assert.assertEquals(vals[i], cc2.at0(i+l), 0);
      Assert.assertTrue(cc2.isNA0(vals.length+l));

      Assert.assertTrue(Arrays.equals(cc._mem, cc2._mem));
    }
  }
}
