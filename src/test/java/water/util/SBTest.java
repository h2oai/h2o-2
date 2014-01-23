package water.util;

import org.junit.Assert;
import org.junit.Test;

/** Test set for {@link SB} pretty printer. */
public class SBTest {

  @Test
  public void testFloatSerialization() {
    Assert.assertEquals("3.1415f", new SB().pj(3.1415f).toString());
    Assert.assertEquals("Float.NaN", new SB().pj(Float.NaN).toString());
    Assert.assertEquals("Float.POSITIVE_INFINITY", new SB().pj(Float.POSITIVE_INFINITY).toString());
    Assert.assertEquals("Float.NEGATIVE_INFINITY", new SB().pj(Float.NEGATIVE_INFINITY).toString());
  }
}
