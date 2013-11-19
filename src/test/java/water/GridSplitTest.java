package water;

import junit.framework.Assert;

import org.junit.Test;

public class GridSplitTest {
  @Test public void testBasic() {
    Assert.assertEquals(Request2.split("4").length, 1);
    String[] splits = Request2.split("4, 5, (2,3), 7");
    int i = 0;
    Assert.assertEquals("4", splits[i++]);
    Assert.assertEquals("5", splits[i++]);
    Assert.assertEquals("2,3", splits[i++]);
    Assert.assertEquals("7", splits[i++]);
  }
}
