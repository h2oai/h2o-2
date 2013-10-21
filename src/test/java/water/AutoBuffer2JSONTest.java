package water;

import org.junit.*;

import water.api.DocGen;

public class AutoBuffer2JSONTest extends TestUtil {

  @BeforeClass public static void stall() { TestUtil.stall_till_cloudsize(1); }

  public static abstract class A extends Request2 {
    @Override protected Response serve() { return null; }
  }

  static class A1 extends A {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    @API(help="double field with NaN")       double d1 = Double.NaN;
    @API(help="double field with +Infinity") double d2 = Double.POSITIVE_INFINITY;
    @API(help="double field with -Infinity") double d3 = Double.NEGATIVE_INFINITY;
  }

  @Test public void testDouble() {
    assertEqual(new A1(), "{\"Request2\":0,\"d1\":\\\"NaN\\\",\"d2\":\\\"Infinity\\\",\"d3\":\\\"-Infinity\\\"}");
  }

  static class A2 extends A {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    @API(help="float field with NaN")       float f1 = Float.NaN;
    @API(help="float field with +Infinity") float f2 = Float.POSITIVE_INFINITY;
    @API(help="float field with -Infinity") float f3 = Float.NEGATIVE_INFINITY;
  }

  @Test public void testFloat() {
    assertEqual(new A2(), "{\"Request2\":0,\"f1\":\\\"NaN\\\",\"f2\":\\\"Infinity\\\",\"f3\":\\\"-Infinity\\\"}");
  }

  private void assertEqual(A test, String expJson) {
    AutoBuffer ab = new AutoBuffer();
    String json = new String(test.writeJSON(ab).buf());
    Assert.assertEquals(expJson, json);
  }
}
