package water;

import org.junit.*;

import dontweave.gson.JsonObject;

import water.api.DocGen;
import water.api.Request.API;
import water.util.JsonUtil;

public class AutoBuffer2JSONTest extends TestUtil {

  @BeforeClass public static void stall() { TestUtil.stall_till_cloudsize(1); }

  static class A1 extends Iced {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    @API(help="double field with NaN")       double d1 = Double.NaN;
    @API(help="double field with +Infinity") double d2 = Double.POSITIVE_INFINITY;
    @API(help="double field with -Infinity") double d3 = Double.NEGATIVE_INFINITY;
    @API(help="double field with a number")  double d4 = -3.141527;
  }

  @Test public void testDouble() {
    assertEqual(new A1(), "{\"d1\":\"NaN\",\"d2\":\"Infinity\",\"d3\":\"-Infinity\",\"d4\":-3.141527}");
  }

  static class A2 extends Iced {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;
    @API(help="float field with NaN")       float f1 = Float.NaN;
    @API(help="float field with +Infinity") float f2 = Float.POSITIVE_INFINITY;
    @API(help="float field with -Infinity") float f3 = Float.NEGATIVE_INFINITY;
    @API(help="float field with a number")  float f4 = -3.141527f;
  }

  @Test public void testFloat() {
    assertEqual(new A2(), "{\"f1\":\"NaN\",\"f2\":\"Infinity\",\"f3\":\"-Infinity\",\"f4\":-3.141527}");
  }

  // ---- Only Request1 tests for correct JSON
  @Test public void testDoubleFromRequest() {
    JsonObject o = new JsonObject();
    o.addProperty("d1", Double.NaN);
    o.addProperty("d2", Double.POSITIVE_INFINITY);
    o.addProperty("d3", Double.NEGATIVE_INFINITY);
    o.addProperty("d4", 3.141527);
    o = JsonUtil.escape(o);
    Assert.assertEquals("{\"d1\":\"NaN\",\"d2\":\"Infinity\",\"d3\":\"-Infinity\",\"d4\":3.141527}", o.toString());
  }
  //---- Only Request1 tests
  @Test public void testFloatFromRequest() {
    JsonObject o = new JsonObject();
    o.addProperty("f1", Float.NaN);
    o.addProperty("f2", Float.POSITIVE_INFINITY);
    o.addProperty("f3", Float.NEGATIVE_INFINITY);
    o.addProperty("f4", 3.141527f);
    o = JsonUtil.escape(o);
    Assert.assertEquals("{\"f1\":\"NaN\",\"f2\":\"Infinity\",\"f3\":\"-Infinity\",\"f4\":3.141527}", o.toString());
   }

  private void assertEqual(Iced test, String expJson) {
    AutoBuffer ab = new AutoBuffer();
    String json = new String(test.writeJSON(ab).buf());
    Assert.assertEquals(expJson, json);
  }
}
