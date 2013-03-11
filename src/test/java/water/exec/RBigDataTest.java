package water.exec;

import static org.junit.Assert.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.parser.ParseDataset;

public class RBigDataTest extends TestUtil {
  static private final AtomicInteger UNIQUE = new AtomicInteger(1);

  protected void testParseFail(String expr, int errorPos) {
    try {
      RLikeParser parser = new RLikeParser();
      parser.parse(expr);
      assertTrue("An exception should have been thrown.",false);
    } catch (ParserException e) {
      if (errorPos != -1)
        assertEquals(errorPos,e._pos);
    }
  }

  protected void testParseFail(String expr) {
    testParseFail(expr,-1);
  }

  protected void testExecFail(String expr, int errorPos) {
    DKV.write_barrier();
    int keys = H2O.store_size();
    try {
      int i = UNIQUE.getAndIncrement();
      System.err.println("result"+(new Integer(i).toString())+": "+expr);
      Key key = Exec.exec(expr, "result"+(new Integer(i).toString()));
      UKV.remove(key);
      assertTrue("An exception should have been thrown.",false);
    } catch (ParserException e) {
      assertTrue(false);
    } catch (EvaluationException e) {
      if (errorPos!=-1)
        assertEquals(errorPos,e._pos);
    }
    DKV.write_barrier();
    assertEquals("Keys were not properly deleted for expression "+expr,keys,H2O.store_size());
  }

  protected void testExecFail(String expr) {
    testExecFail(expr,-1);
  }

  protected Key executeExpression(String expr) {
    DKV.write_barrier();
    try {
      int i = UNIQUE.getAndIncrement();
      Key key = Exec.exec(expr, "RBigResult"+i);
      return key;
    } catch (PositionedException e) {
      System.out.println(e.report(expr));
      e.printStackTrace();
      assertTrue(false);
      return null;
    }
  }

  protected void testScalarExpression(String expr, double result) {
    Key key = executeExpression(expr);
    ValueArray va = ValueArray.value(key);
    assertEquals(va.numRows(), 1);
    assertEquals(va.numCols(), 1);
    assertEquals(result,va.datad(0,0), 0.0);
    UKV.remove(key);
  }

  protected void testKeyValues(Key k, double n1, double n2, double n3, double nx3, double nx2, double nx1) {
    ValueArray v = ValueArray.value(k);
    assertEquals(v.datad(0,0),n1,0.0);
    assertEquals(v.datad(1,0),n2,0.0);
    assertEquals(v.datad(2,0),n3,0.0);
    assertEquals(v.datad(v.numRows()-3,0),nx3,0.0);
    assertEquals(v.datad(v.numRows()-2,0),nx2,0.0);
    assertEquals(v.datad(v.numRows()-1,0),nx1,0.0);
  }

  public void testVectorExpression(String expr, double n1, double n2, double n3, double nx3, double nx2, double nx1) {
    Key key = executeExpression(expr);
    testKeyValues(key,n1,n2,n3,nx3,nx2,nx1);
    UKV.remove(key);
  }

  public void testDataFrameStructure(Key k, int rows, int cols) {
    ValueArray v = ValueArray.value(k);
    assertEquals(v.numRows(), rows);
    assertEquals(v.numCols(), cols);
  }

  @Test public void testFullVectAssignment() {
    Key k = loadAndParseKey("cars.hex", "smalldata/cars.csv");
    Key k2 = executeExpression("cars.hex");
    testDataFrameStructure(k2, 406, 8);
    UKV.remove(k2);
    k2 = executeExpression("a5 = cars.hex[2]");
    testVectorExpression("a5",8,8,8,4,6,6);
    UKV.remove(k2);
    UKV.remove(k);
    UKV.remove(Key.make("a5"));
  }

  @Test public void testSingleVectorAssignment() {
    Key k = loadAndParseKey("cars.hex", "smalldata/cars.csv");
    UKV.remove(k);
  }

  @Test public void testVectorOperators() {
    Key k = loadAndParseKey("cars.hex", "smalldata/cars.csv");
    testVectorExpression("cars.hex[2] + cars.hex$year", 81, 78, 80, 80, 84, 87);
    testVectorExpression("cars.hex[2] - cars.hex$year", -65, -62, -64, -72, -72, -75);
    testVectorExpression("cars.hex[2] * cars.hex$year", 584, 560, 576, 304, 468, 486);
    testVectorExpression("cars.hex$year / cars.hex[2]", 9.125, 8.75, 9.0, 19.0, 13.0, 13.5);
    UKV.remove(k);
  }

  @Test public void testColumnSelectors() {
    Key k = loadAndParseKey("cars.hex", "smalldata/cars.csv");
    Key k2 = executeExpression("cars.hex[2]");
    testDataFrameStructure(k2, 406, 1);
    testKeyValues(k2, 8, 8, 8, 4, 6, 6);
    UKV.remove(k2);
    k2 = executeExpression("cars.hex$year");
    testDataFrameStructure(k2, 406, 1);
    testKeyValues(k2, 73, 70, 72, 76, 78, 81);
    UKV.remove(k2);
    UKV.remove(k);
  }

  @Test public void testLargeDataOps() {
    Key poker = loadAndParseKey("p.hex", "smalldata/poker/poker-hand-testing.data");
    testVectorExpression("p.hex[1] + p.hex[2]", 2, 15, 13, 15, 12, 7);
    testVectorExpression("p.hex[1] - p.hex[2]", 0, 9, 5, 7, 10, 3);
    testVectorExpression("p.hex[1] * p.hex[2]", 1, 36, 36, 44, 11, 10);
    testVectorExpression("p.hex[1] / p.hex[2]", 1.0, 4.0, 2.25, 2.75, 11.0, 2.5);
    UKV.remove(poker);
  }

  @Test public void testBigLargeExpression() {
    Key poker = loadAndParseKey("p.hex", "smalldata/poker/poker-hand-testing.data");
    testVectorExpression("p.hex[1] / p.hex[2] + p.hex[3] * p.hex[1] - p.hex[5] + (2* p.hex[1] - (p.hex[2]+3))", 8, 35, 63.25, 85.75, 116.0, 43.5);
    UKV.remove(poker);
  }

  @Test public void testDifferentSizeOps() {
    Key cars = loadAndParseKey("cars.hex", "smalldata/cars.csv");
    Key poker = loadAndParseKey("p.hex", "smalldata/poker/poker-hand-testing.data");
    testVectorExpression("cars.hex$year + p.hex[1]", 74, 82, 81, 84, 86, 81);
    testVectorExpression("cars.hex$year - p.hex[1]", 72, 58, 63, 62, 64, 71);
    testVectorExpression("cars.hex$year * p.hex[1]", 73, 840, 648, 803, 825, 380);
    //testVectorExpression("cars.hex$year / p.hex[1]", 73, 70/12, 8, 76/11, 78/11, 15.2); // hard to get the numbers right + not needed no new coverage
    testVectorExpression("p.hex[1] + cars.hex$year", 74, 82, 81, 84, 86, 81);
    testVectorExpression("p.hex[1] - cars.hex$year", -72, -58, -63, -62, -64, -71);
    testVectorExpression("p.hex[1] * cars.hex$year", 73, 840, 648, 803, 825, 380);
    //testVectorExpression("p.hex[1] / cars.hex$year", 1/73, 12/70, 0.125, 11/76, 11/78, 5/81);
    UKV.remove(poker);
    UKV.remove(cars);
  }

  // ---
  // Test some basic expressions on "cars.csv"
  @Test public void testBasicCrud() {
    Key k = loadAndParseKey("cars.hex", "smalldata/cars.csv");
    testVectorExpression("cars.hex[1] + cars.hex$cylinders", 21,23,25,24,23,36.7);
    UKV.remove(k);
  }
}
