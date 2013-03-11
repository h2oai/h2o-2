package water.exec;

import static org.junit.Assert.*;
import org.junit.Test;
import water.*;

public class ExprTest extends TestUtil {
  int i = 0;

  @Test public void testMakeEnum() {
    Key k1=null,k2=null,kg=null,k3=null,ki=null;
    try {
      k1 = loadAndParseKey("h.hex","smalldata/cars.csv");
      ValueArray.value(k1);
      k2 = executeExpression("g=colSwap(h.hex,2,factor(h.hex[2]))");
      kg = Key.make("g");
      ValueArray va2 = ValueArray.value(kg);
      ValueArray.Column col = va2._cols[2];
      assertArrayEquals(col._domain,new String[]{"3","4","5","6","8"});
      k3 = executeExpression("i=colSwap(h.hex,2,h.hex[2]==3?1:0)");
      ki = Key.make("i");
    } finally {
      if( k1 != null ) UKV.remove(k1);
      if( k2 != null ) UKV.remove(k2);
      if( kg != null ) UKV.remove(kg);
      if( k3 != null ) UKV.remove(k3);
      if( ki != null ) UKV.remove(ki);
    }
  }

  // Test a big slice
  @Test public void testMultiChunkFile() {
    Key k1 = loadAndParseKey("hhp.hex","smalldata/hhp.cut3.214.data.gz");
    ValueArray.value(k1);
    Key k2 = executeExpression("g=slice(hhp.hex,1,131248)");
    Key kg = Key.make("g");
    ValueArray va2 = ValueArray.value(kg);
    assertEquals(va2.data(0, 72), 1);
    assertEquals(va2.data(0, 77), 1);
    assertEquals(va2.data(2,105),10);
    assertEquals(va2.data(va2.numRows()-1, 13), 8);
    assertEquals(va2.data(va2.numRows()-1,179),28);
    assertEquals(va2.data(va2.numRows()-1,184),13);
    UKV.remove(kg);
    UKV.remove(k1);
    UKV.remove(k2);
  }


  // Test a big random filter
  @Test public void testRandomFilter() {
    Key k1=null, k2=null, kg=null;
    try {
      k1 = loadAndParseKey("kaggle.hex","smalldata/kaggle/creditsample-training.csv.gz");
      ValueArray.value(k1);
      k2 = executeExpression("g=randomFilter(kaggle.hex,5432,1232123)");
      kg = Key.make("g");
      ValueArray va2 = ValueArray.value(kg);
      assertEquals(va2.data(0, 0),  5);
      assertEquals(va2.data(1, 0), 29);
      assertEquals(va2.data(2, 0), 46);
      assertEquals(va2.data(va2.numRows()-3, 0), 149943);
      assertEquals(va2.data(va2.numRows()-2, 0), 149948);
      assertEquals(va2.data(va2.numRows()-1, 0), 149951);
    } finally {
      UKV.remove(k1);
      if( k2 != null ) UKV.remove(k2);
      if( kg != null ) UKV.remove(kg);
    }
  }


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

  @Test public void testParserFails() {
    testParseFail("4.5.6");
    testParseFail("4e4e4");
    testParseFail("a +* b");
    testParseFail("(a + b");
    testParseFail(" |hello");
    testParseFail(" a $ 5");
  }

  @Test public void testExecFails() {
    testExecFail("a");
    testExecFail("a$hello");
    testExecFail("a[2]");
    testScalarExpression("a=5",5);
    testExecFail("a$hello");
    testExecFail("a[2]");
    UKV.remove(Key.make("a"));
  }

  @Test public void testDivByZero() {
    testScalarExpression("5/0", Double.POSITIVE_INFINITY);
    testScalarExpression("n = 6",6);
    testScalarExpression("g = 0",0);
    testScalarExpression("n/g",Double.POSITIVE_INFINITY);
    testScalarExpression("n/0",Double.POSITIVE_INFINITY);
    UKV.remove(Key.make("n"));
    UKV.remove(Key.make("g"));
  }

  protected Key executeExpression(String expr) {
    DKV.write_barrier();
    try {
      ++i;
      Key key = Exec.exec(expr, "result"+(new Integer(i).toString()));
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

  @Test public void testNumberParsing() {
    testScalarExpression("5",5);
    testScalarExpression("5.0",5.0);
    testScalarExpression("5e4",5e4);
    testScalarExpression("5.2e3",5.2e3);
  }


  @Test public void testScalarExpressions() {
    testScalarExpression("5", 5);
    testScalarExpression("-5", -5);
    testScalarExpression("5+6", 11);
    testScalarExpression("5    + 7", 12);
    testScalarExpression("5+-5", 0);
  }

  @Test public void testOperators() {
    testScalarExpression("1+2",3);
    testScalarExpression("1-2",-1);
    testScalarExpression("1*2",2);
    testScalarExpression("1/2",0.5);
    testScalarExpression("2-1",1);
    testScalarExpression("2/1",2);
  }

  @Test public void testOperatorPrecedence() {
    testScalarExpression("1+2*3",7);
    testScalarExpression("1*2+3",5);
    testScalarExpression("1+2*3+4",11);
    testScalarExpression("1+2*3+3*3",16);
    testScalarExpression("1-2/4",0.5);
    testScalarExpression("1+2-3",0);
    testScalarExpression("1*2/4",0.5);
  }

  @Test public void testParentheses() {
    testScalarExpression("(1+2)*3",9);
    testScalarExpression("(1+2)*(3+3)*3",54);
  }

  @Test public void testAssignments() {
    testScalarExpression("a1 = 5",5);
    testScalarExpression("b1 = 6",6);
    testScalarExpression("a1",5);
    testScalarExpression("b1",6);
    testScalarExpression("a2 <- 1",1);
    testScalarExpression("a2",1);
    testScalarExpression("1 -> b2",1);
    testScalarExpression("b2",1);
    UKV.remove(Key.make("a1"));
    UKV.remove(Key.make("b1"));
    UKV.remove(Key.make("a2"));
    UKV.remove(Key.make("b2"));
  }

  @Test public void testIdentOperators() {
    testScalarExpression("a3 = 8", 8);
    testScalarExpression("b3 = 2", 2);
    testScalarExpression("a3+b3",10);
    testScalarExpression("a3-b3",6);
    testScalarExpression("a3*b3",16);
    testScalarExpression("a3/b3",4);
    testScalarExpression("a3+4",12); // from right
    testScalarExpression("a3-4",4);
    testScalarExpression("a3*4",32);
    testScalarExpression("a3/4",2);
    testScalarExpression("4+a3",12); // from left
    testScalarExpression("4-a3",-4);
    testScalarExpression("4*a3",32);
    testScalarExpression("32/a3",4);
    testScalarExpression("-a3+2",-6);
    UKV.remove(Key.make("a3"));
    UKV.remove(Key.make("b3"));
  }

  @Test public void testQuotedIdents() {
    testScalarExpression("|a\"b/c\\\\d| = 5", 5);
    testScalarExpression("|a\"b/c\\\\d|", 5);
    UKV.remove(Key.make("a\"b/c\\d"));
  }

  @Test public void testComplexAssignments() {
    testScalarExpression("a4 = 5",5);
    testScalarExpression("b4 = 6",6);
    testScalarExpression("c4 = a4 + b4",11);
    testScalarExpression("c4",11);
    testScalarExpression("c4 + a4 -> c5",16);
    testScalarExpression("c5",16);
    UKV.remove(Key.make("a4"));
    UKV.remove(Key.make("b4"));
    UKV.remove(Key.make("c4"));
    UKV.remove(Key.make("c5"));
  }

}
