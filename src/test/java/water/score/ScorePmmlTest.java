package water.score;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;
import water.H2O;
import water.TestUtil;
import water.parser.PMMLParser;

public class ScorePmmlTest extends TestUtil {
  private static final String HEADER = "<?xml version='1.0' encoding='UTF-8'?>\n" +
  		"<PMML version='4.1' xmlns='http://www.dmg.org/PMML-4_1' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>\n" +
  		"  <Header copyright='0xData Testing' description='0xData testing Model'>\n" +
  		"    <Application name='H2O' version='0.1'/>\n" +
  		"    <Timestamp>2013-01-11T15:52:10.8</Timestamp>\n" +
  		"  </Header>\n" +
  		"";
  private static final String FOOTER = "</PMML>";

  enum DataType {
    DOUBLE("continuous"),
    INT("continuous"),
    BOOLEAN("categorical"),
    STRING("categorical"),
    ;

    private final String opType;
    private DataType(String op) {
      opType = op;
    }

    public String getOpType() { return opType; }
    public String getDataType() { return toString().toLowerCase(); }
  }

  enum UsageType {
    PREDICTED,
    ACTIVE,
    ;
    public String getUsageType() { return toString().toLowerCase(); }
  }

  enum SimpleOp {
    EQ("equal", true),
    LE("lessOrEqual", true),
    LT("lessThan", true),
    GE("greaterOrEqual", true),
    GT("greaterThan", true),
    MISSING("isMissing", false),
    ;

    private final String op;
    private final boolean hasArg;
    private SimpleOp(String op, boolean hasArg) {
      this.op = op;
      this.hasArg = hasArg;
    }
    public String getOp() { return op; }
    public boolean hasArg() { return hasArg; }
  }

  enum SimpleSetOp {
    IN("isIn"),
    NOT_IN("isNotIn"),
    ;

    private final String op;
    private SimpleSetOp(String op) { this.op = op; }
    public String getOp() { return op; }
  }

  enum CompoundOp {
    AND("and"),
    OR("or"),
    ;
    private final String op;
    private CompoundOp(String op) {
      this.op = op;
    }
    public String getOp() { return op; }
  }

  public static String makePmml(String dataDict, String scoreCard) {
    return HEADER + dataDict + scoreCard + FOOTER;
  }

  public static String makeDataDictionary(String... fields) {
    StringBuilder sb = new StringBuilder();
    sb.append("<DataDictionary>\n");
    for(String f : fields) sb.append("  ").append(f);
    sb.append("</DataDictionary>\n");
    return sb.toString();
  }
  public static String makeDataField(String name, DataType dt) {
    return String.format("<DataField name='%s' dataType='%s' optype='%s'/>\n",
        name, dt.getDataType(), dt.getOpType());
  }

  public static String makeScorecard(String miningSchema, String outputSchema, String... characteristics) {
    StringBuilder sb = new StringBuilder();
    sb.append("<Scorecard modelName='TEST_MODEL' functionName='regression' \n");
    sb.append("   useReasonCodes='false' initialScore='0' baselineMethod='other'>\n");
    sb.append(miningSchema);
    sb.append(outputSchema);
    sb.append("  <Characteristics>\n");
    for(String f : characteristics) sb.append(f);
    sb.append("  </Characteristics>\n");
    sb.append("</Scorecard>\n");
    return sb.toString();
  }

  public static String makeMiningSchema(String... fields) {
    StringBuilder sb = new StringBuilder();
    sb.append("<MiningSchema>\n");
    for(String f : fields) sb.append("  ").append(f);
    sb.append("</MiningSchema>\n");
    return sb.toString();
  }
  public static String makeMiningField(String name, String usageType) {
    return String.format("<MiningField name='%s' usageType='%s'/>\n",
        name, usageType);
  }

  public static String makeOutputSchema(String name, DataType dt) {
    return String.format("<Output>\n" +
        "  <OutputField name='%s' feature='predictedValue' dataType='%s' optype='%s'/>\n" +
        "</Output>", name, dt.getDataType(), dt.getOpType());
  }

  public static String makeCharacteristic(String name, String... attributes) {
    StringBuilder sb = new StringBuilder();
    sb.append("<Characteristic name='").append(name).append("'>\n");
    for(String f : attributes) sb.append("  ").append(f);
    sb.append("</Characteristic>\n");
    return sb.toString();
  }

  public static String makeAttribute(double score, String predicate) {
    return String.format("<Attribute partialScore='%f'>\n  %s</Attribute>\n",
        score, predicate);
  }

  public static String makeSimplePredicate(String field, SimpleOp operator, Object value) {
    if( operator.hasArg() )
      return String.format("<SimplePredicate field='%s' operator='%s' value='%s'/>\n",
                           field, operator.getOp(), value.toString());
    else
      return String.format("<SimplePredicate field='%s' operator='%s'/>\n",
          field, operator.getOp());
  }

  public static String makeCompoundPredicate(CompoundOp op, String p1, String p2) {
    StringBuilder sb = new StringBuilder();
    sb.append("<CompoundPredicate booleanOperator='").append(op.getOp()).append("'>\n");
    sb.append("  ").append(p1);
    sb.append("  ").append(p2);
    sb.append("</CompoundPredicate>\n");
    return sb.toString();
  }

  public static String makeSetPredicate(String field, SimpleSetOp op, String... values) {
    StringBuilder sb = new StringBuilder();
    sb.append("<SimpleSetPredicate field='").append(field).append("'");
    sb.append(" booleanOperator='").append(op.getOp()).append("'>\n");
    sb.append("  <Array n='").append(values.length).append("' type='string'>\n");
    for( String v : values ) sb.append("    ").append(v).append("\n");
    sb.append("  </Array>\n");
    sb.append("</SimpleSetPredicate>\n");
    return sb.toString();
  }

  static final String simplePmml = makePmml(
      makeDataDictionary(
          makeDataField("x", DataType.DOUBLE),
          makeDataField("y", DataType.DOUBLE),
          makeDataField("res", DataType.DOUBLE)),
      makeScorecard(
          makeMiningSchema(
              makeMiningField("res", "predicted"),
              makeMiningField("x", "active"),
              makeMiningField("y", "active")),
          makeOutputSchema("res", DataType.DOUBLE),
          makeCharacteristic("x_check",
              makeAttribute(1.0, makeSimplePredicate("x", SimpleOp.LT, 0.0)),
              makeAttribute(2.0, makeSimplePredicate("x", SimpleOp.GE, 0.0))),
          makeCharacteristic("y_check",
              makeAttribute(1.0, makeSimplePredicate("y", SimpleOp.LE, 0.0)),
              makeAttribute(2.0, makeSimplePredicate("y", SimpleOp.GT, 0.0)))
              ));

  @Test
  public void testBasic() throws Exception {

    String pmml = simplePmml;

    double[][] tests = new double[][] {
        { -1.0, -1.0, 2.0 },
        { -1.0,  1.0, 3.0 },
        {  1.0, -1.0, 3.0 },
        {  1.0,  1.0, 4.0 },
        {  0.0,  0.0, 3.0 },
    };

    for( double[] t : tests) {
      HashMap<String, Comparable> m = new HashMap<String, Comparable>();
      m.put("x", t[0]);
      m.put("y", t[1]);

      ScorecardModel scm     = getSCM(pmml);
      double predictedScore  = score2(scm,m);

      Assert.assertEquals(t[2], predictedScore, 0.00001);
    }
  }

  private static String makeBasic(DataType dt, SimpleOp op, Object val) {
    return makePmml(
        makeDataDictionary(
            makeDataField("x", dt),
            makeDataField("res", DataType.DOUBLE)),
        makeScorecard(
            makeMiningSchema(
                makeMiningField("res", "predicted"),
                makeMiningField("x", "active")),
            makeOutputSchema("res", DataType.DOUBLE),
            makeCharacteristic("x_check",
                makeAttribute(1.0, makeSimplePredicate("x", op, val)))));
  }

  @Test
  public void testComparators() throws Exception {
    Object[][] tests = new Object[][] {
        { DataType.DOUBLE, SimpleOp.LT, 0.0, -1.0, 1.0 },
        { DataType.DOUBLE, SimpleOp.LT, 0.0,  0.0, 0.0 },
        { DataType.DOUBLE, SimpleOp.LT, 0.0,  1.0, 0.0 },
        { DataType.INT,    SimpleOp.LT,   0L, -1L, 1.0 },
        { DataType.INT,    SimpleOp.LT,   0L,  0L, 0.0 },
        { DataType.INT,    SimpleOp.LT,   0L,  1L, 0.0 },

        { DataType.DOUBLE, SimpleOp.GT, 0.0, -1.0, 0.0 },
        { DataType.DOUBLE, SimpleOp.GT, 0.0,  0.0, 0.0 },
        { DataType.DOUBLE, SimpleOp.GT, 0.0,  1.0, 1.0 },
        { DataType.INT,    SimpleOp.GT,   0L, -1L, 0.0 },
        { DataType.INT,    SimpleOp.GT,   0L,  0L, 0.0 },
        { DataType.INT,    SimpleOp.GT,   0L,  1L, 1.0 },

        { DataType.DOUBLE, SimpleOp.LE, 0.0, -1.0, 1.0 },
        { DataType.DOUBLE, SimpleOp.LE, 0.0,  0.0, 1.0 },
        { DataType.DOUBLE, SimpleOp.LE, 0.0,  1.0, 0.0 },
        { DataType.INT,    SimpleOp.LE,   0L, -1L, 1.0 },
        { DataType.INT,    SimpleOp.LE,   0L,  0L, 1.0 },
        { DataType.INT,    SimpleOp.LE,   0L,  1L, 0.0 },

        { DataType.DOUBLE, SimpleOp.GE, 0.0, -1.0, 0.0 },
        { DataType.DOUBLE, SimpleOp.GE, 0.0,  0.0, 1.0 },
        { DataType.DOUBLE, SimpleOp.GE, 0.0,  1.0, 1.0 },
        { DataType.INT,    SimpleOp.GE,   0L, -1L, 0.0 },
        { DataType.INT,    SimpleOp.GE,   0L,  0L, 1.0 },
        { DataType.INT,    SimpleOp.GE,   0L,  1L, 1.0 },

        { DataType.DOUBLE, SimpleOp.EQ, 0.0, -1.0, 0.0 },
        { DataType.DOUBLE, SimpleOp.EQ, 0.0,  0.0, 1.0 },
        { DataType.DOUBLE, SimpleOp.EQ, 0.0,  1.0, 0.0 },
        { DataType.INT,    SimpleOp.EQ,   0L, -1L, 0.0 },
        { DataType.INT,    SimpleOp.EQ,   0L,  0L, 1.0 },
        { DataType.INT,    SimpleOp.EQ,   0L,  1L, 0.0 },
        { DataType.STRING, SimpleOp.EQ, "a",  "a", 1.0 },
        { DataType.STRING, SimpleOp.EQ, "a",  "b", 0.0 },
        { DataType.STRING, SimpleOp.EQ, "a", null, 0.0 },
        { DataType.BOOLEAN,SimpleOp.EQ, true, true, 1.0 },
        { DataType.BOOLEAN,SimpleOp.EQ, true, false, 0.0 },
    };

    for( Object[] t : tests) {
      String pmml = makeBasic((DataType)t[0], (SimpleOp) t[1], t[2]);
      HashMap<String, Comparable> m = new HashMap<String, Comparable>();
      m.put("x", (Comparable)t[3]);

      ScorecardModel scm     = getSCM(pmml);
      double predictedScore  = score2(scm,m);

      Assert.assertEquals((Double)t[4], predictedScore, 0.00001);
    }
  }

  @Test
  public void testMissing() throws Exception {
    String pmml = makePmml(
        makeDataDictionary(
            makeDataField("x", DataType.DOUBLE),
            makeDataField("y", DataType.DOUBLE),
            makeDataField("res", DataType.DOUBLE)),
        makeScorecard(
            makeMiningSchema(
                makeMiningField("res", "predicted"),
                makeMiningField("x", "active"),
                makeMiningField("y", "active")),
            makeOutputSchema("res", DataType.DOUBLE),
            makeCharacteristic("x_check",
                makeAttribute(1.0, makeSimplePredicate("x", SimpleOp.MISSING, 0.0))),
            makeCharacteristic("y_check",
                makeAttribute(2.0, makeSimplePredicate("y", SimpleOp.MISSING, 0.0)))
                ));

    Double[][] tests = new Double[][] {
        {  1.0, null, 2.0 },
        { -1.0,  1.0, 0.0 },
        { null, -1.0, 1.0 },
        { null, null, 3.0 },
    };

    for( Double[] t : tests) {
      HashMap<String, Comparable> m = new HashMap<String, Comparable>();
      m.put("x", t[0]);
      m.put("y", t[1]);

      ScorecardModel scm     = getSCM(pmml);
      double predictedScore  = score2(scm,m);
      Assert.assertEquals(t[2], predictedScore, 0.00001);
    }
  }

  @Test
  public void testCompound() throws Exception {
    String pmml = makePmml(
        makeDataDictionary(
            makeDataField("x", DataType.DOUBLE),
            makeDataField("res", DataType.DOUBLE)),
        makeScorecard(
            makeMiningSchema(
                makeMiningField("res", "predicted"),
                makeMiningField("x", "active")),
            makeOutputSchema("res", DataType.DOUBLE),
            makeCharacteristic("x_check",
                makeAttribute(1.0,
                    makeCompoundPredicate(CompoundOp.AND,
                        makeSimplePredicate("x", SimpleOp.GT, 0.0),
                        makeSimplePredicate("x", SimpleOp.LT, 1.0)
                    )))
                ));

    double[][] tests = new double[][] {
        {  1.0, 0.0 },
        {  0.0, 0.0 },
        {  0.3, 1.0 },
        { -0.3, 0.0 },
        {  1.3, 0.0 },
    };

    for( double[] t : tests) {
      HashMap<String, Comparable> m = new HashMap<String, Comparable>();
      m.put("x", t[0]);

      ScorecardModel scm     = getSCM(pmml);
      double predictedScore  = score2(scm,m);
      Assert.assertEquals(t[1], predictedScore, 0.00001);
    }
  }


  @Test
  public void testSet() throws Exception {
    String pmml = makePmml(
        makeDataDictionary(
            makeDataField("x", DataType.STRING),
            makeDataField("y", DataType.STRING),
            makeDataField("res", DataType.DOUBLE)),
        makeScorecard(
            makeMiningSchema(
                makeMiningField("res", "predicted"),
                makeMiningField("x", "active")),
            makeOutputSchema("res", DataType.DOUBLE),
            makeCharacteristic("x_check",
                makeAttribute(1.0,
                    makeSetPredicate("x", SimpleSetOp.IN, "asdf", "qwer"))),
            makeCharacteristic("y_check",
                makeAttribute(2.0,
                    makeSetPredicate("y", SimpleSetOp.NOT_IN, "qwer", "monkey", "ninja")))
                ));

    Object[][] tests = new Object[][] {
        {  "asdf",   "asdf", 3.0 },
        {  "qwer",   "qwer", 1.0 },
        {  "monkey", "monkey", 0.0 },
        {  "cowboy", "cowboy", 2.0 },
        {  "",       "",       2.0 },
        {  null,     null,     2.0 },
        {  "ASDF",   "ASDF",   2.0 },
    };

    for( Object[] t : tests) {
      HashMap<String, Comparable> m = new HashMap<String, Comparable>();
      m.put("x", (Comparable) t[0]);
      m.put("y", (Comparable) t[1]);

      ScorecardModel scm     = getSCM(pmml);
      double predictedScore  = score2(scm,m);
      Assert.assertEquals((Double)t[2], predictedScore, 0.00001);
    }
  }

  @Test
  public void testMissingParams1() throws Exception {
    String pmml = makePmml(
        makeDataDictionary(
            makeDataField("x", DataType.DOUBLE),
            makeDataField("y", DataType.DOUBLE),
            makeDataField("res", DataType.DOUBLE)),
        makeScorecard(
            makeMiningSchema(
                makeMiningField("res", "predicted"),
                makeMiningField("x", "active"),
                makeMiningField("y", "active")),
            makeOutputSchema("res", DataType.DOUBLE),
            makeCharacteristic("x_check",
                makeAttribute(1.0, makeSimplePredicate("x", SimpleOp.GE, 0.0))),
            makeCharacteristic("y_check",
                makeAttribute(2.0, makeSimplePredicate("y", SimpleOp.GE, 0.0)))
                ));
    ScorecardModel scm     = getSCM(pmml);

    HashMap<String, Comparable> m = new HashMap<String, Comparable>();
    Assert.assertEquals(0.0, score2(scm,m), 0.00001);

    m.put("y", 1.0);
    Assert.assertEquals(2.0, score2(scm,m), 0.00001);
    m.put("x", 2.0);
    Assert.assertEquals(3.0, score2(scm,m), 0.00001);
    m.remove("y");
    Assert.assertEquals(1.0, score2(scm,m), 0.00001);
  }

  @Test
  public void testMissingParams2() throws Exception {
    String pmml = makePmml(
        makeDataDictionary(
            makeDataField("x", DataType.STRING),
            makeDataField("res", DataType.DOUBLE)),
        makeScorecard(
            makeMiningSchema(
                makeMiningField("res", "predicted"),
                makeMiningField("x", "active")),
            makeOutputSchema("res", DataType.DOUBLE),
            makeCharacteristic("x_check",
                makeAttribute(  0.0, makeSimplePredicate("x", SimpleOp.EQ, "XY")),
                makeAttribute(  1.0, makeSimplePredicate("x", SimpleOp.EQ, "X")),
                makeAttribute(  2.0, makeSimplePredicate("x", SimpleOp.EQ, "Y")),
                makeAttribute(100.0, makeCompoundPredicate(
                                         CompoundOp.OR,
                                         makeSimplePredicate("x", SimpleOp.MISSING, 0.0),
                                         makeSimplePredicate("x", SimpleOp.EQ, ""))),
                makeAttribute(100.0, makeCompoundPredicate(
                     CompoundOp.AND,
                     makeSimplePredicate("x", SimpleOp.MISSING, 0.0),
                     makeSimplePredicate("x", SimpleOp.EQ, ""))
                     )
                )));
    Object[][] tests = new Object[][] {
        {  "XY",   0.0 },
        {  "X",    1.0 },
        {  "Y",    2.0 },
        {  "",   100.0 },
        {  null, 100.0 },
        {  "BLUDICKA", 0.0 },
    };

    ScorecardModel scm = getSCM(pmml);
    for( Object[] t : tests) {
      HashMap<String, Comparable> m = new HashMap<String, Comparable>();
      m.put("x", (Comparable) t[0]);
      m.put("dummy", (Comparable) t[0]);

      double predictedScore  = score2(scm,m);
      Assert.assertEquals((Double)t[1], predictedScore, 0.00001);
    }
  }

  @Test
  public void testWrongTypes() throws Exception {

    String pmml = simplePmml;

    Object[][] tests = new Object[][] {
        { -1.0,   "Y",  1.0 },
        {  "X",   1.0,  2.0 },
        {  1.0,   true, 2.0 },
        {  false, 1.0,  2.0 },
        {  1890L, 0.0,  3.0 },
        {  0.0,   12L,  4.0 },
        {  2222,  0.0,  3.0 },
        {  0.0,   99,   4.0 },
    };

    for( Object[] t : tests) {
      HashMap<String, Comparable> m = new HashMap<String, Comparable>();
      m.put("x", (Comparable) t[0]);
      m.put("y", (Comparable) t[1]);

      ScorecardModel scm     = getSCM(pmml);
      double predictedScore0 = score2(scm,m); // "interpreter" version
      Assert.assertEquals((Double)t[2], predictedScore0, 0.00001);
      double predictedScore1 = score2(scm,m); // "compiled" w/hashmap version
      Assert.assertEquals((Double)t[2], predictedScore1, 0.00001);
    }
  }

  private static double score2( ScorecardModel scm, HashMap<String, Comparable> m) {
    double s0 = scm.score_interpreter(m);
    double s1 = scm.score(m);
    Assert.assertEquals(s0,s1, 0.0000001);
    return s0;
  }


  private ScorecardModel getSCM(final String pmml) throws Exception {
    return (ScorecardModel)PMMLParser.parse(new ByteArrayInputStream(pmml.getBytes()));
  }
}
