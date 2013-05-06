package water.score;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import javassist.*;
import water.parser.PMMLParser.DataTypes;
import water.parser.PMMLParser.Predicate;
import water.parser.PMMLParser;
import water.score.ScoreModel;
import water.util.Log;
import water.util.Log.Tag.Sys;

/**
 * Scorecard model - decision table.
 */
public class ScorecardModel extends ScoreModel {
  /** Initial score */
  final double _initialScore;
  /** The rules to each for each feature, they map 1-to-1 with the Model's
   *  column list.  */
  final RuleTable _rules[];

  /** Score this model on the specified row of data.  */
  public double score(final HashMap<String, Comparable> row ) {
    // By default, use the scoring interpreter.  The Builder JITs a new
    // subclass with an overloaded 'score(row)' call which has a JIT'd version
    // of the rules.  i.e., calling 'score(row)' on the returned ScorecardModel
    // instance runs the fast version, but you can cast to the base version if
    // you want the interpreter.
    return score_interpreter(row);
  }
  // Use the rule interpreter
  public double score_interpreter(final HashMap<String, Comparable> row ) {
    double score = _initialScore;
    for( int i=0; i<_rules.length; i++ )
      score += _rules[i].score(row.get(_colNames[i]));
    return score;
  }
  public double score(int[] MAP, String[] SS, double[] DS) {
    return score_interpreter(MAP,SS,DS);
  }
  private double score_interpreter(int[] MAP, String[] SS, double[] DS) {
    double score = _initialScore;
    for( int i=0; i<_rules.length; i++ ) {
      int idx = MAP[i];
      String ss = idx==-1 ? null       : SS[idx];
      double dd = idx==-1 ? Double.NaN : DS[idx];
      double s = _rules[i].score(ss,dd);
      score += s;
    }
    return score;
  }

  // JIT a score method with signature 'double score(HashMap row)'
  public void makeScoreHashMethod(CtClass scClass) {
    // Map of previously extracted PMML names, and their java equivs
    HashMap<String,String> vars = new HashMap<String,String>();
    StringBuilder sb = new StringBuilder();
    sb.append("double score( java.util.HashMap row ) {\n"+
              "  double score = "+_initialScore+";\n");
    try {
      for( int i=0; i<_rules.length; i++ )
        _rules[i].makeFeatureHashMethod(sb,vars,scClass);
      sb.append("  return score;\n}\n");
      CtMethod happyMethod = CtMethod.make(sb.toString(),scClass);
      scClass.addMethod(happyMethod);
    } catch( Exception re ) {
      Log.err(Sys.SCORM,"Crashing:"+sb.toString(), new RuntimeException(re));
    }
  }

  public void makeScoreAryMethod(CtClass scClass) {
    // Map of previously extracted PMML names, and their java equivs
    HashMap<String,String> vars = new HashMap<String,String>();
    StringBuilder sb = new StringBuilder();
    sb.append("double score( int[] MAP, java.lang.String[] SS, double[] DS ) {\n"+
              "  double score = "+_initialScore+";\n");
    try {
      for( int i=0; i<_rules.length; i++ )
        _rules[i].makeFeatureAryMethod(sb,vars,scClass,i);
      sb.append("  return score;\n}\n");

      CtMethod happyMethod = CtMethod.make(sb.toString(),scClass);
      scClass.addMethod(happyMethod);

    } catch( Exception re ) {
      Log.err(Sys.SCORM,"Crashing:"+sb.toString(), new RuntimeException(re));
    }
  }

  // Return the java-equivalent from the PMML variable name, creating and
  // installing it as needed.  If the value is created, we also emit Java code
  // to emit it at runtime.
  public static String getName( String pname, DataTypes type, StringBuilder sb ) {
    String jname = xml2jname(pname);

    // Emit the code to do the load
    return jname;
  }

  /** Feature decision table */
  public static class RuleTable {
    final String _name;
    final Rule[] _rule;
    final DataTypes _type;
    final double _baseScore;

    public RuleTable(String name, DataTypes type, Rule[] decisions, double baseScore) { _name = name; _type = type; _rule = decisions; _baseScore = baseScore; }

    public void makeFeatureHashMethod( StringBuilder sbParent, HashMap<String,String> vars, CtClass scClass ) {
      if( _type == null ) {
        Log.warn("Ignore untyped feature "+_name);
        return;
      }
      String jname = xml2jname(_name);
      StringBuilder sb = new StringBuilder();
      sb.append("double ").append(jname).append("( java.util.HashMap row ) {\n"+
                "  double score = 0;\n");
      switch( _type ) {
      case STRING : sb.append("  String " ); break;
      case BOOLEAN: sb.append("  double "); break;
      default     : sb.append("  double " ); break;
      }
      sb.append(jname);
      switch( _type ) {
      case STRING : sb.append(" = water.parser.PMMLParser.getString (row,\""); break;
      case BOOLEAN: sb.append(" = water.parser.PMMLParser.getBoolean(row,\"" ); break;
      default     : sb.append(" = water.parser.PMMLParser.getNumber (row,\""  ); break;
      }
      sb.append(_name).append("\");\n");
      sb.append("  if( false ) ;\n");
      for (Rule r : _rule)
        if( _type == DataTypes.STRING) r.toJavaStr(sb,jname);
        else if( _type == DataTypes.BOOLEAN) r.toJavaBool(sb,jname);
        else r.toJavaNum(sb,jname);
      // close the dangling 'else' from all the prior rules
      sb.append("  return score;\n}\n");
      sbParent.append("  score += ").append(jname).append("(row);\n");

      // Now install the method
      try {
        CtMethod happyMethod = CtMethod.make(sb.toString(),scClass);
        scClass.addMethod(happyMethod);
      } catch( Exception re ) {
        Log.err(Sys.SCORM,"Crashing:"+sb.toString(), new RuntimeException(re));
      }
    }

    public void makeFeatureAryMethod( StringBuilder sbParent, HashMap<String,String> vars, CtClass scClass, int fidx ) {
      if( _type == null ) return; // Untyped, ignore
      String jname = xml2jname(_name);
      StringBuilder sb = new StringBuilder();
      sb.append("double ").append(jname);
      sb.append("( int[]MAP, java.lang.String[]SS, double[]DS ) {\n"+
                "  double score = 0;\n"+
                "  int didx=MAP[").append(fidx).append("];\n");
      switch( _type ) {
      case STRING : sb.append("  String " ); break;
      case BOOLEAN: sb.append("  boolean "); break;
      default     : sb.append("  double " ); break;
      }
      sb.append(jname);
      switch( _type ) {
      case STRING : sb.append(" = didx==-1 ? null : SS[didx];\n"); break;
      case BOOLEAN: sb.append(" = didx==-1 ? false : DS[didx]==1.0;\n"); break;
      default     : sb.append(" = didx==-1 ? Double.NaN : DS[didx];\n" ); break;
      }
      sb.append("  if( false ) ;\n");
      for (Rule r : _rule)
        if( _type == DataTypes.STRING) r.toJavaStr(sb,jname);
        else if( _type == DataTypes.BOOLEAN) r.toJavaBool(sb,jname);
        else r.toJavaNum(sb,jname);
      // close the dangling 'else' from all the prior rules
      sb.append("  return score;\n}\n");
      sbParent.append("  score += ").append(jname).append("(MAP,SS,DS);\n");

      // Now install the method
      try {
        CtMethod happyMethod = CtMethod.make(sb.toString(),scClass);
        scClass.addMethod(happyMethod);

      } catch( Exception re ) {
        Log.err(Sys.SCORM,"Crashing:"+sb.toString(), new RuntimeException(re));
      }
    }

    // The rule interpreter
    double score(Comparable value) {
      double score = 0;
      for (Rule r : _rule) {
        if( r.match(value) ) {
          score += r._score;
          break;
        }
      }
      return score;
    }

    double score(String s, double d) {
      double score = 0;
      for (Rule r : _rule) {
        if( r.match(s,d) ) {
          score += r._score;
          break;
        }
      }
      return score;
    }

    @Override
    public String toString() {
      return "RuleTable [_name=" + _name + ", _rule=" + Arrays.toString(_rule) + ", _type=" + _type + " baseScore="+_baseScore+"]";
    }
  }

  /** Scorecard decision rule */
  public static class Rule {
    final double _score;
    final Predicate _predicate;
    public Rule(double score, Predicate pred) { assert pred != null; _score = score; _predicate = pred; }
    boolean match(Comparable value) { return _predicate.match(value); }
    boolean match(String s, double d) { return _predicate.match(s,d); }
    @Override public String toString() { return _predicate.toString() + " => " + _score; }
    public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      sb.append("  else if( ");
      return _predicate.toJavaNum(sb,jname).append(" ) score += ").append(_score).append(";\n");
    }
    public StringBuilder toJavaBool( StringBuilder sb, String jname ) {
      sb.append("  else if( ");
      return _predicate.toJavaBool(sb,jname).append(" ) score += ").append(_score).append(";\n");
    }
    public StringBuilder toJavaStr( StringBuilder sb, String jname ) {
      sb.append("  else if( ");
      return _predicate.toJavaStr(sb,jname).append(" ) score += ").append(_score).append(";\n");
    }
    String unique_name() { return _predicate.unique_name(); }
  }

  @Override
  public String toString() {
    return super.toString()+", _initialScore=" + _initialScore;
  }

  private ScorecardModel(String name, String[] colNames, double initialScore, RuleTable[] rules) {
    super(name,colNames);
    assert colNames.length==rules.length;
    _initialScore = initialScore;
    _rules = rules;
  }
  protected ScorecardModel(ScorecardModel base) {
    this(base._name,base._colNames,base._initialScore,base._rules);
  }


  /** Scorecard model builder: JIT a subclass with the fast version wired in to
   *  'score(row)' */
  public static ScorecardModel make(final String name, final double initialScore, RuleTable[] rules) {
    // Get the list of features
    String[] colNames = new String[rules.length];
    for( int i=0; i<rules.length; i++ )
      colNames[i] = rules[i]._name;

    // javassist support for rewriting class files
    ClassPool _pool = ClassPool.getDefault();
    try {
      // Make a javassist class in the java hierarchy
      String cname = uniqueClassName(name);
      CtClass scClass = _pool.makeClass(cname);
      CtClass baseClass = _pool.get("water.score.ScorecardModel"); // Full Name Lookup
      scClass.setSuperclass(baseClass);
      // Produce the scoring method(s)
      ScorecardModel scm = new ScorecardModel(name, colNames,initialScore, rules);
      scm.makeScoreHashMethod(scClass);
      scm.makeScoreAryMethod(scClass);
      // Produce a 1-arg constructor
      String cons = "  public "+cname+"(water.score.ScorecardModel base) { super(base); }";
      CtConstructor happyConst = CtNewConstructor.make(cons,scClass);
      scClass.addConstructor(happyConst);

      Class myClass = scClass.toClass(ScorecardModel.class.getClassLoader(), null);
      Constructor<ScorecardModel> co = myClass.getConstructor(ScorecardModel.class);
      ScorecardModel jitted_scm = co.newInstance(scm);
      return jitted_scm;

    } catch( Exception e ) {
      Log.err(Sys.SCORM,"Javassist failed",e);
    }
    return null;
  }

  // -------------------------------------------------------------------------
  public static ScorecardModel parse( PMMLParser pmml ) {
    HashMap<String,String> attrs = pmml.attrs();
    pmml.expect('>');
    pmml.skipWS().expect('<').pGeneric("MiningSchema");
    pmml.skipWS().expect('<').pGeneric("Output");
    pmml.skipWS().expect('<');
    RuleTable[] rules = pCharacteristics(pmml);
    pmml.skipWS().expect("</Scorecard>");
    String is = attrs.get("initialScore");
    double initialScore = is==null?0:PMMLParser.getNumber(is);
    return make(attrs.get("modelName"), initialScore, rules);
  }

  private static RuleTable[] pCharacteristics( PMMLParser pmml ) {
    pmml.expect("Characteristics>");
    ArrayList<RuleTable> rts = new ArrayList();
    while( pmml.skipWS().expect('<').peek() != '/' )
      rts.add(pCharacteristic(pmml));
    pmml.expect("/Characteristics>");
    return rts.toArray(new RuleTable[0]);
  }

  private static RuleTable pCharacteristic( PMMLParser pmml ) {
    HashMap<String,String> attrs = pmml.expect("Characteristic").attrs();
    pmml.expect('>');
    ArrayList<Rule> rules = new ArrayList();
    while( pmml.skipWS().expect('<').peek() != '/' )
      rules.add(pAttribute(pmml));
    pmml.expect("/Characteristic>");
    String name = rules.get(0).unique_name();
    DataTypes t = pmml._types.get(name);
    String bls = attrs.get("baselineScore");
    double baseScore = bls == null?0:PMMLParser.getNumber(bls);
    return new RuleTable(name,t,rules.toArray(new Rule[0]),baseScore);
  }

  private static Rule pAttribute( PMMLParser pmml ) {
    HashMap<String,String> attrs = pmml.expect("Attribute").attrs();
    pmml.expect('>').skipWS().expect('<');
    Predicate pred = pmml.pPredicate();
    pmml.skipWS().expect("</Attribute>");
    String ps = attrs.get("partialScore");
    double partialScore = ps==null?0:PMMLParser.getNumber(ps);
    return new Rule(partialScore,pred);
  }
}
