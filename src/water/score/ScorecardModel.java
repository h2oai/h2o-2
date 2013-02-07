package water.score;

import java.util.*;
import water.H2O;
import javassist.*;

/**
 * Scorecard model - decision table.
 *
 */
public class ScorecardModel {

  /** Name */
  final String _name;
  /** Output column */
  String _predictor;
  /** Initial score */
  final double _initialScore;

  /** Scorecard features */
  final Map<String, RuleTable> _features;

  final static HashMap<String,String> CLASS_NAMES = new HashMap<String,String>();


  // Convert an XML name to a java name
  final static String xml2jname( String xml, HashMap<String,String> vars ) {
    // Convert pname to a valid java name
    StringBuilder nn = new StringBuilder();
    char[] cs = xml.toCharArray();
    if( !Character.isJavaIdentifierStart(cs[0]) )
      nn.append('X');
    for( char c : cs ) {
      if( !Character.isJavaIdentifierPart(c) ) {
        nn.append('_');
      } else {
        nn.append(c);
      }
    }
    String jname = nn.toString();
    if( vars.containsKey(jname) ) {
      throw H2O.unimpl();
      //int i=0;
      //while( vars.containsKey(jname+i) ) i++;
      //jname = jname+i;
    }
    return jname;
  }

  protected ScorecardModel(final String name, double initialScore) { 
    _name = name; 
    _initialScore = initialScore; 
    _features = new HashMap<String, ScorecardModel.RuleTable>(); 
  }

  /** Score this model on the specified row of data.  */
  public double score(final HashMap<String, Comparable> row ) {
    return score0(row);
  }
  // Use the rule interpreter
  public double score0(final HashMap<String, Comparable> row ) {
    double score = _initialScore;
    for (String k : _features.keySet()) {
      RuleTable ruleTable = _features.get(k);
      double s = ruleTable.score(row.get(k));
      score += s;
    }
    return score;
  }

  public String toJava() {
    // Map of previously extracted PMML names, and their java equivs
    HashMap<String,String> vars = new HashMap<String,String>();
    StringBuilder sb = new StringBuilder();
    sb.append("double score( java.util.HashMap row ) {\n"+
              "  double score = "+_initialScore+";\n");
    try {
      for (String k : _features.keySet())
        sb = _features.get(k).toJava(sb,vars);
      sb.append("  return score;\n}\n");
    } catch( RuntimeException re ) {
      System.err.println("=== crashing ===");
      System.err.println(sb.toString());
      throw re;
    }
    return sb.toString();
  }

  // Return the java-equivalent from the PMML variable name, creating and
  // installing it as needed.  If the value is created, we also emit Java code
  // to emit it at runtime.
  public static String getName( String pname, DataTypes type, HashMap<String,String> vars, StringBuilder sb ) {
    String jname = vars.get(pname);
    if( jname != null ) {
      throw H2O.unimpl();
      //return jname;
    }
    jname = xml2jname(pname,vars);
    vars.put(pname,jname);

    // Emit the code to do the load
    if( type == DataTypes.STRING ) {
      sb.append("  String ").append(jname).append(" = (String)row.get(\"").append(pname).append("\");\n");
    } else if( type == DataTypes.BOOLEAN ) {
      sb.append("  boolean ").append(jname).append(" = getBoolean(row,\"").append(pname).append("\");\n");
    } else {
      sb.append("  double ").append(jname).append(" = getNumber(row,\"").append(pname).append("\");\n");
    }
    return jname;
  }

  /** Feature decision table */
  public static class RuleTable<T> {
    final String     _name;
    final Rule<T>[]  _rule;
    final DataTypes  _type;

    public RuleTable(final String name, final DataTypes type, final Rule<T>[] decisions) { _name = name; _type = type; _rule = decisions; }

    public StringBuilder toJava( StringBuilder sb, HashMap<String,String> vars ) {
      String pname = getName(_name,_type,vars,sb);
      sb.append("  if( false ) ;\n");
      for (Rule r : _rule) 
        if( _type == DataTypes.STRING) r.toJavaStr(sb,pname);
        else if( _type == DataTypes.BOOLEAN) r.toJavaBool(sb,pname);
        else r.toJavaNum(sb,pname);
      // close the dangling 'else' from all the prior rules
      sb.append("\n");
      return sb;
    }

    double score(T value) {
      /* The code introduced by cyprien, but I do not see test case for this flow.
       * But I leave here for now intentionally.
      if(value instanceof String) {
        switch(_type) {
          case BOOLEAN:
            value = (T) Boolean.valueOf((String) value);
            break;
          case INT:
            value = (T) new Long(Double.valueOf((String) value).longValue());
            break;
          case DOUBLE:
            value = (T) Double.valueOf((String) value);
            break;
          case STRING:
            break;
        }
      } else if(value instanceof Integer) {
        value = (T) new Long(((Integer) value).intValue());
      } else if(value instanceof Float) {
        value = (T) new Double(((Float) value).floatValue());
      } */

      double score = 0;
      for (Rule r : _rule) score += r.score(value);
      return score;
    }

    @Override
    public String toString() {
      return "RuleTable [_name=" + _name + ", _rule=" + Arrays.toString(_rule) + ", _type=" + _type + "]";
    }
  }

  /** Scorecard decision rule */
  public static class Rule<T> {
    final double _score;
    final Predicate<T> _predicate;
    public Rule(double score, Predicate<T> pred) { _score = score; _predicate = pred; }
    double score(T value) { return _predicate!=null && _predicate.match(value) ? _score : 0; }
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
  }

  public static abstract class Predicate<T> {
    abstract boolean match(T value);
    abstract StringBuilder toJavaNum( StringBuilder sb, String jname );
    StringBuilder toJavaBool( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    StringBuilder toJavaStr( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
  }
  /** Less or equal */
  public static class LessOrEqual<T extends Comparable<T>> extends Predicate<T> {
    T _value;
    public LessOrEqual(T value) { _value = value; }
    @Override boolean match(T value) {
      if( value != null && _value != null && value.getClass() != _value.getClass() ) {
        if(value.getClass() == Long.class &&
           _value.getClass() == Double.class ) {
          long   val1 = ((Long)((Object)value)).longValue();
          double val2 = ((Double)((Object)_value)).doubleValue();
          return val1 <= val2;
        }
      }
      return value!=null && _value.compareTo(value) >= 0;
    }
    @Override public String toString() { return "X<=" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append("<=").append(d);
    }
  }

  public static class LessThan<T extends Comparable<T>> extends LessOrEqual<T> {
    public LessThan(T value) { super(value); }
    @Override boolean match(T value) { return value!=null && _value.compareTo(value) > 0; }
    @Override public String toString() { return "X<" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append("<").append(d);
    }
  }

  public static class GreaterOrEqual<T extends Comparable<T>> extends LessThan<T> {
    public GreaterOrEqual(T value) { super(value); }
    @Override boolean match(T value) { return value!=null && ! super.match(value); }
    @Override public String toString() { return "X>=" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append(">=").append(d);
    }
  }

  public static class GreaterThan<T extends Comparable<T>> extends LessOrEqual<T> {
    public GreaterThan(T value) { super(value); }
    @Override boolean match(T value) { return value!=null && ! super.match(value); }
    @Override public String toString() { return "X>" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append(">").append(d);
    }
  }

  public static class IsMissing<T> extends Predicate<T> {
    @Override boolean match(T value) { return value==null; }
    @Override public String toString() { return "isMissing"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { 
      return sb.append("Double.isNaN("+jname+")");
    }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) { 
      return sb.append(jname).append("==null");
    }
  }

  public static class Equals<T extends Comparable<T>> extends Predicate<T> {
    T _value;
    public Equals(T value) { _value = value; }
    @Override boolean match(T value) { return value!=null && _value.compareTo(value) == 0; }
    @Override public String toString() { return "X==" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append("==").append(d);
    }
    @Override StringBuilder toJavaBool( StringBuilder sb, String jname ) { 
      boolean b = ((Boolean)_value);
      return sb.append(jname).append("==").append(b);
    }
    @Override StringBuilder toJavaStr( StringBuilder sb, String jname ) { 
      String s = ((String)_value);
      return sb.append("\"").append(s).append("\".equals(").append(jname).append(")");
    }
  }

  public static abstract class CompoundPredicate<T> extends Predicate<T> {
    Predicate<T> _l,_r;
    public final void add(Predicate<T> pred) {
      assert _l== null || _r==null : "Predicate already filled";
      if (_l==null) _l = pred; else _r = pred;
    }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    public StringBuilder makeNum(StringBuilder sb, String jname, String rel) {
      sb.append("(");
      _l.toJavaNum(sb,jname);
      sb.append(" ").append(rel).append(" ");
      _r.toJavaNum(sb,jname);
      sb.append(")");
      return sb;
    }
    public StringBuilder makeStr(StringBuilder sb, String jname, String rel) {
      sb.append("(");
      _l.toJavaStr(sb,jname);
      sb.append(" ").append(rel).append(" ");
      _r.toJavaStr(sb,jname);
      sb.append(")");
      return sb;
    }
  }
  public static class And<T> extends CompoundPredicate<T> {
    @Override final boolean match(T value) { return _l.match(value) && _r.match(value); }
    @Override public String toString() { return "(" + _l.toString() + " and " + _r.toString() + ")"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { return makeNum(sb,jname,"&&"); }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) { return makeStr(sb,jname,"&&"); }
  }
  public static class Or<T> extends CompoundPredicate<T> {
    @Override final boolean match(T value) { return _l.match(value) || _r.match(value); }
    @Override public String toString() { return "(" + _l.toString() + " or " + _r.toString() + ")"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { return makeNum(sb,jname,"||"); }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) { return makeStr(sb,jname,"||"); }
  }

  public static abstract class SetPredicate<T> extends Predicate<T> {
    public T[] _values;
    public SetPredicate(T[] value) { _values = value; }
  }

  public static class IsIn<T> extends SetPredicate<T> {
    public IsIn(T[] value) { super(value); }
    @Override boolean match(T value) {
      for (T t : _values) if (t.equals(value)) return true;
      return false;
    }
    @Override public String toString() {
      String x = "";
      for (T s: _values) x += s.toString() + " ";
      return "X is in {" + x + "}"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    @Override StringBuilder toJavaStr( StringBuilder sb, String jname ) { 
      for( String s : (String[])_values )
        sb.append("\"").append(s).append("\".equals(").append(jname).append(") || ");
      return sb.append("false");
    }
  }

  public static class IsNotIn<T> extends IsIn<T> {
    public IsNotIn(T[] value) { super(value); }
    @Override boolean match(T value) { return ! super.match(value); }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    @Override StringBuilder toJavaStr( StringBuilder sb, String jname ) { 
      sb.append("!(");
      super.toJavaStr(sb,jname);
      return sb.append(")");
    }
  }

  @Override
  public String toString() {
    return "ScorecardModel [_name=" + _name + ", _predictor=" + _predictor + ", _initialScore=" + _initialScore + "]";
  }

  // Happy Helper Methods for the generated code
  public double getNumber( HashMap<String,Object> row, String s ) {
    Object o = row.get(s);
    if( o instanceof Number ) return ((Number)o).doubleValue();
    return Double.NaN;
  }
  public boolean getBoolean( HashMap<String,Object> row, String s ) {
    Object o = row.get(s);
    if( o instanceof Boolean ) return ((Boolean)o).booleanValue();
    if( o instanceof String && "true".equalsIgnoreCase((String)o) ) return true;
    return false;
  }


  /** Scorecard model builder */
  public static class Builder {
    ScorecardModel _scm;
    private ClassPool _pool; // The pool of altered classes

    public Builder(final String name, double initialScore) { _scm = new ScorecardModel(name, initialScore); }

    // JIT the fast version
    public final ScorecardModel build() { 
      String java = _scm.toJava();
      System.err.println(java);
      // javassist support for rewriting class files
      _pool = ClassPool.getDefault();
      try { 
        String cname = xml2jname(_scm._name,CLASS_NAMES);
        CtClass scClass = _pool.makeClass(cname);
        CtClass baseClass = _pool.get("water.score.ScorecardModel"); // Full Name Lookup
        scClass.setSuperclass(baseClass);
        CtMethod happyMethod = CtMethod.make(java,scClass);
        scClass.addMethod(happyMethod);

        String cons = "  public "+cname+"() { super(\""+_scm._name+"\","+_scm._initialScore+"); }";
        System.err.println(cons);
        CtConstructor happyConst = CtNewConstructor.make(cons,scClass);
        scClass.addConstructor(happyConst);
        Class myClass = scClass.toClass();
        ScorecardModel scm = (ScorecardModel)myClass.newInstance();
        return scm;
        
      } catch( Exception e ) {
        System.err.println("javassit failed: "+e);
        e.printStackTrace();
      }
      return  _scm; 
    }

    public final void addRuleTable(final String featureName, final DataTypes type, final List<Rule> rules) {
      _scm._features.put(featureName, new RuleTable(featureName, type, rules.toArray(new Rule[rules.size()])));
    }
  }

  /** Features datatypes promoted by PMML spec. */
  public enum DataTypes {
    DOUBLE("double"), INT("int"), BOOLEAN("boolean"), STRING("String");
    final String _jname;
    DataTypes( String jname ) { _jname = jname; }
    public static DataTypes parse(String s) {return DataTypes.valueOf(s.toUpperCase()); }
    public String jname() { return _jname; }
  }
}
