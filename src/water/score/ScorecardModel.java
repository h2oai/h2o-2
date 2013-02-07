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
  ArrayList<String> _features;
  ArrayList<RuleTable> _rules;

  final static HashMap<String,String> CLASS_NAMES = new HashMap<String,String>();

  // Convert an XML name to a java name
  final static String xml2jname( String xml ) {
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
    return jname;
  }

  protected ScorecardModel(final String name, double initialScore) { 
    _name = name; 
    _initialScore = initialScore; 
    _features = new ArrayList();
    _rules = new ArrayList();
  }


  // A mapping from the dense columns desired by the model, to the above
  // feature list, computed by asking the model for a mapping (given a list of
  // features).  Some features may be unused and won't appear in the mapping.
  // If the data row features list does not mention all the features the model
  // needs, then this map will contain a -1 for the missing feature index.
  public int[] columnMapping( String[] features ) {
    String[] fs = _features.toArray(new String[0]); // The model features
    int[] map = new int[fs.length];
    for( int i=0; i<fs.length; i++ ) {
      map[i] = -1;              // Assume it is missing
      for( int j=0; j<features.length; j++ ) {
        if( fs[i].equals(features[j]) ) {
          if( map[i] != -1 ) throw new IllegalArgumentException("duplicate feature "+fs[i]);
          map[i] = j;
        }
      }
      if( map[i] == -1 ) System.err.println("[h2o] Warning: feature "+fs[i]+" used by the model is not in the provided feature list from the data");
    }
    return map;
  }

  /** Score this model on the specified row of data.  */
  public double score(final HashMap<String, Comparable> row ) {
    return score0(row);
  }
  // Use the rule interpreter
  public double score0(final HashMap<String, Comparable> row ) {
    double score = _initialScore;
    for( int i=0; i<_features.size(); i++ ) {
      RuleTable ruleTable = _rules.get(i);
      double s = ruleTable.score(row.get(_features.get(i)));
      score += s;
    }
    return score;
  }
  public double score(int[] MAP, String[] SS, double[] DS) {
    return score0(MAP,SS,DS);
  }
  public double score0(int[] MAP, String[] SS, double[] DS) {
    double score = _initialScore;
    for( int i=0; i<_features.size(); i++ ) {
      RuleTable ruleTable = _rules.get(i);
      int idx = MAP[i];
      String ss = idx==-1 ? null       : SS[idx];
      double dd = idx==-1 ? Double.NaN : DS[idx];
      double s = ruleTable.score(ss,dd);
      score += s;
    }
    return score;
  }

  public void makeScoreHashMethod(CtClass scClass) {
    // Map of previously extracted PMML names, and their java equivs
    HashMap<String,String> vars = new HashMap<String,String>();
    StringBuilder sb = new StringBuilder();
    sb.append("double score( java.util.HashMap row ) {\n"+
              "  double score = "+_initialScore+";\n");
    try {
      for( int i=0; i<_features.size(); i++ )
        _rules.get(i).makeFeatureHashMethod(sb,vars,scClass);
      sb.append("  return score;\n}\n");

      CtMethod happyMethod = CtMethod.make(sb.toString(),scClass);
      scClass.addMethod(happyMethod);

    } catch( Exception re ) {
      System.err.println("=== crashing ===");
      System.err.println(sb.toString());
      throw new Error(re);
    } finally {
    }
  }

  public void makeScoreAryMethod(CtClass scClass) {
    // Map of previously extracted PMML names, and their java equivs
    HashMap<String,String> vars = new HashMap<String,String>();
    StringBuilder sb = new StringBuilder();
    sb.append("double score( int[] MAP, java.lang.String[] SS, double[] DS ) {\n"+
              "  double score = "+_initialScore+";\n");
    try {
      for( int i=0; i<_features.size(); i++ )
        _rules.get(i).makeFeatureAryMethod(sb,vars,scClass,i);
      sb.append("  return score;\n}\n");

      CtMethod happyMethod = CtMethod.make(sb.toString(),scClass);
      scClass.addMethod(happyMethod);

    } catch( Exception re ) {
      System.err.println("=== crashing ===");
      System.err.println(sb.toString());
      throw new Error(re);
    } finally {
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
  public static class RuleTable<T> {
    final String     _name;
    final Rule<T>[]  _rule;
    final DataTypes  _type;

    public RuleTable(final String name, final DataTypes type, final Rule<T>[] decisions) { _name = name; _type = type; _rule = decisions; }

    public void makeFeatureHashMethod( StringBuilder sbParent, HashMap<String,String> vars, CtClass scClass ) {
      String jname = xml2jname(_name);
      StringBuilder sb = new StringBuilder();
      sb.append("double ").append(jname).append("( java.util.HashMap row ) {\n"+
                "  double score = 0;\n");
      switch( _type ) {
      case STRING : sb.append("  String " ); break;
      case BOOLEAN: sb.append("  boolean "); break;
      default     : sb.append("  double " ); break;
      }
      sb.append(jname);
      switch( _type ) {
      case STRING : sb.append(" = (String)row.get(\""); break;
      case BOOLEAN: sb.append(" = getBoolean(row,\"" ); break;
      default     : sb.append(" = getNumber(row,\""  ); break;
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
        System.err.println("=== crashing ===");
        System.err.println(sb.toString());
        throw new Error(re);
      } finally {
      }
    }

    public void makeFeatureAryMethod( StringBuilder sbParent, HashMap<String,String> vars, CtClass scClass, int fidx ) {
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
        System.err.println("=== crashing ===");
        System.err.println(sb.toString());
        throw new Error(re);
      } finally {
      }
    }

    // The rule interpreter
    double score(T value) {
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
      return "RuleTable [_name=" + _name + ", _rule=" + Arrays.toString(_rule) + ", _type=" + _type + "]";
    }
  }

  /** Scorecard decision rule */
  public static class Rule<T> {
    final double _score;
    final Predicate<T> _predicate;
    public Rule(double score, Predicate<T> pred) { _score = score; _predicate = pred; }
    boolean match(T value) { return _predicate.match(value); }
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
  }

  public static abstract class Predicate<T> {
    abstract boolean match(T value);
    abstract boolean match(String s, double d);
    abstract StringBuilder toJavaNum( StringBuilder sb, String jname );
    StringBuilder toJavaBool( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    StringBuilder toJavaStr( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
  }
  /** Less or equal */
  public static class LessOrEqual<T extends Comparable<T>> extends Predicate<T> {
    T _value;
    double _d;
    public LessOrEqual(T value, double d) { _value = value; _d=d;}
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
    @Override boolean match(String s, double d) { return d <= _d; }
    @Override public String toString() { return "X<=" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append("<=").append(d);
    }
  }

  public static class LessThan<T extends Comparable<T>> extends LessOrEqual<T> {
    public LessThan(T value, double d) { super(value,d); }
    @Override boolean match(T value) { return value!=null && _value.compareTo(value) > 0; }
    @Override boolean match(String s, double d) { return d < _d; }
    @Override public String toString() { return "X<" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append("<").append(d);
    }
  }

  public static class GreaterOrEqual<T extends Comparable<T>> extends LessThan<T> {
    public GreaterOrEqual(T value, double d) { super(value,d); }
    @Override boolean match(T value) { return value!=null && ! super.match(value); }
    @Override boolean match(String s, double d) { return d >= _d; }
    @Override public String toString() { return "X>=" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append(">=").append(d);
    }
  }

  public static class GreaterThan<T extends Comparable<T>> extends LessOrEqual<T> {
    public GreaterThan(T value, double d) { super(value,d); }
    @Override boolean match(T value) { return value!=null && ! super.match(value); }
    @Override boolean match(String s, double d) { return d > _d; }
    @Override public String toString() { return "X>" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)_value).doubleValue();
      return sb.append(jname).append(">").append(d);
    }
  }

  public static class IsMissing<T> extends Predicate<T> {
    @Override boolean match(T value) { return value==null; }
    @Override boolean match(String s, double d) { return Double.isNaN(d); }
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
    double _d;
    public Equals(T value, double d) { _value = value; _d = d; }
    @Override boolean match(T value) { 
      return value!=null && _value.compareTo(value) == 0; 
    }
    @Override boolean match(String s, double d) { 
      return Double.isNaN(_d) ? ((String)((Object)_value)).equals(s) : (d==_d);
    }
    @Override public String toString() { return "X==" + _value; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      double d = ((Number)((Object)_value)).doubleValue();
      return sb.append(jname).append("==").append(d);
    }
    @Override StringBuilder toJavaBool( StringBuilder sb, String jname ) { 
      boolean b = ((Boolean)((Object)_value));
      return sb.append(jname).append("==").append(b);
    }
    @Override StringBuilder toJavaStr( StringBuilder sb, String jname ) { 
      String s = ((String)((Object)_value));
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
    @Override final boolean match(String s, double d) { return _l.match(s,d) && _r.match(s,d); }
    @Override public String toString() { return "(" + _l.toString() + " and " + _r.toString() + ")"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { return makeNum(sb,jname,"&&"); }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) { return makeStr(sb,jname,"&&"); }
  }
  public static class Or<T> extends CompoundPredicate<T> {
    @Override final boolean match(T value) { return _l.match(value) || _r.match(value); }
    @Override final boolean match(String s, double d) { return _l.match(s,d) || _r.match(s,d); }
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
    @Override boolean match(String s, double d) {
      for (String t : (String[])_values) if (t.equals(s)) return true;
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
    @Override boolean match(String s, double d) { return ! super.match(s,d); }
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
    // hint to the jit to do a instanceof breakdown tree
    if( o instanceof Double ) return ((Double)o).doubleValue();
    if( o instanceof Long   ) return ((Long  )o).doubleValue();
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
      // javassist support for rewriting class files
      _pool = ClassPool.getDefault();
      try { 
        // Make a unique class name
        String cname = xml2jname(_scm._name);
        if( CLASS_NAMES.containsKey(cname) ) {
          throw H2O.unimpl();
          //int i=0;
          //while( vars.containsKey(jname+i) ) i++;
          //jname = jname+i;
        }

        CtClass scClass = _pool.makeClass(cname);
        CtClass baseClass = _pool.get("water.score.ScorecardModel"); // Full Name Lookup
        scClass.setSuperclass(baseClass);
        // Produce the scoring method
        _scm.makeScoreHashMethod(scClass);
        _scm.makeScoreAryMethod(scClass);

        String cons = "  public "+cname+"() { super(\""+_scm._name+"\","+_scm._initialScore+"); }";
        CtConstructor happyConst = CtNewConstructor.make(cons,scClass);
        scClass.addConstructor(happyConst);
        Class myClass = scClass.toClass();
        ScorecardModel scm = (ScorecardModel)myClass.newInstance();
        scm._features = _scm._features;
        scm._rules    = _scm._rules   ;
        return scm;
        
      } catch( Exception e ) {
        System.err.println("javassist failed: "+e);
        e.printStackTrace();
      }
      return  _scm; 
    }

    public final void addRuleTable(final String featureName, final DataTypes type, final List<Rule> rules) {
      _scm._features.add(featureName);
      _scm._rules   .add(new RuleTable(featureName, type, rules.toArray(new Rule[rules.size()])));
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
