package water.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import water.H2O;
import water.score.*;
import water.util.Log;

/** Parse PMML models
 *
 *  Full recursive-descent style parsing.  MUCH easier to track the control
 *  flows than a SAX-style parser, and does not require the entire doc like a
 *  DOM-style.  More tightly tied to the XML structure, but in theory PMML is
 *  a multi-vendor standard and fairly stable.
 *
 *  Like a good R-D parser, uses a separate function for parsing each XML
 *  element.  Each function expects to be at a particular parse-point
 *  (generally after the openning '&lt;' and before the tag is parsed), and
 *  always leaves the parse just after the close-tag '&gt;'.  The semantic
 *  interpretation is then interleaved with the parsing, with higher levels
 *  passing down needed info to lower element levels, and lower levels
 *  directly returning results to the higher levels.
 *
 *  @author <a href="mailto:cliffc@h2o.ai"></a>
 *  @version 1.0
 */
public class PMMLParser {
  final InputStream _is; // Stream to read from
  int [] _buf;           // Pushback buffer
  int _idx;              // Pushback index

  /** Features datatypes promoted by PMML spec.  These appear before we know what
   *  kind of model we are parsing, so must be parsed globally (for all models).  */
  public static enum DataTypes {
    DOUBLE("double"), INT("int"), BOOLEAN("boolean"), STRING("String");
    final String _jname;
    DataTypes( String jname ) { _jname = jname; }
    public static DataTypes parse(String s) {return DataTypes.valueOf(s.toUpperCase()); }
    public String jname() { return _jname; }
  }
  // Global (per-parse) type mappings.  Examples:
  // <DataField name="Species" optype="categorical" dataType="string">
  // <DataField name="creditScore" dataType="double" optype="continuous" />
  public final HashMap<String,DataTypes> _types = new HashMap();
  // Global (per-parse) enum mappings.  Examples:
  //<DataField name="Species" optype="categorical" dataType="string">
  // <Value value="setosa"/>
  // <Value value="versicolor"/>
  // <Value value="virginica"/>
  //</DataField>
  public final HashMap<String,String[]> _enums = new HashMap();

  public static class ParseException extends RuntimeException {
    public ParseException( String msg ) { super(msg); }
  }

  public static ScoreModel parse( InputStream is ) {
    return new PMMLParser(is).parse();
  }
  private PMMLParser(InputStream is) { _is = is; _buf=new int[2]; }
  private ScoreModel parse() {
    skipWS().expect('<');
    if( peek()=='?' ) pXMLVersion().skipWS().expect('<');
    return pPMML();
  }

  // Parse/skip XML version element
  private PMMLParser pXMLVersion() {
    expect("?xml");
    while( peek() != '?' ) {  // Look for closing '?>'
      String attr = skipWS().token();
      String val = skipWS().expect('=').str();
    }
    return expect("?>");
  }

  // The whole PMML element.  Breaks out the different model types.
  private ScoreModel pPMML() {
    expect("PMML").skipAttrs();
    expect('>').skipWS().expect('<');
    pGeneric("Header");         // Skip a generic XML subtree
    skipWS().expect('<');
    pDataDictionary();
    String mtag = skipWS().expect('<').token();
    ScoreModel scm = null;
    if( "Scorecard"  .equals(mtag) ) scm = ScorecardModel.parse(this);
    //if( "MiningModel".equals(mtag) ) scm =   RFScoreModel.parse(this);
    skipWS().expect("</PMML>");
    return scm;
  }

  // Skip generic XML subtree
  public PMMLParser pGeneric(String hdr) {
    String t = token();
    assert hdr==null || t.equals(hdr);
    skipAttrs();
    if( peek()=='/' ) return expect("/>");
    expect('>');
    while( true ) {
      if( get()=='<' ) {
        if( peek()=='/' ) return expect('/').expect(t).expect('>');
        pGeneric(null);
      }
    }
  }

  // Reads the DataDictionary element, accumulating fields & types
  private PMMLParser pDataDictionary() {
    expect("DataDictionary").skipAttrs();
    expect('>');
    while( skipWS().expect('<').peek() != '/' )  pDataField();
    return expect("/DataDictionary>");
  }
  // Read a single field name & type, plus any enum/factor/level info
  private PMMLParser pDataField() {
    HashMap<String,String> attrs = expect("DataField").attrs();
    String name = attrs.get("name");
    _types.put(name, DataTypes.parse(attrs.get("dataType")));
    if( peek()=='/' ) return expect("/>");
    expect('>');
    ArrayList<String> str = new ArrayList();
    while( skipWS().expect('<').peek() != '/' ) str.add(pDataFieldValue());
    String[] ss = str.toArray(new String[0]);
    Arrays.sort(ss,null);
    _enums.put(name,ss);
    return expect("/DataField>");
  }
  // A single enum/level value
  private String pDataFieldValue() {
    expect("Value").skipWS().expect("value=");
    String str = str();
    expect("/>");
    return str;
  }


  // Parse out an PMML predicate.  Common across several models.
  public Predicate pPredicate() {
    String t = token();
    HashMap<String,String> attrs = attrs();
    if( "SimplePredicate"   .equals(t) ) return    pSimplePredicate(attrs);
    if( "CompoundPredicate" .equals(t) ) return  pCompoundPredicate(attrs);
    if( "SimpleSetPredicate".equals(t) ) return pSimpleSetPredicate(attrs);
    if( "True".equals(t) ) { expect("/>"); return new True(); }
    expect("unhandled_predicate");
    return null;
  }

  private Predicate pSimplePredicate(HashMap<String,String> attrs) {
    expect("/>");
    return Comparison.makeSimple(attrs.get("field"),
                                 Operators.valueOf(attrs.get("operator")),
                                 attrs.get("value"));
  }

  private Predicate pCompoundPredicate(HashMap<String,String> attrs) {
    expect(">");
    CompoundPredicate cp = CompoundPredicate.make(BooleanOperators.valueOf(attrs.get("booleanOperator")));
    cp._l = skipWS().expect('<').pPredicate();
    cp._r = skipWS().expect('<').pPredicate();
    skipWS().expect("</CompoundPredicate>");
    return cp;
  }

  private Predicate pSimpleSetPredicate(HashMap<String,String> attrs) {
    expect('>');
    IsIn in = IsIn.make(attrs.get("field"),
                        BooleanOperators.valueOf(attrs.get("booleanOperator")));
    in._values = skipWS().expect('<').pArray();
    skipWS().expect("</SimpleSetPredicate>");
    return in;
  }

  private String[] pArray() {
    HashMap<String,String> attrs = expect("Array").attrs();
    expect('>');
    int len = Integer.parseInt(attrs.get("n"));
    assert attrs.get("type").equals("string");
    String[] ss = new String[len];
    for( int i=0; i<len; i++ ) {
      int b = skipWS().peek();
      // Allow both quoted and unquoted tokens
      ss[i] = (b=='&' || b=='"') ? str() : token();
    }
    skipWS().expect("</Array>");
    return ss;
  }

  public int get() {
    if( _idx > 0 ) return _buf[--_idx];
    try {
      int b = _is.read();
      if( b != -1 ) return b;
    } catch( IOException ioe ) { Log.err(ioe);  }
    throw new ParseException("Premature EOF");
  }
  public int peek() {
    if( _idx > 0 ) return _buf[_idx-1];
    try {
      int b = _is.read();
      if( b != -1 ) return push(b);
    } catch( IOException e ) { Log.err(e); }
    throw new ParseException("Premature EOF");
  }
  int push( int b ) { return (_buf[_idx++] = b); }

  public int qget() {
    int b = get();
    if( b!='&' ) return b;
    expect("quot;");
    return '"';
  }

  // Read from stream, skipping whitespace
  public PMMLParser skipWS() {
    int c;
    while( Character.isWhitespace(c=get()) ) ;
    push(c);
    return this;
  }

  // Assert correct token is found
  public PMMLParser expect( char tok ) {
    char c = (char)get();
    return c == tok ? this : barf(tok,c);
  }
  public PMMLParser expect( String toks ) {
    for( int i=0; i<toks.length(); i++ )
      expect(toks.charAt(i));
    return this;
  }
  public PMMLParser barf( char tok, char c ) {
    StringBuilder sb = new StringBuilder();
    sb.append("Expected '").append(tok).append("' but found '").append(c).append("'");
    int line=0;
    for( int i=0; i<512; i++ ) {
      try { c = (char)get(); } catch( ParseException ioe ) { break; }
      sb.append(c);
      if( c=='\n' && line++ > 2 ) break;
    }
    throw new ParseException(sb.toString());
  }

  // Read from stream a valid PMML token
  public String token() {
    int b = get();
    if( !Character.isJavaIdentifierStart(b) )
      throw new ParseException("Expected token start but found '"+(char)b+"'");
    StringBuilder sb = new StringBuilder();
    sb.append((char)b);
    b = get();
    while( Character.isJavaIdentifierPart(b) || b==':' ) {
      sb.append((char)b);
      b = get();
    }
    push(b);
    return sb.toString();
  }
  // Read from stream a "string".  Skips the trailing close-quote
  private String str() {
    int q = skipWS().qget();
    if( q!='"' && q!='\'' )
      throw new ParseException("Expected one of ' or \" but found '"+(char)q+"'");
    StringBuilder sb = new StringBuilder();
    int b = get();
    while( b != q ) {
      sb.append((char)b);
      b = qget();
    }
    return sb.toString();
  }

  // Any number of attributes, or '/' or '>'
  public HashMap<String,String> attrs() {
    HashMap<String,String> attrs = null;
    while( true ) {
      int b = skipWS().peek();
      if( b == '/' || b == '>' ) return attrs;
      if( attrs == null ) attrs = new HashMap();
      String attr = token();
      String val = skipWS().expect('=').str();
      attrs.put(attr,val);
    }
  }
  public void skipAttrs() {
    while( true ) {
      int b = skipWS().peek();
      if( b == '/' || b == '>' ) return;
      while( (b=get())!= '=' ) ;
      int q = skipWS().get();
      if( q!='"' && q!='\'' )
        throw new ParseException("Expected one of ' or \" but found '"+(char)q+"'");
      while( (b=get())!= q ) ;
    }
  }

  // -------------------------------------------------------------------------
  // -------------------------------------------------------------------------
  // Common PMML Operators
  public static enum Operators {
    lessOrEqual, lessThan, greaterOrEqual, greaterThan, equal, isMissing;
  }
  public static enum BooleanOperators {
    isNotIn, and, or, isIn;
  }

  public static abstract class Predicate {
    public abstract boolean match(Comparable value);
    public abstract boolean match(String sval, double dval);
    public abstract StringBuilder toJavaNum( StringBuilder sb, String jname );
    public StringBuilder toJavaBool( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    public StringBuilder toJavaStr( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    public static Predicate makeSimple(String field, Operators op, String cons) {
      if( cons==null ) {
        assert op==Operators.isMissing;
        return new IsMissing(field);
      }
      switch (op) {
      case lessOrEqual   : return new LessOrEqual   (field,cons);
      case lessThan      : return new LessThan      (field,cons);
      case greaterOrEqual: return new GreaterOrEqual(field,cons);
      case greaterThan   : return new GreaterThan   (field,cons);
      case equal         : return new Equals        (field,cons);
      default            : throw new RuntimeException("missing "+field+" "+op+" "+cons);
      }
    }
    public String unique_name() { throw H2O.unimpl(); }
  }

  public static abstract class Comparison extends Predicate {
    // Used to define comparisons like:
    //   "income < 10000" which _name==income, and _str=="10000", _num==10000
    public final String _name;// Feature name, e.g. "bad_email" or "income"
    public final String _str; // Constant compare value as a String
    public final double _num; // Constant compare value or NaN if not applicable
    public final double _bool;// Constant boolean value or NaN if not applicable
    public Comparison(String name, String str) {
      _name = name;
      _str = str;
      _num = getNumber (str);// Convert to a 'double'
      _bool= getBoolean(str);// Convert to a 'boolean'
    }
    public String unique_name() { return _name; }
  }

  /** Less or equal */
  public static class LessOrEqual extends Comparison {
    public LessOrEqual(String name, String str) { super(name,str); }
    @Override public boolean match(Comparable value) {
      if( !Double.isNaN(_num ) ) return getNumber (value) <= _num ;
      if( !Double.isNaN(_bool) ) return getBoolean(value) <= _bool;
      String s = getString(value);
      return s==null ? false : s.compareTo(_str) <= 0;
    }
    @Override public boolean match(String sval, double dval) { return dval <= _num; }
    @Override public String toString() { return "X<=" + _str; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      return sb.append(jname).append("<=").append(_num);
    }
  }

  public static class LessThan extends Comparison {
    public LessThan(String name, String str) { super(name,str); }
    @Override public boolean match(Comparable value) {
      if( !Double.isNaN(_num ) ) return getNumber (value) < _num ;
      if( !Double.isNaN(_bool) ) return getBoolean(value) < _bool;
      String s = getString(value);
      return s==null ? false : s.compareTo(_str) < 0;
    }
    @Override public boolean match(String sval, double dval) { return dval < _num; }
    @Override public String toString() { return "X<" + _str; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      return sb.append(jname).append("<").append(_num);
    }
  }

  public static class GreaterOrEqual extends Comparison {
    public GreaterOrEqual(String name, String con) { super(name,con); }
    @Override public boolean match(Comparable value) {
      if( !Double.isNaN(_num ) ) return getNumber (value) >= _num ;
      if( !Double.isNaN(_bool) ) return getBoolean(value) >= _bool;
      String s = getString(value);
      return s==null ? false : s.compareTo(_str) >= 0;
    }
    @Override public boolean match(String sval, double dval) { return dval >= _num; }
    @Override public String toString() { return "X>=" + _str; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      return sb.append(jname).append(">=").append(_num);
    }
  }

  public static class GreaterThan extends Comparison {
    public GreaterThan(String name, String str) { super(name,str); }
    @Override public boolean match(Comparable value) {
      if( !Double.isNaN(_num ) ) return getNumber (value) > _num ;
      if( !Double.isNaN(_bool) ) return getBoolean(value) > _bool;
      String s = getString(value);
      return s==null ? false : s.compareTo(_str) > 0;
    }
    @Override public boolean match(String sval, double dval) { return dval > _num; }
    @Override public String toString() { return "X>" + _str; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      return sb.append(jname).append(">").append(_num);
    }
  }

  public static class IsMissing extends Predicate {
    public final String _name;  // Feature name, like 'dependents'
    public IsMissing( String name ) { _name=name; }
    @Override public boolean match(Comparable value) { return value==null; }
    @Override public boolean match(String sval, double dval) { return Double.isNaN(dval); }
    @Override public String toString() { return "isMissing"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      return sb.append("Double.isNaN("+jname+")");
    }
    @Override public StringBuilder toJavaBool( StringBuilder sb, String jname ) {
      return sb.append("Double.isNaN("+jname+")");
    }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) {
      return sb.append(jname).append("==null");
    }
    public String unique_name() { return _name; }
  }

  public static class Equals extends Comparison {
    public Equals(String name, String str) { super(name,str); }
    @Override public boolean match(Comparable value) {
      if( !Double.isNaN(_num ) ) return getNumber (value) == _num ;
      if( !Double.isNaN(_bool) ) return getBoolean(value) == _bool;
      String s = getString(value);
      return s==null ? false : s.compareTo(_str) == 0;
    }
    @Override public boolean match(String sval, double dval) {
      if( !Double.isNaN(_num ) ) return dval == _num ;
      if( !Double.isNaN(_bool) ) return dval == _bool;
      return _str.equals(sval);
    }
    @Override public String toString() { return "X==" + _str; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      return sb.append(jname).append("==").append(_num);
    }
    @Override public StringBuilder toJavaBool( StringBuilder sb, String jname ) {
      return sb.append(jname).append("==").append(_bool);
    }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) {
      return sb.append("\"").append(_str).append("\".equals(").append(jname).append(")");
    }
  }

  public static abstract class CompoundPredicate extends Predicate {
    Predicate _l,_r;
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

    public static CompoundPredicate make(BooleanOperators op) {
      switch( op ) {
      case and: return new And();
      case or : return new Or();
      default : return null;
      }
    }
    public String unique_name() { return _l.unique_name(); }
  }

  public static class And extends CompoundPredicate {
    @Override public final boolean match(Comparable value) { return _l.match(value) && _r.match(value); }
    @Override public final boolean match(String sval, double dval) { return _l.match(sval,dval) && _r.match(sval,dval); }
    @Override public String toString() { return "(" + _l.toString() + " and " + _r.toString() + ")"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { return makeNum(sb,jname,"&&"); }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) { return makeStr(sb,jname,"&&"); }
  }
  public static class Or extends CompoundPredicate {
    @Override public final boolean match(Comparable value) { return _l.match(value) || _r.match(value); }
    @Override public final boolean match(String sval, double dval) { return _l.match(sval,dval) || _r.match(sval,dval); }
    @Override public String toString() { return "(" + _l.toString() + " or " + _r.toString() + ")"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { return makeNum(sb,jname,"||"); }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) { return makeStr(sb,jname,"||"); }
  }

  public static class IsIn extends Predicate {
    public final String _name;  // Feature name, like 'state'
    public String[] _values;
    public IsIn(String name, String[] values) { _name=name; _values = values; }
    @Override public boolean match(Comparable value) {
      for( String t : _values ) if (t.equals(value)) return true;
      return false;
    }
    @Override public boolean match(String sval, double dval) {
      for( String t : _values ) if (t.equals(sval)) return true;
      return false;
    }
    @Override public String toString() {
      String x = "";
      for( String s: _values ) x += s + " ";
      return "X is in {" + x + "}"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) {
      for( String s : _values )
        sb.append("\"").append(s).append("\".equals(").append(jname).append(") || ");
      return sb.append("false");
    }
    public static IsIn make(String name, BooleanOperators op) {
      switch( op ) {
      case isIn   : return new IsIn   (name,null);
      case isNotIn: return new IsNotIn(name,null);
      default     : return null;
      }
    }
    public String unique_name() { return _name; }
  }

  public static class IsNotIn extends IsIn {
    public IsNotIn(String name, String[] values) { super(name,values); }
    @Override public boolean match(Comparable value) { return ! super.match(value); }
    @Override public boolean match(String sval, double dval) { return ! super.match(sval,dval); }
    @Override public String toString() { return "!("+super.toString()+")"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) { throw H2O.unimpl(); }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) {
      sb.append("!(");
      super.toJavaStr(sb,jname);
      return sb.append(")");
    }
  }

  public static class True extends Predicate {
    @Override public boolean match(Comparable value) { return true; }
    @Override public boolean match(String sval, double dval) { return true; }
    @Override public String toString() { return "true"; }
    @Override public StringBuilder toJavaNum( StringBuilder sb, String jname ) {
      return sb.append("true");
    }
    @Override public StringBuilder toJavaBool( StringBuilder sb, String jname ) {
      return sb.append("true");
    }
    @Override public StringBuilder toJavaStr( StringBuilder sb, String jname ) {
      return sb.append("true");
    }
    @Override public String unique_name() { return ""; }
  }

  // Happy Helper Methods for the generated code
  public static double getNumber( HashMap<String,Comparable> row, String s ) {
    return getNumber(row.get(s));
  }
  public static double getNumber( Comparable o ) {
    // hint to the jit to do a instanceof breakdown tree
    if( o instanceof Double ) return ((Double)o).doubleValue();
    if( o instanceof Long   ) return ((Long  )o).doubleValue();
    if( o instanceof Number ) return ((Number)o).doubleValue();
    if( o instanceof String ) {
      try { return Double.valueOf((String)o); } catch( Throwable t ) { }
    }
    return Double.NaN;
  }
  public static double getBoolean( HashMap<String,Comparable> row, String s ) {
    return getBoolean(row.get(s));
  }
  public static double getBoolean( Comparable o ) {
    if( o instanceof Boolean ) return ((Boolean)o) ? 1.0 : 0.0;
    if( o instanceof String ) {
      try {
        if( "true" .equalsIgnoreCase((String) o) ) return 1.0;
        if( "false".equalsIgnoreCase((String) o) ) return 0.0;
      } catch( Throwable t ) { Log.err(t); }
    }
    return Double.NaN;
  }
  public static String getString( HashMap<String,Comparable> row, String s ) {
    return getString(row.get(s));
  }
  public static String getString( Comparable o ) {
    if( o instanceof String ) return (String)o;
    return o == null ? null : o.toString();
  }
}
