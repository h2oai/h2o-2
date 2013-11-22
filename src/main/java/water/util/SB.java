package water.util;

// Tight/tiny StringBuilder wrapper.
// Short short names on purpose; so they don't obscure the printing.
// Can't believe this wasn't done long long ago.
public class SB {
  public final StringBuilder _sb;
  private int _indent = 0;
  public SB(        ) { _sb = new StringBuilder( ); }
  public SB(String s) { _sb = new StringBuilder(s); }
  public SB p( String s ) { _sb.append(s); return this; }
  public SB p( float  s ) { _sb.append(s); return this; }
  public SB p( char   s ) { _sb.append(s); return this; }
  public SB p( int    s ) { _sb.append(s); return this; }
  public SB p( Object s ) { _sb.append(s.toString()); return this; }
  public SB indent( int d ) { for( int i=0; i<d+_indent; i++ ) p("  "); return this; }
  public SB indent( ) { return indent(0); }
  // Java specif append of float
  public SB pj( float  s ) { _sb.append(s).append('f'); return this; }
  // Increase indentation
  public SB ii( int i) { _indent += i; return this; }
  // Decrease indentation
  public SB di( int i) { _indent -= i; return this; }
  public SB nl( ) { return p('\n'); }
  // Convert a String[] into a valid Java String initializer
  public SB toJavaStringInit( String[] ss ) {
    p('{');
    for( int i=0; i<ss.length-1; i++ )  p('"').p(ss[i]).p("\",");
    if( ss.length > 0 ) p('"').p(ss[ss.length-1]).p('"');
    return p('}');
  }
  public SB toJSArray(float[] nums) {
    p('[');
    for (int i=0; i<nums.length; i++) {
      if (i>0) p(',');
      p(nums[i]);
    }
    return p(']');
  }
  public SB toJSArray(String[] ss) {
    p('[');
    for (int i=0; i<ss.length; i++) {
      if (i>0) p(',');
      p('"').p(ss[i]).p('"');
    }
    return p(']');
  }

  // Mostly a fail, since we should just dump into the same SB.
  public SB p( SB sb ) { _sb.append(sb._sb); return this;  }
  @Override public String toString() { return _sb.toString(); }
}
