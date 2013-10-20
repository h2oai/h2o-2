package water.exec;

/** Typing system for a generic R-like parser.
 *  Supports Hindley-Milner style type inference.
 *  @author cliffc@0xdata.com
 */
// --------------------------------------------------------------------------
public class Type {
  final static private int UNBOUND= 0;
  final static private int BOUND  = 1; // to _ts[0]
  final static private int DBL0   = 2;
  final static private int ARY0   = 3;
  final static private int FCN0   = 4; // Return type in _ts[0], args in _ts[1...]
  final static private int VARARGS=32; // OR'd onto last type in a fcn, allows zero or more of this type
  int _t;                       // One of the above #s
  int _x;                       // Point in program where type is 1st defined.
  final Type[] _ts; // null==prim, else fcn and _ts[0] is return, _ts[1+...] are arg types
  Type( int x, int t, Type[] ts ) { assert varargs_clean(t,ts); _x=x; _t=t; _ts=ts; }
  Type( int x, int t, Type[] ts, float f ) { this(x,t,ts); _t|=VARARGS;}

  // Check no varargs flags, except on the last type of functions
  private boolean varargs_clean( int t, Type ts[] ) {
    if( (t&VARARGS)!=0 ) return false; // Need to clean this upfront
    if( t!=FCN0 || ts==null ) return true;
    for( int i=0; i<ts.length-1; i++ )
      if( ts[i] != null && (ts[i]._t&VARARGS)!=0 )
        return false;
    return true;
  }

  // Make some base types
  static Type DBL = new Type(0,DBL0,null);
  static Type ARY = new Type(0,ARY0,null);
  public static Type unbound(int x) { return new Type(x,UNBOUND,new Type[1]); }
  public static Type fcn(int x, Type[] ts) { return new Type(x,FCN0,ts); }
  public static Type varargs(Type t) { return new Type(t._x,t._t,t._ts,1f);}

  public Type ret( ) { Type t=find(); assert t._t == FCN0; return t._ts[0]; }

  // Tarjan Union-Find
  Type find() { return _t==BOUND ? (_ts[0]=_ts[0].find()) : this; }
  boolean union( Type t ) {
    Type ta=  find();
    Type tb=t.find();
    int tta = ta._t&(VARARGS-1); // Strip off varargs
    int ttb = tb._t&(VARARGS-1); // Strip off varargs
    if( ta==tb ) return true;
    else if( tta==FCN0 && ttb==FCN0 ) { // Functions are equal?
      // Structural breakdown of function-type equality.
      // Made more complex by allowing varargs types.
      Type t0 = ta, t1 = tb;    // Shorter type in t0
      if( ta._ts.length>tb._ts.length ) { t0=tb; t1=ta; }
      // Walk the shorter list, checking types
      for( int i=0; i<t0._ts.length; i++ )
        if( !t0._ts[i].union(t1._ts[i]) )
          return false;         // Subtypes are unequal
      if( t0._ts.length == t1._ts.length ) return true;
      // Extra args in T1 can only be matched with a varargs repeat from T0
      Type varargs = t0._ts[t0._ts.length-1];
      if( (varargs._t&VARARGS)==0 ) return false;
      for( int i=t0._ts.length; i<t1._ts.length; i++ )
        if( !varargs.union(t1._ts[i]) )
          return false;         // Subtypes are unequal
    } 
    else if( tta==UNBOUND ) { ta._t=BOUND; ta._ts[0]= tb; }
    else if( ttb==UNBOUND ) { tb._t=BOUND; tb._ts[0]= ta; }
    else if( tta == ttb ) return true; // Equal after varargs stripping
    else return false;          // Types are unequal
    return true;
  }

  // If clearly not a function.  False for unbound variables, which might
  // become "not a function" later.
  boolean isNotFun() { Type t = find();  return t._t==DBL0 || t._t==ARY0; }

  @Override public String toString() {
    String s=null;
    switch( _t&(VARARGS-1) ) {
    case UNBOUND: s = "@"+_x;  break;
    case BOUND:   s = _ts[0].toString(); break;
    case DBL0:    s = "dbl";   break;
    case ARY0:    s = "ary";   break;
    case FCN0: {
      s = _ts[0]+"(";
      for( int i=1; i<_ts.length-1; i++ )
        s += _ts[i]+",";
      if( _ts.length > 1 ) s += _ts[_ts.length-1];
      s += ")";
      break;
    }
    default: throw water.H2O.unimpl();
    }
    if( (_t&VARARGS)!=0 ) s += "...";
    return s;
  }
}
