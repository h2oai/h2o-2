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
  final static private int FCN0   = 4; // Return type in _ts[0], args in _ts[1...];
  final static private int DBLARY0= 5; // Type is either DBL or ARY but not FCN
  final static private int ANYARY0= 6; // Type is ARY if any ts[] is an ARY, else DBL
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
  public static Type dblary() { return new Type(0,DBLARY0,new Type[1]); }
  public static Type anyary(Type ts[]) { return new Type(0,ANYARY0,ts); }

  // Tarjan Union-Find
  Type find() {
    Type t = this;
    if( _t==BOUND ) t=_ts[0]=_ts[0].find();
    if( t._t!=ANYARY0 ) return t;
    return t.findAnyAry();
  }
  // Sort out 4 options: 
  // If any ary, then ary
  // If exactly 1 unbound, then return that guy
  // If 2+ unbound, then might be ary someday so return self
  // If all bound to dbl, then dbl
  private Type findAnyAry() {
    Type unb=null;
    int unbound = 0;
    for( int i=0; i<_ts.length; i++ ) {
      Type t2 = _ts[i] = _ts[i].find();
      if( t2._t==ARY0 ) { _t=BOUND; return (_ts[0] = ARY); }
      if( t2._t==UNBOUND || t2._t==DBLARY0 ) { unb=t2; unbound++; }
      else if( t2._t!=DBL0 ) return this;
    }
    if( unbound==0 ) { _t=BOUND; return (_ts[0] = DBL); }
    if( unbound==1 ) { _t=BOUND; return (_ts[0] = unb); }
    return this;
  }
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
      boolean ok=true;
      for( int i=0; i<t0._ts.length; i++ )
        if( !t0._ts[i].union(t1._ts[i]) )
          ok = false;           // Subtypes are unequal
      if( t0._ts.length == t1._ts.length ) return ok;
      // Extra args in T1 can only be matched with a varargs repeat from T0
      Type varargs = t0._ts[t0._ts.length-1];
      if( (varargs._t&VARARGS)==0 ) return false;
      for( int i=t0._ts.length; i<t1._ts.length; i++ )
        if( !varargs.union(t1._ts[i]) )
          ok = false;         // Subtypes are unequal
      return ok;
    } 
    else if( tta==UNBOUND || (tta==DBLARY0 && tb.isDblAry()) ) { ta._t=BOUND; ta._ts[0]= tb; }
    else if( ttb==UNBOUND || (ttb==DBLARY0 && ta.isDblAry()) ) { tb._t=BOUND; tb._ts[0]= ta; }
    else if( tta==DBLARY0 && ttb==DBLARY0 ) { ta._t=BOUND; ta._ts[0]=tb; }
    else if( tta==ANYARY0 && ttb==ANYARY0 ) throw water.H2O.unimpl(); // Combine lists
    else if( tta==ANYARY0 && ttb==DBLARY0 ) throw water.H2O.unimpl(); // ???
    else if( tta==ANYARY0 && ttb==ARY0 )    throw water.H2O.unimpl(); // ?one of many must be an array?
    else if( tta==ANYARY0 && ttb==DBL0 )    { // Force all to DBL
      boolean ok=true;
      for( Type t2 : ta._ts ) ok |= !Type.DBL.union(t2);
      return ok;
    }
    else if( ttb==ANYARY0 ) throw water.H2O.unimpl();
    else if( tta==ttb ) return true; // Equal after varargs stripping
    else return false;          // Types are unequal
    return true;
  }

  // If clearly not a function.  False for unbound variables, which might
  // become "not a function" later.
  boolean isUnbound(){ Type t=find(); return t._t==UNBOUND; }
  boolean isAry()    { Type t=find(); return t._t==ARY0; }
  boolean isDbl()    { Type t=find(); return t._t==DBL0; }
  boolean isFcn()    { Type t=find(); return t._t==FCN0; }
  boolean isNotFun() { Type t=find(); return t._t==DBL0 || t._t==ARY0; }
  boolean isDblAry() { Type t=find(); return t._t==DBL0 || t._t==ARY0; }
  // Return type of functions
  public Type ret()  { Type t=find(); assert t._t == FCN0; return t._ts[0].find(); }


  @Override public String toString() {
    String s=null;
    switch( _t&(VARARGS-1) ) {
    case UNBOUND: s = "@"+_x;   break;
    case BOUND:   s = _ts[0].toString(); break;
    case DBL0:    s = "dbl";    break;
    case ARY0:    s = "ary";    break;
    case DBLARY0: s = "dblary"; break;
    case ANYARY0: {
      s = "anyary{"; 
      for( Type t : _ts ) s += t+",";
      s += "}";
      break;
    }
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
