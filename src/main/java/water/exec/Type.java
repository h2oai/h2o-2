package water.exec;

import java.util.Arrays;
import water.Iced;

/** Typing system for a generic R-like parser.
 *  Supports Hindley-Milner style type inference.
 *  @author cliffc@0xdata.com
 */
// --------------------------------------------------------------------------
public class Type extends Iced {
  final static private int UNBOUND= 0;
  final static private int BOUND  = 1; // to _ts[0]
  final static private int DBL0   = 2;
  final static private int ARY0   = 3;
  final static private int FCN0   = 4; // Return type in _ts[0], args in _ts[1...];
  final static private int DBLARY0= 5; // Type is either DBL or ARY but not FCN
  final static private int ANYARY0= 6; // Type is ARY if any ts[] is an ARY, else DBL
  final static private int STR0   = 7;
  final static private int VARARGS=32; // OR'd onto last type in a fcn, allows zero or more of this type
  int _t;                       // One of the above #s
  static private int UNIQUE;    // Unique ID handy for debugging
  final int _x = UNIQUE++;      // Point in program where type is 1st defined.
  Type[] _ts; // null==prim, else fcn and _ts[0] is return, _ts[1+...] are arg types
  Type( int t, Type[] ts ) { assert varargs_clean(t,ts); _t=t; _ts=ts; }
  Type( int t, Type[] ts, float f ) { this(t,ts); _t|=VARARGS;}

  Type copy() {
    Type[] ts = null;
    if (_ts!=null) {
      ts =_ts.clone(); for (int i = 0; i < ts.length; i++)
        if (_ts[i]!=null) ts[i] = _ts[i].copy();
    }
    int vararg = _t&VARARGS;
    Type copy = new Type(_t&~VARARGS,ts);
    copy._t |= vararg;
    return copy;
  }

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
  static Type DBL = new Type(DBL0,null);
  static Type ARY = new Type(ARY0,null);
  static Type STR = new Type(STR0, null);
  public static Type unbound() { return new Type(UNBOUND,new Type[1]); }
  public static Type fcn(Type[] ts) { return new Type(FCN0,ts); }
  public static Type varargs(Type t) { return new Type(t._t,t._ts,1f);}
  public static Type dblary() { return new Type(DBLARY0,new Type[1]); }
  public static Type anyary(Type ts[]) { return new Type(ANYARY0,ts); }

  // Tarjan Union-Find
  Type find() {
    Type t = this;
    if( _t==BOUND ) t=_ts[0]=_ts[0].find();
    if( t._t!=ANYARY0 ) return t;
    return t.findAnyAry();
  }

  // "anyary" was my 1st attempt at a Union-Type.  It's not going to work so
  // easily.  Need back-ptrs from the component types to the different
  // union-type flavors.  Then when union'ing a component, I can visit types
  // constructed from the component & union them also as needed.  For IfElse, I
  // need the True & False types, the Test type and the Result type.  These
  // combo's are legal, and all others illegal:
  // rez  tst  T  F
  //  D    D   D  D
  //  A    A   D  A
  //  A    A   A  D
  //  A    A   A  A
  //  A    D   A  A
  //  F    D   F  F   // and all Fcns are union'd
  //
  //  DA   DA  D  DA  // a single Dbl is not constraining
  //  DA   DA  DA D
  //  DA   D   DA DA
  //   A   DA   A  A
  //   A   DA   A DA  // Any array means the result is ary
  //   A   DA  DA  A
  //   A    A  DA DA  // weird: at least one of DA must be an A
  //   A   DA  DA DA  // 
  //  DA   DA  DA DA  // no functions
  //
  //   U   D    U  U  // could be all Fcns or any other mix
  //   U   DA   U  U  // Most general allowed type for IfElse


  // Drop DBL's, drop dups
  // If any are ARY, can only be ARY or fail
  // If FCNs, all must be equal
  // Return any singular type.
  private Type findAnyAry() {
    int len=0;
    Type fun=null;
    for( int i=0; i<_ts.length; i++ ) {
      Type t = _ts[i].find();
      if( t._t == FCN0 && fun != null ) { 
        t.union(fun); t=fun=t.find();
      } else {
        if( t._t == FCN0 ) fun = t;
        if( t._t != DBL0 && t._t != STR0 &&  // Keep non-DBL & non-STR
            !dupType(len,t) )   // But remove dups
          _ts[len++] = t;
      }
    }
    // No more types?  Defaults to DBL
    if( len == 0 ) { _t=BOUND; return (_ts[0] = DBL); }
    // Single variant type?  Defaults to that type
    if( len == 1 ) { _t=BOUND; return  _ts[0]; }
    if( len < _ts.length ) _ts = Arrays.copyOf(_ts, len);
    return this;
  }
  private boolean dupType( int len, Type t ) {
    for( int j=0; j<len; j++ ) if( _ts[j]==t ) return true;
    return false;
  }

  boolean union( Type t ) {
    Type ta=  find();
    Type tb=t.find();
    int tta = ta._t&(VARARGS-1); // Strip off varargs
    int ttb = tb._t&(VARARGS-1); // Strip off varargs
    if( ta==tb ) return true;
    else if( (tta==   FCN0 && ttb==   FCN0) ||  // Functions are equal?
             (tta==ANYARY0 && ttb==ANYARY0) ) { // AnyArys   are equal?
      // Structural breakdown of function-type equality.
      // Made more complex by allowing varargs types.
      Type t0 = ta, t1 = tb;    // Shorter type in t0
      if( ta._ts.length>tb._ts.length ) { t0=tb; t1=ta; }
      // Walk the shorter list, checking types
      boolean ok=true;
      int len=t0._ts.length;
      Type varargs=null;
      // Extra args in T1 can only be matched with a varargs repeat from T0
      if( len < t1._ts.length ) {
        varargs = t0._ts[len-1].find();
        if( (varargs._t&VARARGS)!=0 )
          len--;                // Dont match the varargs arg in 1st loop
        else varargs=null;      // Else not a varargs
      }
      for( int i=0; i<len; i++ ) // Match all args
        if( !t0._ts[i].union(t1._ts[i]) )
          ok = false;           // Subtypes are unequal
      if( len == t1._ts.length ) return ok;
      if( len == t1._ts.length-1 && (t1._ts[len].find()._t&VARARGS) != 0 )
        return true;  // Also ok for a zero-length varargs in t1, and no arg in t0
      if( varargs==null ) return false;
      // Must be varargs: 
      for( int i=len; i<t1._ts.length; i++ ) {
        int tvar = (varargs._t&(VARARGS-1));
        Type var = tvar==DBLARY0 ? dblary() : (tvar==UNBOUND ? unbound() : varargs); // Use a new unbound type
        if( !var.union(t1._ts[i]) )
          ok = false;         // Subtypes are unequal
      }
      return ok;
    } 
    else if( tta==UNBOUND || (tta==DBLARY0 && tb.isDblAry()) ) { ta._t=BOUND; ta._ts[0]= tb; }
    else if( ttb==UNBOUND || (ttb==DBLARY0 && ta.isDblAry()) ) { tb._t=BOUND; tb._ts[0]= ta; }
    else if( tta==DBLARY0 && ttb==DBLARY0 ) { ta._t=BOUND; ta._ts[0]=tb; }
    else if( tta==ANYARY0 && ttb==DBLARY0 ) throw water.H2O.unimpl(); // ???
    else if( tta==ANYARY0 && ttb==ARY0 )    throw water.H2O.unimpl(); // ?one of many must be an array?
    else if( tta==ANYARY0 && ttb==DBL0 ) { // Force all to DBL
      boolean ok=true;
      for( Type t2 : ta._ts ) ok |= !Type.DBL.union(t2);
      return ok;
    } else if( ttb==ANYARY0 ) throw water.H2O.unimpl();
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
  boolean isNotFun() { Type t=find(); return t._t==DBL0 || t._t==ARY0 || t._t==DBLARY0 || t._t==STR0; }
  boolean isDblAry() { Type t=find(); return t._t==DBL0 || t._t==ARY0; }
  boolean isStr()    { Type t=find(); return t._t==STR0; }
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
    case STR0:    s = "str";    break;
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
