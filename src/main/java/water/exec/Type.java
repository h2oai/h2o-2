package water.exec;

/** Typing system for a generic R-like parser.
 *  Supports Hindley-Milner style type inference.
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
public class Type {
  final int _x;                 // Point in program where type is 1st defined.
        int _t;                 // 0==unbound, 1=dbl, 2=ary, 3=fcn, 4=bound to _ts[0]
  final Type[] _ts; // null==prim, else fcn and _ts[0] is return, _ts[1+...] are arg types
  Type( int x, int t, Type[] ts ) { _x=x; _t=t; _ts=ts; }
  static Type DBL = new Type(0,1,null);
  static Type ARY = new Type(0,2,null);
  public static Type unbound(int x) { return new Type(x,0,new Type[1]); }
  public static Type fcn(int x, Type[] ts) { return new Type(x,3,ts); }

  // Tarjan Union-Find
  Type find() { return _t==4 ? (_ts[0]=_ts[0].find()) : this; }
  boolean union( Type t ) {
    Type ta=  find();
    Type tb=t.find();
    if( ta==tb ) return true;
    else if( ta._t==3 && tb._t==3 && ta._ts.length==tb._ts.length ) {
      for( int i=0; i<ta._ts.length; i++ )
        if( !ta._ts[i].union(tb._ts[i]) )
          return false;         // Subtypes are unequal
    } 
    else if( ta._t==0 ) { ta._t=4; ta._ts[0]= tb; }
    else if( tb._t==0 ) { tb._t=4; tb._ts[0]= ta; }
    else return false;          // Types are unequal
    return true;
  }

  // If clearly not a function.  False for unbound variables, which might
  // become "not a function" later.
  boolean isNotFun() { Type t = find();  return t._t==1 || t._t==2; }

  @Override public String toString() {
    switch( _t ) {
    case 0: return "@"+_x;
    case 1: return "dbl";
    case 2: return "ary";
    case 3: {
      String s = _ts[0]+"(";
      for( int i=1; i<_ts.length-1; i++ )
        s += _ts[i]+",";
      if( _ts.length > 1 ) s += _ts[_ts.length-1];
      return s+")";
    }
    case 4: return _ts[0].toString();
    }
    throw water.H2O.fail();
  }
}
