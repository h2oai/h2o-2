package water.exec;

import java.text.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import water.*;
import water.fvec.*;
import water.fvec.Vec.VectorGroup;
import water.util.Log;

/** Execute a R-like AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */
public class Env extends Iced {
  // An environment is a classic stack of values, passed into AST's as the
  // execution state.  The 3 types we support are Frames (2-d tables of data),
  // doubles (which are an optimized form of a 1x1 Frame), and ASTs (which are
  // 1st class functions).
  String _key[] = new String[4]; // For top-level globals only, record a frame Key
  Frame  _ary[] = new Frame [4]; // Frame (or null if not a frame)
  double _d  [] = new double[4]; // Double (only if frame & func are null)
  ASTOp  _fcn[] = new ASTOp [4]; // Functions (or null if not a function)
  int    _sp;                    // Stack pointer
  // Also a Pascal-style display, one display entry per lexical scope.  Slot
  // zero is the start of the global scope (which contains all global vars like
  // hex Keys) and always starts at offset 0.
  int _display[] = new int[4];
  int _tod;

  // Ref Counts for each vector
  transient final HashMap<Vec,Integer> _refcnt;

  transient final public StringBuilder _sb; // Holder for print results

  transient boolean _allow_tmp;           // Deep-copy allowed to tmp
  transient boolean _busy_tmp;            // Assert temp is available for use
  transient Frame  _tmp;                  // The One Big Active Tmp

  Env() {
    _key = new String[4]; // Key for Frame
    _ary = new Frame [4]; // Frame (or null if not a frame)
    _d   = new double[4]; // Double (only if frame & func are null)
    _fcn = new ASTOp [4]; // Functions (or null if not a function)
    _display= new int[4];
    _refcnt = new HashMap<Vec,Integer>();
    _sb = new StringBuilder();
  }

  public int sp() { return _sp; }
  public boolean isAry() { return _ary[_sp-1] != null; }
  public boolean isFcn  () { return _fcn[_sp-1] != null; }
  public boolean isDbl  () { return !isAry() && !isFcn(); }
  public boolean isFcn  (int i) { return _fcn[_sp+i] != null; }
  public boolean isAry(int i) { return _ary[_sp+i] != null; }
  // Peek operators
  public Frame  ary(int i) { Frame fr = _ary[_sp+i]; assert fr != null; return fr; }
  public ASTOp  fcn(int i) { ASTOp op = _fcn[_sp+i]; assert op != null; return op; }
  public double dbl(int i) { double d = _d  [_sp+i]; return d; }

  // Load the nth Id/variable from the named lexical scope, typed as a Frame
  public Frame frId(int d, int n) {
    int idx = _display[_tod-d]+n;
    assert _ary[idx]!=null;
    return _ary[idx];
  }

  // Push k empty slots
  void push( int slots ) {
    assert 0 <= slots && slots < 1000;
    int len = _d.length;
    _sp += slots;
    while( _sp > len ) {
      _key= Arrays.copyOf(_key,len<<1);
      _ary= Arrays.copyOf(_ary,len<<1);
      _d  = Arrays.copyOf(_d  ,len<<1);
      _fcn= Arrays.copyOf(_fcn,len<<=1);
    }
  }
  void push( Frame fr ) { push(1); _ary[_sp-1] = addRef(fr); assert _ary[0]==null||check_refcnt(_ary[0].anyVec());}
  void push( double d ) { push(1); _d  [_sp-1] = d  ; }
  void push( ASTOp fcn) { push(1); _fcn[_sp-1] = addRef(fcn); }
  void push( Frame fr, String key ) { push(fr); _key[_sp-1]=key; }

  // Copy from display offset d, nth slot
  void push_slot( int d, int n ) {
    assert d==0;                // Should use a fcn's closure for d>1
    int idx = _display[_tod-d]+n;
    push(1);
    _ary[_sp-1] = addRef(_ary[idx]);
    _d  [_sp-1] =        _d  [idx];
    _fcn[_sp-1] = addRef(_fcn[idx]);
    assert _ary[0]==null || check_refcnt(_ary[0].anyVec());
  }
  void push_slot( int d, int n, Env global ) {
    assert _refcnt==null;       // Should use a fcn's closure for d>1
    int idx = _display[_tod-d]+n;
    int gidx = global._sp;
    global.push(1);
    global._ary[gidx] = global.addRef(_ary[idx]);
    global._d  [gidx] =               _d  [idx] ;
    global._fcn[gidx] = global.addRef(_fcn[idx]);
    assert _ary[0]==null || global.check_refcnt(_ary[0].anyVec());
  }
  // Copy from TOS into a slot.  Does NOT pop results.
  void tos_into_slot( int d, int n, String id ) {
    // In a copy-on-modify language, only update the local scope, or return val
    assert d==0 || (d==1 && _display[_tod]==n+1);
    int idx = _display[_tod-d]+n;
    subRef(_ary[idx], _key[idx]);
    subRef(_fcn[idx]);
    Frame fr =                   _ary[_sp-1];
    _ary[idx] = fr==null ? null : addRef(new Frame(fr));
    _d  [idx] =                  _d  [_sp-1] ;
    _fcn[idx] =           addRef(_fcn[_sp-1]);
    _key[idx] = d==0 && fr!=null ? id : null;
    assert _ary[0]== null || check_refcnt(_ary[0].anyVec());
  }
  // Copy from TOS into stack.  Pop's all intermediate.
  // Example: pop_into_stk(-4)  BEFORE: A,B,C,D,TOS  AFTER: A,TOS
  void pop_into_stk( int x ) {
    assert x < 0;
    subRef(_ary[_sp+x], _key[_sp+x]);  // Nuke out old stuff
    subRef(_fcn[_sp+x]);
    _ary[_sp+x] = _ary[_sp-1];  // Copy without changing ref cnt
    _fcn[_sp+x] = _fcn[_sp-1];
    _d  [_sp+x] = _d  [_sp-1];
    _sp--;  x++;                // Pop without changing ref cnt
    while( x++ < -1 ) pop();
  }

  // Push a scope, leaving room for passed args
  int pushScope(int args) {
    assert fcn(-args-1) instanceof ASTFunc; // Expect a function under the args
    return _display[++_tod] = _sp-args;
  }
  // Grab the function for nested scope d
  ASTFunc fcnScope( int d ) { return (ASTFunc)_fcn[_display[_tod]-1]; }

  // Pop a slot.  Lowers refcnts on vectors.  Always leaves stack null behind
  // (to avoid dangling pointers stretching lifetimes).
  void pop( Env global ) {
    assert _sp > _display[_tod]; // Do not over-pop current scope
    _sp--;
    _fcn[_sp]=global.subRef(_fcn[_sp]);
    _ary[_sp]=global.subRef(_ary[_sp],_key[_sp]);
    assert _sp==0 || _ary[0]==null || check_refcnt(_ary[0].anyVec());
  }
  public void popUncheck( ) {
    _sp--;
    _fcn[_sp]=subRef(_fcn[_sp]);
    _ary[_sp]=subRef(_ary[_sp],_key[_sp]);
  }
  public void pop( ) { pop(this); }
  public void pop( int n ) {
    for( int i=0; i<n; i++ )
      pop();
  }

  void popScope() {
    assert _tod > 0;            // Something to pop?
    assert _sp >= _display[_tod]; // Did not over-pop already?
    while( _sp > _display[_tod] ) pop();
    _tod--;
  }

  // Pop & return a Frame or Fcn; ref-cnt of all things remains unchanged.
  // Caller is responsible for tracking lifetime.
  public double popDbl()  { assert isDbl(); return _d  [--_sp]; }
  public ASTOp  popFcn()  { assert isFcn(); ASTOp op = _fcn[--_sp]; _fcn[_sp]=null; return op; }
  public Frame  popAry()  { assert isAry(); Frame fr = _ary[--_sp]; _ary[_sp]=null; assert allAlive(fr); return fr; }
  public Frame  peekAry() { assert isAry(); Frame fr = _ary[_sp-1]; assert allAlive(fr); return fr; }
  public ASTOp  peekFcn() { assert isFcn(); ASTOp op = _fcn[_sp-1]; return op; }
  public String peekKey() { return _key[_sp-1]; }
  public String key()     { return _key[_sp]; }

  // Replace a function invocation with it's result
  public void poppush( int n, Frame ary, String key) {
    addRef(ary);
    for( int i=0; i<n; i++ ) {
      assert _sp > 0;
      _sp--;
      _fcn[_sp] = subRef(_fcn[_sp]);
      _ary[_sp] = subRef(_ary[_sp], _key[_sp]);
    }
    push(1); _ary[_sp-1] = ary; _key[_sp-1] = key;
    assert check_all_refcnts();
  }
  // Replace a function invocation with it's result
  public void poppush(double d) { pop(); push(d); }

  // Capture the current environment & return it (for some closure's future execution).
  Env capture( ) { return new Env(this); }
  private Env( Env e ) {
    _sp = e._sp;
    _key= Arrays.copyOf(e._key,_sp);
    _ary= Arrays.copyOf(e._ary,_sp);
    _d  = Arrays.copyOf(e._d  ,_sp);
    _fcn= Arrays.copyOf(e._fcn,_sp);
    _tod= e._tod;
    _display = Arrays.copyOf(e._display,_tod+1);
    // All other fields are ignored/zero
    _refcnt = null;
    _sb = null;
  }


  // Nice assert
  boolean allAlive(Frame fr) {
    for( Vec vec : fr.vecs() )
      assert _refcnt.get(vec) > 0;
    return true;
  }

  public Futures subRef( Vec vec, Futures fs ) {

    if ( vec.masterVec() != null ) subRef(vec.masterVec(), fs);
    int cnt = _refcnt.get(vec)-1;
    //Log.info(" --- " + vec._key.toString()+ " RC=" + cnt);
    if( cnt > 0 ) _refcnt.put(vec,cnt);
    else {
      if( fs == null ) fs = new Futures();
      UKV.remove(vec._key,fs);
      _refcnt.remove(vec);
    }
    return fs;
  }

  // Lower the refcnt on all vecs in this frame.
  // Immediately free all vecs with zero count.
  // Always return a null.
  public Frame subRef( Frame fr, String key ) {
    if( fr == null ) return null;
    Futures fs = null;
    for( Vec vec : fr.vecs() ) fs = subRef(vec,fs);
    if( key != null && fs != null ) UKV.remove(Key.make(key),fs);
    if( fs != null )
      fs.blockForPending();
    return null;
  }
  // Lower refcounts on all vecs captured in the inner environment
  public ASTOp subRef( ASTOp op ) {
    if( op == null ) return null;
    if( !(op instanceof ASTFunc) ) return null;
    ASTFunc fcn = (ASTFunc)op;
    if( fcn._env != null ) fcn._env.subRef(this);
    else System.out.println("Popping fcn object, never executed no environ capture");
    return null;
  }

  Vec addRef( Vec vec ) {
    Integer I = _refcnt.get(vec);
    assert I==null || I>0;
    assert vec.length() == 0 || (vec.at(0) > 0 || vec.at(0) <= 0 || Double.isNaN(vec.at(0)));
    _refcnt.put(vec,I==null?1:I+1);
    //Log.info(" +++ " + vec._key.toString() + " RC=" + (I==null?1:I+1));
    //if (I!=null&&I==1)
    //  for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
    //    System.out.println(ste);
    //  }
    if (vec.masterVec()!=null) addRef(vec.masterVec());
    return vec;
  }
  // Add a refcnt to all vecs in this frame
  Frame addRef( Frame fr ) {
    if( fr == null ) return null;
    for( Vec vec : fr.vecs() ) addRef(vec);
    return fr;
  }
  ASTOp addRef( ASTOp op ) {
    if( op == null ) return null;
    if( !(op instanceof ASTFunc) ) return op;
    ASTFunc fcn = (ASTFunc)op;
    if( fcn._env != null ) fcn._env.addRef(this);
    else System.out.println("Pushing fcn object, never executed no environ capture");
    return op;
  }
  private void addRef(Env global) {
    for( int i=0; i<_sp; i++ ) {
      if( _ary[i] != null ) global.addRef(_ary[i]);
      if( _fcn[i] != null ) global.addRef(_fcn[i]);
    }
  }
  private void subRef(Env global) {
    for( int i=0; i<_sp; i++ ) {
      if( _ary[i] != null ) global.subRef(_ary[i],_key[i]);
      if( _fcn[i] != null ) global.subRef(_fcn[i]);
    }
  }


  // Remove everything
  public void remove() {
    // Remove all shallow scopes
    while( _tod > 0 ) popScope();
    // Push changes at the outer scope into the K/V store
    while( _sp > 0 ) {
      if( isAry() && _key[_sp-1] != null ) { // Has a K/V mapping?
        Frame fr = popAry();    // Pop w/o lowering refcnt
        Frame fr2=fr;
        String skey = key();
        for( int i=0; i<fr.numCols(); i++ ) {
          Vec v = fr.vecs()[i];
          int refcnt = _refcnt.get(v);
          assert refcnt > 0;
          if( refcnt > 1 ) {    // Need a deep-copy now
            if( fr2==fr ) fr2 = new Frame(fr);
            Vec v2 = new Frame(v).deepSlice(null,null).vecs()[0];
            fr2.replace(i,v2);  // Replace with private deep-copy
            subRef(v,null);     // Now lower refcnt for good assertions
            addRef(v2);
          } // But not down to zero (do not delete items in global scope)
        }
        UKV.put(Key.make(_key[_sp]),fr2);
      } else
        popUncheck();
    }
  }

  // Done writing into all things.  Allow rollups.
  public void postWrite() {
    for( Vec vec : _refcnt.keySet() )
      vec.postWrite();
  }

  // Count references the "hard way" - used to check refcnting math.
  int compute_refcnt( Vec vec ) {
    int cnt=0;
    HashSet<Vec> refs = new HashSet<Vec>();
    for( int i=0; i<_sp; i++ )
      if( _ary[i] != null) {
        for (Vec v : _ary[i].vecs()) {
          Vec vm;
          if (v.equals(vec)) cnt++;
          else if ((vm = v.masterVec()) !=null && vm.equals(vec)) cnt++;
        }
      }
      else if( _fcn[i] != null && (_fcn[i] instanceof ASTFunc) )
        cnt += ((ASTFunc)_fcn[i])._env.compute_refcnt(vec);
    return cnt + refs.size();
  }
  boolean check_refcnt( Vec vec ) {
    Integer I = _refcnt.get(vec);
    int cnt0 = I==null ? 0 : I;
    int cnt1 = compute_refcnt(vec);
    if( cnt0==cnt1 ) return true;
    System.out.println("Refcnt is "+cnt0+" but computed as "+cnt1);
    return false;
  }

  boolean check_all_refcnts() {
    for (Vec v : _refcnt.keySet())
      if (check_refcnt(v) == false)
        return false;
    return true;
  }

  // Pop and return the result as a string
  public String resultString( ) {
    assert _tod==0 : "Still have lexical scopes past the global";
    String s = toString(_sp-1,true);
    pop();
    return s;
  }

  public String toString(int i, boolean verbose_fcn) {
    if( _ary[i] != null ) return _ary[i].numRows()+"x"+_ary[i].numCols();
    else if( _fcn[i] != null ) return _fcn[i].toString(verbose_fcn);
    return Double.toString(_d[i]);
  }
  @Override public String toString() {
    String s="{";
    for( int i=0; i<_sp; i++ )   s += toString(i,false)+",";
    return s+"}";
  }
}
