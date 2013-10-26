package water.exec;

import java.text.*;
import java.util.Arrays;
import java.util.HashMap;
import water.*;
import water.fvec.*;
import water.fvec.Vec.VectorGroup;

/** Execute a R-like AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */
public class Env extends Iced {
  // An environment is a classic stack of values, passed into AST's as the
  // execution state.  The 3 types we support are Frames (2-d tables of data),
  // doubles (which are an optimized form of a 1x1 Frame), and ASTs (which are
  // 1st class functions).
  String _key[] = new String[4]; // For top-level globals only, record a frame Key
  Frame  _fr [] = new Frame [4]; // Frame (or null if not a frame)
  double _d  [] = new double[4]; // Double (only if frame & func are null)
  ASTOp  _fun[] = new ASTOp [4]; // Functions (or null if not a function)
  int    _sp;                    // Stack pointer
  VectorGroup _vg;
  Frame _currentMasterFrame;
  // Also a Pascal-style display, one display entry per lexical scope.  Slot
  // zero is the start of the global scope (which contains all global vars like
  // hex Keys) and always starts at offset 0.
  int _display[] = new int[4];
  int _tod;

  // Ref Counts for each vector
  transient final HashMap<Vec,Integer> _refcnt;

  transient boolean _allow_tmp;           // Deep-copy allowed to tmp
  transient boolean _busy_tmp;            // Assert temp is available for use
  transient Frame  _tmp;                  // The One Big Active Tmp

  Env() {
    _key = new String[4]; // Key for Frame
    _fr  = new Frame [4]; // Frame (or null if not a frame)
    _d   = new double[4]; // Double (only if frame & func are null)
    _fun = new ASTOp [4]; // Functions (or null if not a function)
    _display= new int[4];
    _refcnt = new HashMap<Vec,Integer>();
  }

  public int sp() { return _sp; }
  public boolean isFrame() { return _fr [_sp-1] != null; }
  public boolean isFun  () { return _fun[_sp-1] != null; }
  public boolean isDbl  () { return !isFrame() && !isFun(); }
  public boolean isFun  (int i) { return _fun[_sp+i] != null; }
  public ASTOp  fun(int i) { ASTOp op = _fun[_sp+i]; assert op != null; return op; }
  public double dbl(int i) { double d = _d  [_sp+i]; return d; }
  public Frame frId(int d, int n) {
    int idx = _display[_tod-d]+n;
    assert _fr[idx]!=null;
    return _fr[idx];
  }

  // Push k empty slots
  void push( int slots ) {
    assert 0 <= slots && slots < 1000;
    int len = _d.length;
    _sp += slots;
    while( _sp > len ) {
      _key= Arrays.copyOf(_key,len<<1);
      _fr = Arrays.copyOf(_fr ,len<<1);
      _d  = Arrays.copyOf(_d  ,len<<1);
      _fun= Arrays.copyOf(_fun,len<<=1);
    }
  }
  void push( Frame fr ) { push(1); _fr [_sp-1] = addRef(fr) ; assert check_refcnt(fr.anyVec()); }
  void push( double d ) { push(1); _d  [_sp-1] = d  ; }
  void push( ASTOp fun) { push(1); _fun[_sp-1] = addRef(fun); }
  void push( Frame fr, String key ) { push(fr); _key[_sp-1]=key; }

  // Copy from display offset d, nth slot
  void push_slot( int d, int n ) {
    assert d==0;                // Should use a fcn's closure for d>1
    int idx = _display[_tod-d]+n;
    push(1);
    _fr [_sp-1] = addRef(_fr [idx]);
    _d  [_sp-1] =        _d  [idx];
    _fun[_sp-1] = addRef(_fun[idx]);
    assert check_refcnt(_fr[0].anyVec());
  }
  void push_slot( int d, int n, Env global ) {
    assert _refcnt==null;       // Should use a fcn's closure for d>1
    int idx = _display[_tod-d]+n;
    int gidx = global._sp;
    global.push(1);
    global._fr [gidx] = global.addRef(_fr [idx]);
    global._d  [gidx] =               _d  [idx] ;
    global._fun[gidx] = global.addRef(_fun[idx]);
    assert global.check_refcnt(global._fr[0].anyVec());
  }
  // Copy from TOS into a slot.  Does NOT pop results.
  void tos_into_slot( int d, int n, String id ) {
    // In a copy-on-modify language, only update the local scope, or return val
    assert d==0 || (d==1 && _display[_tod]==n+1);
    int idx = _display[_tod-d]+n;
    subRef(_fr [idx]);
    subRef(_fun[idx]);
    _fr [idx] = addRef(_fr [_sp-1]);
    _d  [idx] =       _d   [_sp-1] ;
    _fun[idx] = addRef(_fun[_sp-1]);
    if( d==0 ) _key[idx] = id;
    assert _fr[0]== null || check_refcnt(_fr[0].anyVec());
  }

  // Push a scope, leaving room for passed args
  int pushScope(int args) {
    assert fun(-args-1) instanceof ASTFunc; // Expect a function under the args
    return _display[++_tod] = _sp-args;
  }
  // Grab the function for nested scope d
  ASTFunc funScope( int d ) { return (ASTFunc)_fun[_display[_tod]-1]; }

  // Pop a slot.  Lowers refcnts on vectors.  Always leaves stack null behind
  // (to avoid dangling pointers stretching lifetimes).
  void pop( Env global ) {
    assert _sp > _display[_tod]; // Do not over-pop current scope
    _sp--;
    _fun[_sp]=global.subRef(_fun[_sp]);
    _fr [_sp]=global.subRef(_fr [_sp]);
    assert _sp==0 || _fr[0]==null || check_refcnt(_fr[0].anyVec());
  }
  void pop( ) { pop(this); }
  void pop( int n ) { for( int i=0; i<n; i++ ) pop(); }

  void popScope() {
    assert _tod > 0;            // Something to pop?
    assert _sp >= _display[_tod]; // Did not over-pop already?
    while( _sp > _display[_tod] ) pop();
    _tod--;
  }

  public double  popDbl  () { assert isDbl(); return _d  [--_sp]; }
  public ASTOp   popFun  () { assert isFun(); ASTOp  op = _fun[--_sp]; _fun[_sp]=null; return op; }
  // Pop & return a Frame; ref-cnt of all things remains unchanged.
  // Caller is responsible for tracking lifetime.
  public Frame  popFrame() { assert isFrame(); Frame fr = _fr [--_sp]; _fr [_sp]=null; assert allAlive(fr); return fr; }
  // Replace a function invocation with it's result
  public void poppush(double d) { pop(); push(d); }

  // Capture the current environment & return it (for some closure's future execution).
  Env capture( ) { return new Env(this); }
  private Env( Env e ) {
    _sp = e._sp;
    _fr = Arrays.copyOf(e._fr ,_sp);
    _d  = Arrays.copyOf(e._d  ,_sp);
    _fun= Arrays.copyOf(e._fun,_sp);
    _tod= e._tod;
    _display = Arrays.copyOf(e._display,_tod+1);
    // All other fields are ignored/zero
    _refcnt = null;
  }


  // Nice assert
  boolean allAlive(Frame fr) {
    for( Vec vec : fr.vecs() )
      assert _refcnt.get(vec) > 0;
    return true;
  }

  // Lower the refcnt on all vecs in this frame.
  // Immediately free all vecs with zero count.
  // Always return a null.
  public Frame subRef( Frame fr ) {
    if( fr == null ) return null;
    Futures fs = null;
    for( Vec vec : fr.vecs() ) {
      int cnt = _refcnt.get(vec)-1;
      if( cnt > 0 ) _refcnt.put(vec,cnt);
      else {
        if( fs == null ) fs = new Futures();
        UKV.remove(vec._key,fs);
        _refcnt.remove(vec);
      }
    }
    if( fs != null )
      fs.blockForPending();
    return null;
  }
  // Lower refcounts on all vecs captured in the inner environment
  public ASTOp subRef( ASTOp op ) {
    if( op == null ) return null;
    if( !(op instanceof ASTFunc) ) return null;
    ASTFunc fun = (ASTFunc)op;
    if( fun._env != null ) fun._env.subRef(this);
    else System.out.println("Popping fcn object, never executed no environ capture");
    return null;
  }

  // Add a refcnt to all vecs in this frame
  Frame addRef( Frame fr ) {
    if( fr == null ) return null;
    for( Vec vec : fr.vecs() ) {
      Integer I = _refcnt.get(vec);
      assert I==null || I>0;
      _refcnt.put(vec,I==null?1:I+1);
    }
    // (Temporary) Heuristic for picking up current master frame/group.
    // When we're creating new vecs (e.g. runif(10000)) we want to have them compatible with current dataset.
    // To be able to guess the right dataset in cases when the operation does not involve the dataset directly,
    // we set the master frame as the latest referenced dataset frame(not tmp-frame!). I identify dataset frames by checking they're
    // vector group's key (typically based on the underlying filename) for some common patterns such as nfs:/, hdfs:/ commonly found
    // in filesystem paths.
    //
    // TODO If we keep this longer, we should at least ensure that all Frame's have vector group's based on some dataset and skip the
    // dataset test.
    VectorGroup vg = fr.anyVec().group();
    String strKey = vg.vecKey(0).toString(); // ugly heuristic to recognize dataset's frame
    if(strKey.contains(".csv") || strKey.contains(".data")
        || strKey.contains("nfs:/")
        || strKey.contains("hdfs:/")
        || strKey.contains("s3n:/")
        || strKey.contains("s3:/")
        || strKey.contains("autoframe")){
       _vg = vg;
      _currentMasterFrame = fr;
    }
    return fr;
  }
  ASTOp addRef( ASTOp op ) {
    if( op == null ) return null;
    if( !(op instanceof ASTFunc) ) return op;
    ASTFunc fun = (ASTFunc)op;
    if( fun._env != null ) fun._env.addRef(this);
    else System.out.println("Pushing fcn object, never executed no environ capture");
    return op;
  }
  private void addRef(Env global) {
    for( int i=0; i<_sp; i++ ) {
      if( _fr [i] != null ) global.addRef(_fr [i]);
      if( _fun[i] != null ) global.addRef(_fun[i]);
    }
  }
  private void subRef(Env global) {
    for( int i=0; i<_sp; i++ ) {
      if( _fr [i] != null ) global.subRef(_fr [i]);
      if( _fun[i] != null ) global.subRef(_fun[i]);
    }
  }


  // Remove everything
  public void remove() {
    // Remove all shallow scopes
    while( _tod > 1 ) popScope();
    // Push changes at the outer scope into the K/V store
    while( _sp > 0 ) {
      if( isFrame() && _key[_sp-1] != null ) { // Has a K/V mapping?
        Frame fr = popFrame();  // Pop w/out lower refcnt & delete
        int refcnt = _refcnt.get(fr.anyVec());
        for( Vec v : fr.vecs() )
          if( _refcnt.get(v) != refcnt )
            throw H2O.unimpl();
        assert refcnt > 0;
        Frame fr2=fr;
        if( refcnt > 1 ) {       // Need a deep-copy now
          fr2 = fr.deepSlice(null,null);
          subRef(fr);            // Now lower refcnt for good assertions
        }                        // But not down to zero (do not delete items in global scope)
        UKV.put(Key.make(_key[_sp]),fr2);
      } else
        pop();
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
    for( int i=0; i<_sp; i++ )
      if( _fr[i] != null && _fr[i].find(vec) != -1 ) cnt++;
      else if( _fun[i] != null && (_fun[i] instanceof ASTFunc) )
        cnt += ((ASTFunc)_fun[i])._env.compute_refcnt(vec);
    return cnt;
  }
  boolean check_refcnt( Vec vec ) {
    Integer I = _refcnt.get(vec);
    int cnt0 = I==null ? 0 : I;
    int cnt1 = compute_refcnt(vec);
    if( cnt0==cnt1 ) return true;
    System.out.println("Refcnt is "+cnt0+" but computed as "+cnt1);
    return false;
  }


  // Pop and return the result as a string
  public String resultString( ) {
    assert _tod==0 : "Still have lexical scopes past the global";
    String s = toString(_sp-1,true);
    pop();
    return s;
  }

  public String toString(int i, boolean verbose_fun) {
    if( _fr[i] != null ) return _fr[i].numRows()+"x"+_fr[i].numCols();
    else if( _fun[i] != null ) return _fun[i].toString(verbose_fun);
    return Double.toString(_d[i]);
  }
  @Override public String toString() {
    String s="{";
    for( int i=0; i<_sp; i++ )   s += toString(i,false)+",";
    return s+"}";
  }
}
