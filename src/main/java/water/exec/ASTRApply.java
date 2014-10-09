package water.exec;

import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.util.FrameUtils;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
// R's Apply.  Function is limited to taking a single column and returning
// a single column.  Double is limited to 1 or 2, statically determined.
class ASTRApply extends ASTOp {
  static final String VARS[] = new String[]{ "", "ary", "dbl1.2", "fcn"};
  ASTRApply( ) { super(VARS,
                       new Type[]{ Type.ARY, Type.dblary(), Type.dblary(), Type.fcn(new Type[]{Type.dblary(),Type.ARY}) },
                       OPF_PREFIX,
                       OPP_PREFIX,
                       OPA_RIGHT); }
  protected ASTRApply( String vars[], Type ts[], int form, int prec, int asso) { super(vars,ts,form,prec,asso); }
  @Override String opStr(){ return "apply";}
  @Override ASTOp make() {return new ASTRApply();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // Peek everything from the stack
    final ASTOp op = env.fcn(-1);    // ary->dblary but better be ary[,1]->dblary[,1]
    double d = env.dbl(-2);    // MARGIN: ROW=1, COLUMN=2 selector
    Frame fr = env.ary(-3);    // The Frame to work on
    if( d==2 || d== -1 ) {     // Work on columns?
      int ncols = fr.numCols();

      double ds[][] = null; // If results are doubles, gather in small array
      Frame fr2 = null;     // If the results are Vecs, gather them in this Frame
      String err = "apply requires that "+op+" return 1 column";
      if( op._t.ret().isDbl() ) ds = new double[ncols][1];
      else                     fr2 = new Frame(new String[0],new Vec[0]);

      // Apply the function across columns
      try {
        Vec vecs[] = fr.vecs();
        for( int i=0; i<ncols; i++ ) {
          env.push(op);
          env.push(new Frame(new String[]{fr._names[i]},new Vec[]{vecs[i]}));
          env.fcn(-2).apply(env, 2, null);
          if( ds != null ) {    // Doubles or Frame results?
            ds[i][0] = env.popDbl();
          } else {                // Frame results
            fr2.add(fr._names[i], env.popXAry().theVec(err));
          }
        }
      } catch( IllegalArgumentException iae ) {
        env.subRef(fr2,null);
        throw iae;
      }
      env.pop(4);
      if( ds != null ) env.push(FrameUtils.frame(new String[]{"C1"},ds));
      else env.push(fr2);
      assert env.isAry();
      return;
    }
    if( d==1 || d==-2) {      // Work on rows
      // apply on rows is essentially a map function
      Type ts[] = new Type[2];
      ts[0] = Type.unbound();
      ts[1] = Type.ARY;
      Type ft1 = Type.fcn(ts);
      Type ft2 = op._t.find();  // Should be a function type
      if( !ft1.union(ft2) ) {
        if( ft2._ts.length != 2 )
          throw new IllegalArgumentException("FCN " + op.toString() + " cannot accept one argument.");
        if( !ft2._ts[1].union(ts[1]) )
          throw new IllegalArgumentException("Arg " + op._vars[1] + " typed " + ft2._ts[1].find() + " but passed as " + ts[1]);
        assert false;
      }
      // find out return type
      double[] rowin = new double[fr.vecs().length];
      for (int c = 0; c < rowin.length; c++) rowin[c] = fr.vecs()[c].at(0);
      final int outlen = op.map(env,rowin,null).length;
      final Env env0 = env;
      MRTask2 mrt = new MRTask2() {
        @Override public void map(Chunk[] cs, NewChunk[] ncs) {
          double rowin [] = new double[cs.length];
          double rowout[] = new double[outlen];
          for (int row = 0; row < cs[0]._len; row++) {
            for (int c = 0; c < cs.length; c++) rowin[c] = cs[c].at0(row);
            op.map(env0, rowin, rowout);
            for (int c = 0; c < ncs.length; c++) ncs[c].addNum(rowout[c]);
          }
        }
      };
      String[] names = new String[outlen];
      for (int i = 0; i < names.length; i++) names[i] = "C"+(i+1);
      Frame res = mrt.doAll(outlen,fr).outputFrame(names, null);
      env.poppush(4,res,null);
      return;
    }
    throw new IllegalArgumentException("MARGIN limited to 1 (rows) or 2 (cols)");
  }
}

// --------------------------------------------------------------------------
// Same as "apply" but defaults to columns.
class ASTSApply extends ASTRApply {
  static final String VARS[] = new String[]{ "", "ary", "fcn"};
  ASTSApply( ) { super(VARS,
                       new Type[]{ Type.ARY, Type.ARY, Type.fcn(new Type[]{Type.dblary(),Type.ARY}) },
                       OPF_PREFIX,
                       OPP_PREFIX,
                       OPA_RIGHT); }
  @Override String opStr(){ return "sapply";}
  @Override ASTOp make() {return new ASTSApply();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    // Stack: SApply, ary, fcn
    //   -->: RApply, ary, 2, fcn
    assert env.isFcn(-3);
    env._fcn[env._sp-3] = new ASTRApply();
    ASTOp fcn = env.popFcn();   // Pop, no ref-cnt
    env.push(2.0);
    env.push(1);
    env._fcn[env._sp-1] = fcn;  // Push, no ref-cnt
    super.apply(env,argcnt+1,null);
  }
}

// --------------------------------------------------------------------------
// unique(ary)
// Returns only the unique rows

class ASTUnique extends ASTddply {
  static final String VARS[] = new String[]{ "", "ary"};
  ASTUnique( ) { super(VARS, new Type[]{Type.ARY, Type.ARY}); }
  @Override String opStr(){ return "unique";}
  @Override ASTOp make() {return new ASTUnique();}
  @Override void apply(Env env, int argcnt, ASTApply apply) {
    Thread cThr = Thread.currentThread();
    Frame fr = env.peekAry();
    int cols[] = new int[fr.numCols()];
    for( int i=0; i<cols.length; i++ ) cols[i]=i;
    ddplyPass1 p1 = new ddplyPass1( false, cols ).doAll(fr);
    double dss[][] = new double[p1._groups.size()][];
    int i=0;
    for( Group g : p1._groups.keySet() )
      dss[i++] = g._ds;
    Frame res = FrameUtils.frame(fr._names,dss);
    env.poppush(2,res,null);
  }
}
