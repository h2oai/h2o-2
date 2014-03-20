package water.exec;

import java.util.ArrayList;
import water.H2O;
import water.Key;
import water.fvec.AppendableVec;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
public class ASTFunc extends ASTOp {
  final AST _body;
  final int _tmps;
  Env _env;                     // Captured environment at each apply point
  ASTFunc( String vars[], Type vtypes[], AST body, int tmps ) {
    super(vars,vtypes,OPF_PREFIX,OPP_PREFIX,OPA_RIGHT); _body = body; _tmps=tmps;
  }
  ASTFunc( String vars[], Type t, AST body, int tmps ){
    super(vars,t,OPF_PREFIX,OPP_PREFIX,OPA_RIGHT); _body = body; _tmps=tmps;
  }
  @Override String opStr() { return "fun"; }
  //@Override ASTOp make() { throw H2O.fail();}
  @Override ASTOp make() { return new ASTFunc(_vars, _t.copy(), _body, _tmps); }
  static ASTOp parseFcn(Exec2 E ) {
    int x = E._x;
    String var = E.isID();
    if( var == null ) return null;
    if( !"function".equals(var) ) { E._x = x; return null; }
    E.xpeek('(',E._x,null);
    ArrayList<ASTId> vars = new ArrayList<ASTId>();
    if( !E.peek(')',false) ) {
      while( true ) {
        x = E._x;
        var = E.isID();
        if( var == null ) E.throwErr("Invalid var",x);
        for( ASTId id : vars ) if( var.equals(id._id) ) E.throwErr("Repeated argument",x);
        // Add unknown-type variable to new vars list
        vars.add(new ASTId(Type.unbound(),var,0,vars.size()));
        if( E.peek(')') ) break;
        E.xpeek(',',E._x,null);
        E.skipWS();
      }
    }
    int argcnt = vars.size();   // Record current size, as body may extend
    // Parse the body
    E._env.push(vars);
    AST body = E.peek('{',false) ? E.xpeek('}',E._x,ASTStatement.parse(E)) : parseCXExpr(E,true);
    if( body == null ) E.throwErr("Missing function body",x);
    E._env.pop();

    // The body should force the types.  Build a type signature.
    String xvars[] = new String[argcnt+1];
    Type   types[] = new Type  [argcnt+1];
    xvars[0] = "fun";
    types[0] = body._t;         // Return type of body
    for( int i=0; i<argcnt; i++ ) {
      ASTId id = vars.get(i);
      xvars[i+1] = id._id;
      types[i+1] = id._t;
    }
    return new ASTFunc(xvars,types,body,vars.size()-argcnt);
  }  
  
  @Override void exec(Env env) { 
    // We need to push a Closure: the ASTFunc plus captured environment.
    // Make a shallow copy (the body remains shared across all ASTFuncs).
    // Then fill in the current environment.
    ASTFunc fun = (ASTFunc)clone();
    fun._env = env.capture(false);
    env.push(fun);
  }
  @Override void apply(Env env, int argcnt, ASTApply apply) { 
    int res_idx = env.pushScope(argcnt-1);
    env.push(_tmps);
    _body.exec(env);
    env.tos_into_slot(res_idx-1,null);
    env.popScope();
  }

  @Override double[] map(Env env, double[] in, double[] out) {
    final int sp = env._sp;
    Key key = Vec.VectorGroup.VG_LEN1.addVecs(1)[0];
    AppendableVec av = new AppendableVec(key);
    NewChunk nc = new NewChunk(av,0);
    for (double v : in) nc.addNum(v);
    nc.close(0,null);
    Frame fr = new Frame(new String[]{"row"},new Vec[]{av.close(null)});
    env.push(this);
    env.push(fr);
    this.apply(env,2,null);
    if (env.isDbl()) {
      if (out==null || out.length<1) out= new double[1];
      out[0] = env.popDbl();
    } else if (env.isAry()) {
      fr = env.peekAry();
      if (fr.vecs().length > 1) H2O.unimpl();
      Vec vec = fr.anyVec();
      if (vec.length() > 1<<8) H2O.unimpl();
      if (out==null || out.length<vec.length()) out= new double[(int)vec.length()];
      for (long i = 0; i < vec.length(); i++) out[(int)i] = vec.at(i);
      env.pop();
    } else {
      H2O.unimpl();
    }
    assert sp == env._sp;
    return out;
  }

  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    indent(sb,d).append(this).append(") {\n");
    _body.toString(sb,d+1).append("\n");
    return indent(sb,d).append("}");
  }
}
