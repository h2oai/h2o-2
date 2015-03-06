package water.exec;

import water.*;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;

import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

/** Parse a generic R string and build an AST, in the context of an H2O Cloud
 *  @author cliffc@0xdata.com
 */

// --------------------------------------------------------------------------
abstract public class AST extends Iced {
  final Type _t;
  AST( Type t ) { assert t != null; _t = t; }
  static AST parseCXExpr(Exec2 E, boolean EOS ) {
    if( EOS && E.peekEOS() ) { E._x--; return new ASTNop(); }
    AST ast2, ast = ASTApply.parseInfix(E,0,EOS);
    if( ast == null ) return ASTAssign.parseNew(E,EOS);
    // In case of a slice or id, try match an assignment
    if( ast instanceof ASTSlice || ast instanceof ASTId)
      if( (ast2 = ASTAssign.parse(E,ast,EOS)) != null ) return ast2;
    // Next try match an IFELSE statement
    if( (ast2 = ASTIfElse.parse(E,ast,EOS)) != null ) return ast2;
    // Return the infix: op1* expr {op2 op1* expr}*
    return ast;
  }

  static AST parseVal(Exec2 E, boolean EOS ) {
    E.skipWS(EOS);
    AST ast;
    // Simple paren expression
    if( E.peek('(',EOS) )  return E.xpeek(')',E._x,parseCXExpr(E,false));
    if( (ast = ASTId   .parse(E)) != null ) return ast;
    if( (ast = ASTNum  .parse(E)) != null ) return ast;
    if( (ast = ASTOp   .parse(E)) != null ) return ast;
    if( (ast = ASTStr  .parse(E)) != null ) return ast;
    return null;
  }
  abstract void exec(Env env);
  boolean isPosConstant() { return false; }
  // Scrape out a column name, if we can.  NULL if we cannot.
  String argName() { return null; }
  protected StringBuilder indent( StringBuilder sb, int d ) {
    for( int i=0; i<d; i++ ) sb.append("  ");
    return sb.append(_t).append(' ');
  }
  public StringBuilder toString( StringBuilder sb, int d ) { return indent(sb,d).append(this); }
}

// --------------------------------------------------------------------------
class ASTNop extends AST {
  ASTNop() { super(Type.DBL); }
  @Override void exec(Env env) { env.push(Double.NaN); }
}

// --------------------------------------------------------------------------
class ASTStatement extends AST {
  final AST[] _asts;
  ASTStatement( AST[] asts ) { super(asts[asts.length-1]._t); _asts = asts; }

  static ASTStatement parse( Exec2 E ) {
    ArrayList<AST> asts = new ArrayList<AST>();
    while( true ) {
      AST ast = parseCXExpr(E,true);
      if( ast == null ) break;
      asts.add(ast);
      if( !E.peekEOS() ) break; // if not finding statement separator, break
    }
    if( asts.size()==0 ) return null;
    return new ASTStatement(asts.toArray(new AST[asts.size()]));
  }
  @Override void exec(Env env) {
    for( int i=0; i<_asts.length-1; i++ ) {
      _asts[i].exec(env);       // Exec all statements
      env.pop();                // Pop all intermediate results
    }
    _asts[_asts.length-1].exec(env); // Return final statement as result
  }
  @Override public String toString() { return ";;;"; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    for( int i=0; i<_asts.length-1; i++ )
      _asts[i].toString(sb,d+1).append(";\n");
    return _asts[_asts.length-1].toString(sb,d+1);
  }
}

// --------------------------------------------------------------------------
class ASTApply extends AST {
  final AST _args[];
  private ASTApply( AST args[] ) { super(args[0]._t.ret());  _args = args;  }

  // Wrap compatible but different-sized ops in reduce/bulk ops.
  static ASTApply make(AST args[],Exec2 E, int x) {
    // Make a type variable for this application
    Type ts[] = new Type[args.length];
    ts[0] = Type.unbound();
    for( int i=1; i<ts.length; i++ )
      ts[i] = args[i]._t.find();
    Type ft1 = Type.fcn(ts);
    AST fast = args[0];
    Type ft2 = fast._t.find();  // Should be a function type
    if( ft1.union(ft2) )        // Union 'em
      return new ASTApply(args);
    // Error handling
    if( ft2.isNotFun() )      // Oops, failed basic sanity
      E.throwErr("Function-parens following a "+ft2,x);
    if( ft2._ts.length != ts.length )
      E.throwErr("Passed "+(ts.length-1)+" args but expected "+(ft2._ts.length-1),x);
    String vars[] = ((ASTOp)fast)._vars;
    for( int i=1; i<ts.length; i++ )
      if( !ft2._ts[i].union(args[i]._t) )
        E.throwErr("Arg '"+vars[i]+"'"+" typed as "+ft2._ts[i]+" but passed "+args[i]._t.find(),x);
    throw H2O.fail();
  }

  // Parse a prefix operator
  static AST parsePrefix(Exec2 E, boolean EOS) {
    int x0 = E._x;
    AST pre = parseVal(E,EOS);
    if( pre == null ) return null;
    while( true ) {
      int x = E._x;
      if( !E.peek('(', true) ) return pre; // Plain op, no prefix application
      if (pre._t.isNotFun()) {
        E._x = x0; if ((pre = ASTOp.parse(E)) == null) E.throwErr("No potential function was found.", x0);
        if( !E.peek('(') ) return pre;
      }
      AST args[] = new AST[] { pre, null };
      int i=1;
      if( !E.peek(')') ) {
        while( true ) {
          if( (args[i++] = parseCXExpr(E,false)) == null )
            E.throwErr("Missing argument",E._x);
          if (args[i-1] instanceof ASTAssign) {
            ASTAssign a = (ASTAssign)args[i-1];
            if (a._lhs.argName() != null && a._lhs.argName().equals("na.rm")) {
              ASTReducerOp op = (ASTReducerOp)args[0];
              op._narm = (a._eval.argName().equals("T") || a._eval.argName().equals("TRUE") || a._eval.toString().equals("1.0"));
              args[0] = op;
            }
          }
          if( E.peek(')') ) break;
          E.xpeek(',',E._x,null);
          if( i==args.length ) args = Arrays.copyOf(args,args.length<<1);
        }
      }
      args = Arrays.copyOf(args,i);
      pre = make(args,E,x);
    }
  }

  // Parse an infix boolean operator
  static AST parseInfix(Exec2 E, int curr_prec, boolean EOS) {
    int x = E._x;
    AST ast;
    E.skipWS(EOS);
    ASTOp op1 = ASTOp.parseUniInfixOp(E);
    if (op1 != null) {
      // CASE 1 ~ INFIX1 := [] OP INFIX
      if ((ast = parseInfix(E,op1._precedence,EOS)) != null)
        ast = make(new AST[]{op1,ast},E,x);
      else {
        // CASE 2 ~ INFIX1 := [] OP
        E._x = x;
        ast = ASTSlice.parse(E, EOS);
      }
    } else {
      // CASE 3 ~ INFIX1 := [] SLICE
      ast = ASTSlice.parse(E, EOS);
    }
    // CASE 0 ~ []
    if (ast == null) return null;

    // INFIX := INFIX1 OP INFIX
    while( true ) {
      int op_x = E._x;
      E.skipWS(EOS);
      ASTOp op = ASTOp.parseBinInfixOp(E);
      if( op == null
       || op._precedence < curr_prec
       || (op.leftAssociate() && op._precedence == curr_prec) )
      { E._x = op_x; return ast; }
      op_x = E._x;
      AST rite = parseInfix(E,op._precedence, false);
      if (rite == null)
        E.throwErr("Missing expr or unknown ID", op_x);
      ast = make(new AST[]{op,ast,rite},E,x);
    }
  }

  @Override public String toString() { return _args[0].toString()+"()"; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    _args[0].toString(sb,d).append("\n");
    for( int i=1; i<_args.length-1; i++ )
      _args[i].toString(sb,d+1).append('\n');
    return _args[_args.length-1].toString(sb,d+1);
  }
  // Apply: execute all arguments (including the function argument) yielding
  // the function itself, plus all normal arguments on the stack.  Then execute
  // the function, which is responsible for popping all arguments and pushing
  // the result.
  @Override void exec(Env env) {
    int sp = env._sp;
    for( AST arg : _args ) arg.exec(env);
    assert sp+_args.length==env._sp;
    env.fcn(-_args.length).apply(env,_args.length,this);
  }
}

// --------------------------------------------------------------------------
class ASTSlice extends AST {
  final AST _ast, _cols, _rows; // 2-D slice of an expression
  ASTSlice( Type t, AST ast, AST cols, AST rows ) {
    super(t); _ast = ast; _cols = cols; _rows = rows;
  }
  static AST parse(Exec2 E, boolean EOS ) {
    int x = E._x;
    AST ast = ASTApply.parsePrefix(E, EOS);
    if( ast == null ) return null;
    if( !E.peek('[',EOS) )      // Not start of slice?
      return ASTNamedCol.parse(E,ast,EOS); // Also try named col slice
    if( !Type.ARY.union(ast._t) ) E.throwErr("Not an ary",x);
    if(  E.peek(']',false) ) return ast; // [] ===> same as no slice
    AST rows=E.xpeek(',',(x=E._x),parseCXExpr(E, false));
    if( rows != null && !rows._t.union(Type.dblary()) ) E.throwErr("Must be scalar or array",x);
    AST cols=E.xpeek(']',(x=E._x),parseCXExpr(E, false));
    if( cols != null && !cols._t.union(Type.dblary()) )
      if (cols._t.isStr()) E.throwErr("The current Exec does not handle strings",x);
      else E.throwErr("Must be scalar or array",x);
    Type t =                    // Provable scalars will type as a scalar
      rows != null && rows.isPosConstant() &&
      cols != null && cols.isPosConstant() ? Type.DBL : Type.ARY;
    return new ASTSlice(t,ast,cols,rows);
  }

  @Override void exec(Env env) {
    int sp = env._sp;  _ast.exec(env);  assert sp+1==env._sp;

    // Scalar load?  Throws AIIOOB if out-of-bounds
    if( _t.isDbl() ) {
      // Known that rows & cols are simple positive constants.
      // Use them directly, throwing a runtime error if OOB.
      long row = (long)((ASTNum)_rows)._d;
      int  col = (int )((ASTNum)_cols)._d;
      Frame ary=env.popAry();
      String skey = env.key();
      double d = ary.vecs()[col-1].at(row-1);
      env.subRef(ary,skey);     // Toss away after loading from it
      env.push(d);
    } else {
      // Else It's A Big Copy.  Some Day look at proper memory sharing,
      // disallowing unless an active-temp is available, etc.
      // Eval cols before rows (R's eval order).
      Frame ary=env._ary[env._sp-1];  // Get without popping
      Object cols = select(ary.numCols(),_cols,env);
      Object rows = select(ary.numRows(),_rows,env);
      Frame fr2 = ary.deepSlice(rows,cols);
      // After slicing, pop all expressions (cannot lower refcnt till after all uses)
      if( rows!= null ) env.pop();
      if( cols!= null ) env.pop();
      if( fr2 == null ) fr2 = new Frame(); // Replace the null frame with the zero-column frame
      env.pop();                // Pop sliced frame, lower ref
      env.push(fr2);
    }
  }

  // Execute a col/row selection & return the selection.  NULL means "all".
  // Error to mix negatives & positive.  Negative list is sorted, with dups
  // removed.  Positive list can have dups (which replicates cols) and is
  // ordered.  numbers.  1-based numbering; 0 is ignored & removed.
  static Object select( long len, AST ast, Env env ) {
    if( ast == null ) return null; // Trivial "all"
    ast.exec(env);
    long cols[];
    if( !env.isAry() ) {
      int col = (int)env._d[env._sp-1]; // Peek double; Silent truncation (R semantics)
      if( col < 0 && col < -len ) col=0; // Ignore a non-existent column
      if( col == 0 ) return new long[0];
      return new long[]{col};
    }
    // Got a frame/list of results.
    // Decide if we're a toss-out or toss-in list
    Frame ary = env._ary[env._sp-1];  // Peek-frame
    if( ary.numCols() != 1 ) throw new IllegalArgumentException("Selector must be a single column: "+ary.toStringNames());
    Vec vec = ary.anyVec();
    // Check for a matching column of bools.
    if( ary.numRows() == len && vec.min()>=0 && vec.max()<=1 && vec.isInt() && ary.numRows() > 1)
      return ary;    // Boolean vector selection.
    // Convert single vector to a list of longs selecting rows
    if(ary.numRows() > 10000000) throw H2O.fail("Unimplemented: Cannot explicitly select > 10000000 rows in slice.");
    cols = MemoryManager.malloc8((int)ary.numRows());
    for(int i = 0; i < cols.length; ++i){
      if(vec.isNA(i))throw new IllegalArgumentException("Can not use NA as index!");
      cols[i] = vec.at8(i);
    }
    return cols;
  }

  @Override public String toString() { return "[,]"; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    indent(sb,d).append(this).append('\n');
    _ast.toString(sb,d+1).append("\n");
    if( _cols==null ) indent(sb,d+1).append("all\n");
    else      _cols.toString(sb,d+1).append("\n");
    if( _rows==null ) indent(sb,d+1).append("all");
    else      _rows.toString(sb,d+1);
    return sb;
  }
}

// --------------------------------------------------------------------------
class ASTNamedCol extends AST {
  final AST _ast;               // named slice of an expression
  final String _colname;        //
  ASTNamedCol( Type t, AST ast, String colname ) {
    super(t); _ast = ast; _colname=colname;
  }
  static AST parse(Exec2 E, AST ast, boolean EOS ) {
    if( !E.peek('$',true) ) return ast;
    int x = E._x;
    E.skipWS(EOS);
    String colname = E.isID();
    if( colname == null ) E.throwErr("Missing column name after $",x);
    return new ASTNamedCol(Type.ARY,ast,colname);
  }
  @Override void exec(Env env) {
    int sp = env._sp;  _ast.exec(env);  assert sp+1==env._sp;
    Frame ary=env.peekAry();
    int cidx = ary.find(_colname);
    if( cidx== -1 ) throw new IllegalArgumentException("Missing column "+_colname+" in frame "+ary.toStringNames());
    Frame fr2 = new Frame(new String[]{ary._names[cidx]}, new Vec[]{ary.vecs()[cidx]});
    env.poppush(1,fr2,null);
  }
  @Override public String toString() { return "$"+_colname; }
}

// --------------------------------------------------------------------------
class ASTId extends AST {
  final String _id;
  final int _depth;             // *Relative* lexical depth of definition
  final int _num;               // Number/slot in the lexical scope
  ASTId( Type t, String id, int d, int n ) { super(t); _id=id; _depth=d; _num=n; }
  // Parse a valid ID, or return null;
  static ASTId parse(Exec2 E) {
    int x = E._x;
    String var = E.isID();
    if( var == null ) return null;
    // Built-in ops parse as ops, not vars
    if( ASTOp.isInfixOp(var) ) { E._x=x; return null; }
    // See if pre-existing
    for( int d=E.lexical_depth(); d >=0; d-- ) {
      ArrayList<ASTId> asts = E._env.get(d);
      for( int i=asts.size()-1; i >=0; i-- ) {
        ASTId id = asts.get(i);
        if( var.equals(id._id) )
          // Return an ID with a relative lexical depth and same slot#
          return new ASTId(id._t,id._id,E.lexical_depth()-d,id._num);
      }
    }
    // Never see-before ID?  Treat as a bad parse
    E._x=x;
    return null;
  }
  // Parse a NEW valid ID, or return null;
  static String parseNew(Exec2 E) {
    int x = E._x;
    String id = E.isID();
    if( id == null ) return null;
    // Built-in ops parse as ops, not vars
    if( ASTOp.isInfixOp(id) ) { E._x=x; return null; }
    return id;
  }
  @Override void exec(Env env) {
    // Local scope?  Grab from the stack.
    if( _depth ==0 ) {
      env.push_slot(_depth,_num);
      return;
    }
    // Nested scope?  need to grab from the nested-scope closure
    ASTFunc fcn = env.fcnScope(_depth);
    fcn._env.push_slot(_depth-1,_num,env);
  }
  @Override String argName() { return _id; }
  @Override public String toString() { return _id; }
}

class ASTStr extends AST {
  final String _str;
  ASTStr(String str) { super(Type.STR); _str=str; }
  // Parse a string, or throw a parse error
  static ASTStr parse(Exec2 E) {
    String str = E.isString();
    if (str != null) {
      E._x += str.length()+2; //str + quotes
      return new ASTStr(str);
    }
    return null;
  }
  @Override void exec(Env env) { env.push(_str); }
  @Override public String toString() { return _str; }
}

// --------------------------------------------------------------------------
class ASTAssign extends AST {
  final AST _lhs;
  final AST _eval;
  ASTAssign( AST lhs, AST eval ) { super(eval._t); _lhs=lhs; _eval=eval; }
  // Parse a valid LHS= or return null
  static ASTAssign parse(Exec2 E, AST ast, boolean EOS) {
    int x = E._x;
    // Allow '=' and '<-' assignment
    if( !E.isAssign(EOS) ) return null;
    AST ast2=ast;
    ASTSlice slice= null;
    if( (ast instanceof ASTSlice) ) // Peek thru slice op
      ast2 = (slice=(ASTSlice)ast)._ast;
    // Must be a simple in-scope ID
    if( !(ast2 instanceof ASTId) ) E.throwErr("Can only assign to ID (or slice)",x);
    ASTId id = (ASTId)ast2;
    final AST eval = parseCXExpr(E, false);
    if( eval == null ) E.throwErr("Missing RHS",x);
    boolean partial = slice != null && (slice._cols != null || slice._rows != null);
    if( partial ) {             // Partial slice assignment?
      if( eval._t.isFcn() ) E.throwErr("Assigning a "+eval._t+" into '"+id._id+"' which is a "+id._t,x);
      if(  E.lexical_depth()> 0 ) throw H2O.unimpl(); // Must copy whole array locally, before updating the local copy
    }

    if( id._depth > 0 ) {       // Shadowing an outer scope?
      // Inner-scope assignment to a new local
      ast = id = extend_local(E,eval._t,id._id);
    } else {                    // Overwriting same scope
      if( E.lexical_depth()>0 ) { // Inner scope?
        if( !ast._t.union(eval._t) ) // Disallow type changes in local scope in functions.
          E.throwErr("Assigning a "+eval._t+" into '"+id._id+"' which is a "+id._t,x);
      } else {                  // Outer scope; can change type willy-nilly
        if( !partial && !ast._t.union(eval._t) ) {
          ArrayList<ASTId> vars = E._env.get(0);
          ASTId id2 = new ASTId(eval._t,id._id,0,id._num);
          vars.set(id2._num,id2);
          ast = id2;
        }
      }
    }

    return new ASTAssign(ast,eval);
  }
  // Parse a valid LHS= or return null - for a new variable
  static ASTAssign parseNew(Exec2 E, boolean EOS) {
    int x = E._x;
    String var = ASTId.parseNew(E);
    if( var == null ) return null;
    if( !E.isAssign(EOS) ) {
      if( Exec2.isLetter(var.charAt(0) ) ) E.throwErr("Unknown var "+var,x);
      E._x = x; return null;      // Let higher parse levels sort it out
    }
    x = E._x;
    AST eval = parseCXExpr(E, EOS);
    if( eval == null ) E.throwErr("Missing RHS",x);
    // Extend the local environment by the new name
    return new ASTAssign(extend_local(E,eval._t,var),eval);
  }
  static ASTId extend_local( Exec2 E, Type t, String var ) {
    ArrayList<ASTId> vars = E._env.get(E.lexical_depth());
    ASTId id = new ASTId(t,var,0,vars.size());
    vars.add(id);
    return id;
  }

  @Override void exec(Env env) {
    _eval.exec(env);            // RHS before LHS (R eval order)
    if( _lhs instanceof ASTId ) {
      ASTId id = (ASTId)_lhs;
      env.tos_into_slot(id._depth, id._num, id._id);
      return;
    }
    // Peel apart a slice assignment
    ASTSlice slice = (ASTSlice)_lhs;
    ASTId id = (ASTId)slice._ast;
    assert id._depth==0;        // Can only modify in the local scope.
    // Simple assignment using the slice syntax
    if( slice._rows==null & slice._cols==null ) {
      env.tos_into_slot(id._depth,id._num,id._id);
      return;
    }
    // Pull the LHS off the stack; do not lower the refcnt
    Frame ary = env.frId(id._depth,id._num);
    // Pull the RHS off the stack; do not lower the refcnt
    Frame ary_rhs=null;  double d=Double.NaN;
    if( env.isDbl() ) d = env._d[env._sp-1];
    else        ary_rhs = env.peekAry(); // Pop without deleting

    // Typed as a double ==> the row & col selectors are simple constants
    if( slice._t == Type.DBL ) { // Typed as a double?
      assert ary_rhs==null;
      long row = (long)((ASTNum)slice._rows)._d-1;
      int  col = (int )((ASTNum)slice._cols)._d-1;
      Chunk c = ary.vecs()[col].chunkForRow(row);
      c.set(row,d);
      Futures fs = new Futures();
      c.close(c.cidx(),fs);
      fs.blockForPending();
      env.push(d);
      return;
    }

    // Execute the slice LHS selection operators
    Object cols = ASTSlice.select(ary.numCols(),slice._cols,env);
    Object rows = ASTSlice.select(ary.numRows(),slice._rows,env);

    long[] cs1; long[] rs1;
    if(cols != null && rows != null && (cs1 = (long[])cols).length == 1 && (rs1 = (long[])rows).length == 1) {
      assert ary_rhs == null;
      long row = rs1[0]-1;
      int col = (int)cs1[0]-1;
      if(col >= ary.numCols() || row >= ary.numRows())
        throw H2O.unimpl();
      if(ary.vecs()[col].isEnum())
        throw new IllegalArgumentException("Currently can only set numeric columns");
      ary.vecs()[col].set(row,d);
      env.push(d);
      return;
    }

    // Partial row assignment?
    if( rows != null ) {

      // Only have partial row assignment
      if (cols == null) {

        // For every col at the range of indexes, set the value to be the rhs.
        // If the rhs is a double, then fill with doubles, NA where type is Enum.
        if (ary_rhs == null) {
          // Make a new Vec where each row to be written over has the value d
          final long[] rows0 = (long[]) rows;
          final double d0 = d;
          Vec v = new MRTask2() {
            @Override
            public void map(Chunk cs) {
              for (long er : rows0) {
                er = Math.abs(er) - 1; // 1-based -> 0-based
                if (er < cs._start || er > (cs._len + cs._start - 1)) continue;
                cs.set0((int) (er - cs._start), d0);
              }
            }
          }.doAll(ary.anyVec().makeZero()).getResult()._fr.anyVec();

          // MRTask over the lhs array
          new MRTask2() {
            @Override public void map(Chunk[] chks) {
              // Replace anything that is non-zero in the rep_vec.
              Chunk rep_vec = chks[chks.length-1];
              for (int row = 0; row < chks[0]._len; ++row) {
                if (rep_vec.at0(row) == 0) continue;
                for (Chunk chk : chks) {
                  if (chk._vec.isEnum()) { chk.setNA0(row); } else { chk.set0(row, d0); }
                }
              }
            }
          }.doAll(ary.add("rep_vec",v));
          UKV.remove(v._key);
          UKV.remove(ary.remove(ary.numCols()-1)._key);

        // If the rhs is an array, then fail if `height` of the rhs != rows.length. Otherwise, fetch-n-fill! (expensive)
        } else {
          throw H2O.unimpl();
        }

      // Have partial row and col assignment
      } else {
        throw H2O.unimpl();
      }
//      throw H2O.unimpl();
    } else {
      assert cols != null; // all/all assignment uses simple-assignment

      // Convert constant into a whole vec
      if (ary_rhs == null)
        ary_rhs = new Frame(ary.anyVec().makeCon(d));
      // Make sure we either have 1 col (repeated) or exactly a matching count
      long[] cs = (long[]) cols;  // Columns to act on
      if (ary_rhs.numCols() != 1 &&
              ary_rhs.numCols() != cs.length)
        throw new IllegalArgumentException("Can only assign to a matching set of columns; trying to assign " + ary_rhs.numCols() + " cols over " + cs.length + " cols");
      // Replace the LHS cols with the RHS cols
      Vec rvecs[] = ary_rhs.vecs();
      Futures fs = new Futures();
      for (int i = 0; i < cs.length; i++) {
        int cidx = (int) cs[i] - 1;      // Convert 1-based to 0-based
        Vec rv = env.addRef(rvecs[rvecs.length == 1 ? 0 : i]);
        if (cidx == ary.numCols()) {
          if (!rv.group().equals(ary.anyVec().group())) {
            env.subRef(rv);
            rv = ary.anyVec().align(rv);
            env.addRef(rv);
          }
          ary.add("C" + String.valueOf(cidx + 1), rv);     // New column name created with 1-based index
        }
        else {
          if (!(rv.group().equals(ary.anyVec().group())) && rv.length() == ary.anyVec().length()) {
            env.subRef(rv);
            rv = ary.anyVec().align(rv);
            env.addRef(rv);
          }
          fs = env.subRef(ary.replace(cidx, rv), fs);
        }
      }
      fs.blockForPending();
    }
    // After slicing, pop all expressions (cannot lower refcnt till after all uses)
    int narg = 0;
    if( rows!= null ) narg++;
    if( cols!= null ) narg++;
    env.pop(narg);
  }
  @Override String argName() { return _lhs instanceof ASTId ? ((ASTId)_lhs)._id : null; }
  @Override public String toString() { return "="; }
  @Override public StringBuilder toString( StringBuilder sb, int d ) {
    indent(sb,d).append(this).append('\n');
    _lhs.toString(sb,d+1).append('\n');
    _eval.toString(sb,d+1);
    return sb;
  }
}

// --------------------------------------------------------------------------
class ASTNum extends AST {
  static final NumberFormat NF = NumberFormat.getInstance(Locale.US);
  static { NF.setGroupingUsed(false); }
  final double _d;
  ASTNum(double d) { super(Type.DBL); _d=d; }
  // Parse a number, or throw a parse error
  static ASTNum parse(Exec2 E) {
    int startPosition = E._x;
    MyInteger charactersConsumed = new MyInteger();
    double d = parseNumberWithScientificNotationProperlyHandled(E._str, startPosition, charactersConsumed);
    if( charactersConsumed._val == 0 ) return null;
    E._x = startPosition + charactersConsumed._val;
    return new ASTNum(d);
  }
  boolean isPosConstant() { return _d >= 0; }
  @Override void exec(Env env) { env.push(_d); }
  @Override public String toString() { return Double.toString(_d); }

  /** Wrap an integer so that it can be modified by a called method.  i.e. Pass-by-reference.  */
  static class MyInteger { public int _val; }

  /**
   * Parse a scientific number more correctly for commands passed in from R.
   * Unfortunately, NumberFormat.parse doesn't get the job done.
   * It expects 'E' and can't handle 'e' or 'E+nnn'.
   *
   * @param s String to parse
   * @param startPosition Starting position in the string to parse from.
   * @param charactersConsumed [output] Characters consumed.
   * @return The parsed value if one was found, null otherwise.  If a value was parsed, charactersConsumed will be set to something greater than 0.  If no value was parsed, charactersConsumed will be 0.
   */
  static private double parseNumberWithScientificNotationProperlyHandled (String s, final int startPosition, MyInteger charactersConsumed) {
    charactersConsumed._val = 0;    // Paranoid.
    ParsePosition pp = new ParsePosition(startPosition);
    Number N = NF.parse(s, pp);
    if ( pp.getIndex()==startPosition ) // If no number was found, just return immediately.
      return 0;
    assert N instanceof Double || N instanceof Long;

    // Check if the number we just parsed had an 'e' or 'E' in it.  So it's scientific already.
    for (int i = startPosition; i < pp.getIndex(); i++) {
      char c = s.charAt(i);
      if ((c == 'e') || (c == 'E')) {
        // We already got a scientific number.  Return it.
        charactersConsumed._val = pp.getIndex() - startPosition;
        if( N instanceof Double ) return (Double)N;
        return (double)(Long)N;
      }
    }

    // If we consumed all of str, then just return the value now.
    assert (pp.getIndex() <= s.length());
    if (pp.getIndex() >= s.length()) {
      charactersConsumed._val = pp.getIndex() - startPosition;
      if( N instanceof Double ) return (Double)N;
      return (double)(Long)N;
    }

    // If the lookahead character is not 'e' then just return the value now.
    char lookaheadChar = s.charAt(pp.getIndex());
    if ((lookaheadChar != 'e') && (lookaheadChar != 'E')) {
      charactersConsumed._val = pp.getIndex() - startPosition;
      if( N instanceof Double ) return (Double)N;
      return (double)(Long)N;
    }

    // The lookahead character is 'e'.  Find the remaining trailing numbers
    // and attach them to this token.
    // Start with sb as stuff from NF.parse plus the 'e'.
    StringBuilder sb = new StringBuilder();
    sb.append(s.substring(startPosition, Math.min(s.length(),pp.getIndex() + 2)));
    for( int i = pp.getIndex() + 2; i < s.length(); i++ ) {
      char c = s.charAt(i);
      if( c!='+' && c!='-' && !Character.isDigit(c) ) // Only +-digits allowed after that.
        break;
      sb.append(c);
    }

    // Really parse the double now.  If we fail here, just bail out and don't
    // consider it a number.
    try {
      double d = Double.valueOf(sb.toString());
      charactersConsumed._val = sb.length(); // Set length consumed before return
      return d;
    }
    catch (Exception e) {  return 0;   } // No set length; just return.
  }

  public static void main (String[] args) {
    // Unit tests for horrible Double.valueOf parsing hack.

    {
      String s = "fooo1.23e+154";
      int i = 4;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 1.23e+154;
      assert C._val == 9;
      System.out.println (d);
    }

    {
      String s = "fooo1.23e+154blah";
      int i = 4;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 1.23e+154;
      assert C._val == 9;
      System.out.println (d);
    }

    {
      String s = "fooo1.23e14blah";
      int i = 4;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 1.23e14;
      assert C._val == 7;
      System.out.println (d);
    }

    {
      String s = "fooo1.23e";
      int i = 4;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 0;
      assert C._val == 0;
      System.out.println (d);
    }

    {
      String s = "fooo1.23E-10";
      int i = 4;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 1.23E-10;
      assert C._val == 8;
      System.out.println (d);
    }

    {
      String s = "1.23E-10";
      int i = 0;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 1.23E-10;
      assert C._val == 8;
      System.out.println (d);
    }

    {
      String s = "1.23E10E22";
      int i = 0;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 1.23E10;
      assert C._val == 7;
      System.out.println (d);
    }

    {
      String s = "hex[( hex[,c(5)] <= 1.97872258214 ) & ( hex[,c(6)] <= 32.8571773789 ) & ( ( hex[,c(2)] <= 72.2154196079 )) ,]";
      int i = 20;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 1.97872258214;
      assert C._val == 13;
      System.out.println (d);
    }

    {
      String s = "1    ";
      int i = 0;
      MyInteger C = new MyInteger();
      double d = parseNumberWithScientificNotationProperlyHandled(s, i, C);
      assert d == 1;
      assert C._val == 1;
      System.out.println (d);
    }
  }
}
