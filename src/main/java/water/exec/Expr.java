package water.exec;

import java.util.ArrayList;
import java.util.HashMap;
import water.*;
import water.exec.RLikeParser.Token;
import water.parser.ParseDataset;

// =============================================================================
// Expression
// =============================================================================
public abstract class Expr {

  // ---------------------------------------------------------------------------
  // Result

  public static class Result {
    public static enum Type {
      rtKey,
      rtNumberLiteral,
      rtStringLiteral
    }
    public final Key _key;
    private int _refCount;
    boolean _copied;
    private int _colIndex;
    public final double _const;
    public final String _str;
    public Type _type;

    private Result(Key k, int refCount, int pos) throws EvaluationException {
      Value v = DKV.get(k);
      if( v != null && !v.isArray() ) {
        if( v._max > 100 )
          throw new EvaluationException(pos, "Key "+k+" is neither a Vector nor a simple number");
        String s = new String(v.memOrLoad());
        try { _const = Double.valueOf(s); }
        catch( NumberFormatException nfe ) {
          throw new EvaluationException(pos, "Key "+k+" is neither a Vector nor a simple number");
        }
        _key = null;
        _refCount = 1;
        _type = Type.rtNumberLiteral;
      } else {
        _const = 0;
        _key = k;
        _refCount = refCount;
        _type = Type.rtKey;
      }
      _colIndex = -1;
      _str = null;
    }

    private Result(double value) {
      _key = null;
      _refCount = 1;
      _colIndex = -1;
      _const = value;
      _type = Type.rtNumberLiteral;
      _str = null;
    }

    private Result(String str) {
      _key = null;
      _refCount = 1;
      _colIndex = -1;
      _const = 0;
      _type = Type.rtStringLiteral;
      _str = str;
    }

    public static Result temporary(Key k) throws EvaluationException { return new Result(k, 1, 0); }

    public static Result temporary() throws EvaluationException { return new Result(Key.make(), 1, 0); }

    public static Result permanent(Key k) throws EvaluationException { return new Result(k, -1, 0); }

    public static Result scalar(double v) { return new Result(v); }

    public static Result string(String s) { return new Result(s); }

    public void dispose() {
      if( _key == null )
        return;
      --_refCount;
      if( _refCount == 0 )
        if( _copied )
          DKV.remove(_key); // remove only the array header
        else
          UKV.remove(_key);
    }

    public boolean isTemporary() { return _refCount >= 0; }

    public boolean canShallowCopy() { return false; }
    // shallow copy does not seem to be possible at the moment - arraylets are fixed to their key
    //return isTemporary() && (_copied == false);

    public int rawColIndex() { return _colIndex; }

    public int colIndex() { return _colIndex < 0 ? 0 : _colIndex; }

    public void setColIndex(int index) { _colIndex = index; }

    public void convertToSingleColumn(int pos) throws EvaluationException {
      if (_type != Type.rtKey)
        return; // nothing to do
      ValueArray va = getValueArray(pos,_key);
      if (rawColIndex() == -1) {
        setColIndex(0);
        if (va.numCols()!=1)
          throw new EvaluationException(pos, "Column must be specified for the first operand");
      }
    }

  }

  public abstract Result eval() throws EvaluationException;
  /**
   * Position of the expression in the parsed string.
   */
  public final int _pos;

  protected Expr(int pos) { _pos = pos; }

  /**
   * Use this method to get ValueArrays as it is typechecked.
   *
   * @param k
   * @return
   * @throws EvaluationException
   */
  public ValueArray getValueArray(Key k) throws EvaluationException {
    return getValueArray(_pos, k);
  }

  public static ValueArray getValueArray(int pos, Key k) throws EvaluationException {
    return ValueArray.value(k);
  }

}

// =============================================================================
// KeyLiteral
// =============================================================================
class KeyLiteral extends Expr {

  private final Key _key;

  public KeyLiteral(int pos, String id) {
    super(pos);
    _key = Key.make(id);
  }

  @Override
  public Result eval() throws EvaluationException {
    if (DKV.get(_key) == null)
      throw new EvaluationException(_pos, "Key "+_key.toString()+" not found.");
    return Result.permanent(_key);
  }
}

// =============================================================================
// FloatLiteral
// =============================================================================
class FloatLiteral extends Expr {

  public final double _d;

  public FloatLiteral(int pos, double d) {
    super(pos);
    _d = d;
  }

  @Override
  public Expr.Result eval() throws EvaluationException { return Expr.Result.scalar(_d); }
}

// =============================================================================
// StringLiteral
// =============================================================================
class StringLiteral extends Expr {

  public final String _str;

  public StringLiteral(int pos, String str) {
    super(pos);
    _str = str;
  }

  @Override
  public Expr.Result eval() throws EvaluationException { return Expr.Result.string(_str); }
}

// =============================================================================
// AssignmentOperator
// =============================================================================
class AssignmentOperator extends Expr {

  private final Key _lhs;
  private final Expr _rhs;

  public AssignmentOperator(int pos, Key lhs, Expr rhs) {
    super(pos);
    _lhs = lhs;
    _rhs = rhs;
  }

  @Override
  public Result eval() throws EvaluationException {
    Result rhs = _rhs.eval();
    try {
      Helpers.assign(_pos, _lhs, rhs);
      Helpers.calculateSigma(_lhs, 0);
    } finally {
      rhs.dispose();
    }
    return Result.permanent(_lhs);
  }
}

// =============================================================================
// ColumnSelector
// =============================================================================
class ColumnSelector extends Expr {

  private final Expr _expr;
  private final int _colIndex;

  public ColumnSelector(int pos, Expr expr, int colIndex) {
    super(pos);
    _expr = expr;
    _colIndex = colIndex;
  }

  @Override
  public Result eval() throws EvaluationException {
    Result result = _expr.eval();
    ValueArray v = getValueArray(result._key);
    if( v.numCols() <= _colIndex ) {
      result.dispose();
      throw new EvaluationException(_pos, "Column " + _colIndex + " not present in expression (has " + v.numCols() + ")");
    }
    result.setColIndex(_colIndex);
    return result;
  }
}

// =============================================================================
// StringColumnSelector
// =============================================================================
class StringColumnSelector extends Expr {

  private final Expr _expr;
  private final String _colName;

  public StringColumnSelector(int pos, Expr expr, String colName) {
    super(pos);
    _expr = expr;
    _colName = colName;
  }

  @Override
  public Result eval() throws EvaluationException {
    Result result = _expr.eval();
    ValueArray v = getValueArray(result._key);
    if( v == null ) {
      result.dispose();
      throw new EvaluationException(_pos, "Key " + result._key.toString() + " not found");
    }
    for( int i = 0; i < v.numCols(); ++i ) {
      if( v._cols[i]._name.equals(_colName) ) {
        result.setColIndex(i);
        return result;
      }
    }
    result.dispose();
    throw new EvaluationException(_pos, "Column " + _colName + " not present in expression");
  }
}

// =============================================================================
// UnaryOperator
// =============================================================================
class UnaryOperator extends Expr {

  private final Expr _opnd;
  private final Token.Type _type;

  public UnaryOperator(int pos, Token.Type type, Expr opnd) {
    super(pos);
    _type = type;
    _opnd = opnd;
  }

  private Result evalConst(Result o) throws EvaluationException {
    switch( _type ) {
      case ttOpSub:
        return Result.scalar(-o._const);
      default:
        throw new EvaluationException(_pos, "Operator " + _type.toString() + " not applicable to given operand.");
    }
  }

  private Result evalVect(Result o) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray opnd = getValueArray(o._key);
    if (o.rawColIndex() == -1) {
      o.setColIndex(0);
      if (opnd.numCols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for the operand");
    }
    // we do not need to check the columns here - the column selector operator does this for us
    // one step ahead
    VABuilder b = new VABuilder("temp",opnd.numRows()).addDoubleColumn("0").createAndStore(res._key);
    MRVectorUnaryOperator op;
    switch( _type ) {
      case ttOpSub:
        op = new UnaryMinus(o._key, res._key, o.rawColIndex());
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
    op.invoke(res._key);
    b.setColumnStats(0,op._min, op._max, op._tot / opnd.numRows()).createAndStore(res._key);
    return res;
  }

  @Override
  public Result eval() throws EvaluationException {
    // get the keys and the values
    Result op = _opnd.eval();
    try {
      switch (op._type) {
        case rtNumberLiteral:
          return evalConst(op);
        case rtKey:
          return evalVect(op);
        default:
          throw new EvaluationException(_pos, "Incompatible operand type, expected Value or number constant");
      }
    } finally {
      op.dispose();
    }
  }
}

// =============================================================================
// Binary Operator
// =============================================================================
class BinaryOperator extends Expr {

  private final Expr _left;
  private final Expr _right;
  private final Token.Type _type;

  public BinaryOperator(int pos, Token.Type type, Expr left, Expr right) {
    super(pos);
    _left = left;
    _right = right;
    _type = type;

  }

  private Result evalConstConst(Result l, Result r) throws EvaluationException {
    switch( _type ) {
      case ttOpAdd:
        return Result.scalar(l._const + r._const);
      case ttOpSub:
        return Result.scalar(l._const - r._const);
      case ttOpMul:
        return Result.scalar(l._const * r._const);
      case ttOpDiv:
        return Result.scalar(l._const / r._const);
      case ttOpMod:
        return Result.scalar(l._const % r._const);
      case ttOpLess:
        return Result.scalar(l._const < r._const ? 1 : 0);
      case ttOpLessOrEq:
        return Result.scalar(l._const <= r._const ? 1 : 0);
      case ttOpGreater:
        return Result.scalar(l._const > r._const ? 1 : 0);
      case ttOpGreaterOrEq:
        return Result.scalar(l._const >= r._const ? 1 : 0);
      case ttOpEq:
        return Result.scalar(l._const == r._const ? 1 : 0);
      case ttOpNeq:
        return Result.scalar(l._const != r._const ? 1 : 0);
      case ttOpAnd:
        return Result.scalar(((l._const != 0) && (r._const !=0)) ? 1 : 0);
      case ttOpOr:
        return Result.scalar(((l._const != 0) && (r._const !=0)) ? 1 : 0);
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
  }

  private Result evalVectVect(Result l, Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vl = getValueArray(l._key);
    ValueArray vr = getValueArray(r._key);
    if (l.rawColIndex() == -1) {
      l.setColIndex(0);
      if (vl.numCols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for left operand");
    }
    if (r.rawColIndex() == -1) {
      r.setColIndex(0);
      if (vr.numCols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for right operand");
    }
    long resultRows = Math.max(vl.numRows(), vr.numRows());
    // we do not need to check the columns here - the column selector operator does this for us
    // one step ahead
    VABuilder b = new VABuilder("temp",resultRows).addDoubleColumn("0").createAndStore(res._key);
    MRVectorBinaryOperator op;
    switch( _type ) {
      case ttOpAdd:
        op = new AddOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpSub:
        op = new SubOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpMul:
        op = new MulOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpDiv:
        op = new DivOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpMod:
        op = new ModOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpLess:
        op = new LessOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpLessOrEq:
        op = new LessOrEqOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpGreater:
        op = new GreaterOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpGreaterOrEq:
        op = new GreaterOrEqOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpEq:
        op = new EqOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpNeq:
        op = new NeqOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpAnd:
        op = new AndOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      case ttOpOr:
        op = new OrOperator(l._key, r._key, res._key, l.rawColIndex(), r.rawColIndex());
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
    op.invoke(res._key);
    b.setColumnStats(0,op._min, op._max, op._tot / resultRows).createAndStore(res._key);
    return res;
  }

  private Result evalConstVect(final Result l, Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vr = getValueArray(r._key);
    if (r.rawColIndex() == -1) {
      r.setColIndex(0);
      if (vr.numCols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for right operand");
    }
    MRVectorUnaryOperator op;
    switch( _type ) {
      case ttOpAdd:
        op = new LeftAdd(r._key, res._key, r.rawColIndex(), l._const); // commutative
        break;
      case ttOpSub:
        op = new RightSub(r._key, res._key, r.rawColIndex(), l._const);
        break;
      case ttOpMul:
        op = new LeftMul(r._key, res._key, r.rawColIndex(), l._const); // commutative
        break;
      case ttOpDiv:
        op = new RightDiv(r._key, res._key, r.rawColIndex(), l._const);
        break;
      case ttOpMod:
        op = new RightMod(r._key, res._key, r.rawColIndex(), l._const);
        break;
      case ttOpLess:
        op = new LeftGreaterOrEq(l._key, res._key, l.rawColIndex(), r._const); // s < V <-> V >= s
        break;
      case ttOpLessOrEq:
        op = new LeftGreater(l._key, res._key, l.rawColIndex(), r._const); // s <= V <-> V > s
        break;
      case ttOpGreater:
        op = new LeftLessOrEq(l._key, res._key, l.rawColIndex(), r._const); // s > V <-> V <= s
        break;
      case ttOpGreaterOrEq:
        op = new LeftLess(l._key, res._key, l.rawColIndex(), r._const); // s >= V <-> V < s
        break;
      case ttOpEq:
        op = new LeftEq(l._key, res._key, l.rawColIndex(), r._const); // commutative
        break;
      case ttOpNeq:
        op = new LeftNeq(l._key, res._key, l.rawColIndex(), r._const); // commutative
        break;
      case ttOpAnd:
        op = new RightAnd(r._key, res._key, r.rawColIndex(), l._const);
        break;
      case ttOpOr:
        op = new RightOr(r._key, res._key, r.rawColIndex(), l._const);
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
    VABuilder b = new VABuilder("temp",vr.numRows()).addDoubleColumn("0").createAndStore(res._key);
    op.invoke(res._key);
    b.setColumnStats(0,op._min, op._max, op._tot / vr.numRows()).createAndStore(res._key);
    return res;
  }

  private Result evalVectConst(Result l, final Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vl = getValueArray(l._key);
    if (l.rawColIndex() == -1) {
      l.setColIndex(0);
      if (vl.numCols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for left operand");
    }
    MRVectorUnaryOperator op;
    switch( _type ) {
      case ttOpAdd:
        op = new LeftAdd(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpSub:
        op = new LeftSub(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpMul:
        op = new LeftMul(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpDiv:
        op = new LeftDiv(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpMod:
        op = new LeftMod(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpLess:
        op = new LeftLess(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpLessOrEq:
        op = new LeftLessOrEq(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpGreater:
        op = new LeftGreater(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpGreaterOrEq:
        op = new LeftGreaterOrEq(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpEq:
        op = new LeftEq(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpNeq:
        op = new LeftNeq(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpAnd:
        op = new LeftAnd(l._key, res._key, l.rawColIndex(), r._const);
        break;
      case ttOpOr:
        op = new LeftOr(l._key, res._key, l.rawColIndex(), r._const);
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
    VABuilder b = new VABuilder("temp", vl.numRows()).addDoubleColumn("0").createAndStore(res._key);
    op.invoke(res._key);
    b.setColumnStats(0,op._min, op._max, op._tot / vl.numRows()).createAndStore(res._key);
    return res;
  }

  @Override
  public Result eval() throws EvaluationException {
    // get the keys and the values
    Result kl = _left.eval();
    Result kr = _right.eval();
    try {
      switch (kl._type) {
        case rtNumberLiteral:
          switch (kr._type) {
            case rtNumberLiteral:
              return evalConstConst(kl, kr);
            case rtKey:
              return evalConstVect(kl, kr);
            default:
              throw new EvaluationException(_pos,"Only Value or numeric constant are allowed as the second operand");
          }
        case rtKey:
          switch (kr._type) {
            case rtNumberLiteral:
              return evalVectConst(kl, kr);
            case rtKey:
              return evalVectVect(kl, kr);
            default:
              throw new EvaluationException(_pos,"Only Value or numeric constant are allowed as the second operand");
          }
        default:
          throw new EvaluationException(_pos,"Only Value or numeric constant are allowed as the first operand");
      }
    } finally {
      kl.dispose();
      kr.dispose();
    }
  }
}

// =============================================================================
// FunctionCall
// =============================================================================

class FunctionCall extends Expr {

  public final Function _function;

  private final Expr[] _args;

  public FunctionCall(int pos, String fName) throws ParserException {
    super(pos);
    // TODO get to _function
    _function = Function.FUNCTIONS.get(fName);
    if (_function == null)
      throw new ParserException(pos,"Function "+fName+" is not defined.");
    _args = new Expr[_function.numArgs()];
  }

  public void addArgument(Expr argument) throws ParserException {
    int i = 0;
    while ((i < _args.length) && (_args[i] != null)) ++i;
    if (i==_args.length)
      throw new ParserException(argument._pos,"Function "+_function._name+" takes only "+_args.length+" arguments");
    _args[i] = argument;
  }

  public void addArgument(Expr argument, int idx) {
    assert (idx < _args.length) && (idx>=0) && (_args[idx] == null);
    _args[idx] = argument;
  }

  public void addArgument(Expr argument, String name) throws ParserException {
    int idx = _function.argIndex(name);
    if (idx == -1)
      throw new ParserException(argument._pos,"Argument "+name+" not recognized for function "+_function._name);
    if (_args[idx] != null)
      throw new ParserException(argument._pos,"Argument "+name+" already defined for function "+_function._name);
    addArgument(argument,idx);
  }

  /** Just checks that we either have the argument, or that its default value can be obtained. */
  public void staticArgumentVerification() throws ParserException {
    for (int i = 0; i < _args.length; ++i) {
      if (_args[i] != null)
        continue;
      if (_function.checker(i)._defaultValue != null)
        continue;
      throw new ParserException(_pos,"Formal argument index "+i+" is missing and does not have default value for function "+_function._name);
    }
  }

  @Override public Result eval() throws EvaluationException {
    Result[] args = new Result[_args.length];
    for (int i = 0; i<args.length; ++i) {
      if (_args[i] == null) {
        args[i] = _function.checker(i)._defaultValue;
      } else {
        args[i] = _args[i].eval();
        try {
          _function.checker(i).checkResult(args[i]);
        } catch (Exception e) {
          throw new EvaluationException(_args[i]._pos,e.getMessage());
        }
      }
    }
    try {
      return _function.eval(args);
    } catch (Exception e) {
      throw new EvaluationException(_pos,e.toString());
    } finally {
      for( Result r : args ) if( r != null ) r.dispose();
    }
  }
}

// =============================================================================
// Ternary conditional operator ? : (inline if)
// =============================================================================

/** The ternary inline if operator.
 *
 * If the first condition expression evaluates to a scalar, then simply result
 * of either the true or false expression is evaluated and returned whatever
 * they are.
 *
 * If the result of the condition expression is a vector, then a new vector is
 * returned that consists of the values reported by true / false expressions
 * wrapped (if necessary) or multiplied if they are scalars.
 *
 * @author peta
 */
class Iif extends Expr {

  public final Expr _cond;
  public final Expr _ifTrue;
  public final Expr _ifFalse;

  public Iif(int pos, Expr cond, Expr ifTrue, Expr ifFalse)  {
    super(pos);
    _cond = cond;
    _ifTrue = ifTrue;
    _ifFalse = ifFalse;
  }


  @Override public Result eval() throws EvaluationException {
    Result cond = _cond.eval();
    try {
      // if condition is a scalar, return true or false arg unmodified
      if (cond._type == Result.Type.rtNumberLiteral)
        return cond._const != 0 ? _ifTrue.eval() : _ifFalse.eval();
      if (cond._type == Result.Type.rtKey) {
        cond.convertToSingleColumn(_pos);
        ValueArray vc = getValueArray(cond._key);
        Result result = Result.temporary();
        Result ifTrue = _ifTrue.eval();
        Result ifFalse = _ifFalse.eval();
        ifTrue.convertToSingleColumn(_pos);
        ifFalse.convertToSingleColumn(_pos);
        MRColumnProducer task = null;
        if (ifTrue._type == Result.Type.rtNumberLiteral) {
          if (ifFalse._type == Result.Type.rtNumberLiteral)
            task = new IifOperatorScalar23(cond._key, ifTrue._const, ifFalse._const, result._key, cond.colIndex());
          else
            task = new IifOperatorScalar2(cond._key, ifTrue._const, ifFalse._key, result._key, cond.colIndex(), ifFalse.colIndex());
        } else {
          if (ifFalse._type == Result.Type.rtNumberLiteral)
            task = new IifOperatorScalar3(cond._key, ifTrue._key, ifFalse._const, result._key, cond.colIndex(), ifTrue.colIndex());
          else
            task = new IifOperator(cond._key, ifTrue._key, ifFalse._key, result._key, cond.colIndex(), ifTrue.colIndex(), ifFalse.colIndex());
        }
        VABuilder b = new VABuilder("temp", vc.numRows()).addDoubleColumn("0").createAndStore(result._key);
        task.invoke(result._key);
        b.setColumnStats(0, task._min, task._max, task._tot / vc.numRows()).createAndStore(result._key);
        ifTrue .dispose();
        ifFalse.dispose();
        return result;
      }

      return null;
    } finally {
      cond.dispose();
    }
  }
}